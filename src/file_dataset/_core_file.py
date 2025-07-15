"""Core file operations for file-dataset.

This module provides unified file operations for both local and S3 files.
All S3 operations use boto3's S3Transfer API for better performance,
including support for multipart uploads, automatic retries, and
configurable transfer settings via S3Options.transfer_config.
"""

import concurrent.futures
import shutil
from collections.abc import Mapping
from io import BytesIO
from pathlib import Path

from botocore.exceptions import ClientError

from file_dataset.exceptions import FileDatasetError
from file_dataset.s3_options import S3Options
from file_dataset.s3_utils import is_s3_url, parse_s3_url


def _validate_sources(copies: list[tuple[str | Path, str | Path]]) -> dict[str, str]:
    """Validate that all source files exist.

    Args:
        copies: List of (source, destination) tuples

    Returns:
        Dictionary of errors indexed by position
    """
    file_errors: dict[str, str] = {}

    for i, (src, _dst) in enumerate(copies):
        if is_s3_url(src):
            # For S3, we'll validate in _copy_single_file using head_object
            continue

        src_path = Path(src)
        if not src_path.exists():
            file_errors[str(i)] = f"Source file not found: {src}"

    return file_errors


def _validate_unique_destinations(
    copies: list[tuple[str | Path, str | Path]],
) -> dict[str, str]:
    """Validate that all destinations are unique.

    Args:
        copies: List of (source, destination) tuples

    Returns:
        Dictionary of errors indexed by position
    """
    file_errors: dict[str, str] = {}
    destinations = [str(dst) for src, dst in copies]

    if len(destinations) != len(set(destinations)):
        # Find duplicates
        seen = set()
        for i, dest in enumerate(destinations):
            if dest in seen:
                file_errors[str(i)] = f"Duplicate destination: {dest}"
            seen.add(dest)

    return file_errors


def _execute_copies_parallel(
    copies: list[tuple[str | Path, str | Path]],
    s3_options: S3Options,
) -> dict[str, str]:
    """Execute copy operations in parallel."""
    executor = s3_options.executor
    if not executor:
        return {}

    futures = {
        executor.submit(_copy_single_file, src, dst, s3_options): str(i)
        for i, (src, dst) in enumerate(copies)
    }

    errors = {}
    for future in concurrent.futures.as_completed(futures):
        idx = futures[future]
        try:
            future.result()
        except (OSError, ValueError, ClientError) as e:
            errors[idx] = str(e)

    return errors


def _execute_copies_sequential(
    copies: list[tuple[str | Path, str | Path]],
    s3_options: S3Options | None,
) -> dict[str, str]:
    """Execute copy operations sequentially."""
    errors = {}
    for i, (src, dst) in enumerate(copies):
        try:
            _copy_single_file(src, dst, s3_options)
        except (OSError, ValueError, ClientError) as e:
            errors[str(i)] = str(e)
    return errors


def copy_each_file(
    copies: list[tuple[str | Path, str | Path]], s3_options: S3Options | None
) -> None:
    """Copy files from source to destination.

    Args:
        copies: List of (source, destination) tuples
        s3_options: S3 options for S3 operations

    Raises:
        FileDatasetError: If any files cannot be copied
    """
    if not copies:
        return

    # Check if we need S3 options
    needs_s3 = any(is_s3_url(src) or is_s3_url(dst) for src, dst in copies)
    if needs_s3 and s3_options is None:
        s3_options = S3Options.default()

    # Validate sources and destinations
    file_errors = _validate_sources(copies)
    file_errors.update(_validate_unique_destinations(copies))

    # If validation errors, raise
    if file_errors:
        raise FileDatasetError(
            file_errors, "Validation failed for file copy operations"
        )

    # Perform copies, collecting any errors
    if s3_options and s3_options.local_parallelism and s3_options.executor:
        copy_errors = _execute_copies_parallel(copies, s3_options)
    else:
        copy_errors = _execute_copies_sequential(copies, s3_options)

    # If any copies failed, raise error
    if copy_errors:
        raise FileDatasetError(copy_errors, "Failed to copy some files")


def _copy_single_file(
    src: str | Path, dst: str | Path, s3_options: S3Options | None
) -> None:
    """Copy a single file from source to destination.

    Args:
        src: Source file path
        dst: Destination file path
        s3_options: S3 options for S3 operations
    """
    src_str = str(src)
    dst_str = str(dst)

    # Determine copy type
    src_is_s3 = is_s3_url(src_str)
    dst_is_s3 = is_s3_url(dst_str)

    if not src_is_s3 and not dst_is_s3:
        # Local to local
        shutil.copy2(src_str, dst_str)
    elif src_is_s3 and not dst_is_s3:
        # S3 to local
        bucket, key = parse_s3_url(src_str)
        s3_client = s3_options.s3_client
        s3_client.download_file(bucket, key, dst_str, Config=s3_options.transfer_config)
    elif not src_is_s3 and dst_is_s3:
        # Local to S3
        bucket, key = parse_s3_url(dst_str)
        s3_client = s3_options.s3_client
        s3_client.upload_file(src_str, bucket, key, Config=s3_options.transfer_config)
    else:
        # S3 to S3
        src_bucket, src_key = parse_s3_url(src_str)
        dst_bucket, dst_key = parse_s3_url(dst_str)
        s3_client = s3_options.s3_client

        # Use copy for S3 to S3
        copy_source = {"Bucket": src_bucket, "Key": src_key}
        s3_client.copy(copy_source, dst_bucket, dst_key)


def _read_sizes_parallel(
    files: Mapping[str, str | Path],
    s3_options: S3Options,
) -> tuple[dict[str, int], dict[str, str]]:
    """Read file sizes in parallel."""
    executor = s3_options.executor
    if not executor:
        return {}, {}

    futures = {
        executor.submit(_read_file_size, path, s3_options): key
        for key, path in files.items()
    }

    result = {}
    errors = {}
    for future in concurrent.futures.as_completed(futures):
        key = futures[future]
        try:
            result[key] = future.result()
        except (OSError, ClientError) as e:
            errors[key] = str(e)

    return result, errors


def _read_sizes_sequential(
    files: Mapping[str, str | Path],
    s3_options: S3Options | None,
) -> tuple[dict[str, int], dict[str, str]]:
    """Read file sizes sequentially."""
    result = {}
    errors = {}
    for key, path in files.items():
        try:
            result[key] = _read_file_size(path, s3_options)
        except (OSError, ClientError) as e:
            errors[key] = str(e)
    return result, errors


def _read_file_size(path: str | Path, s3_options: S3Options | None) -> int:
    """Read the size of a single file.

    Args:
        path: File path (local or S3)
        s3_options: S3 options for S3 operations

    Returns:
        File size in bytes

    Raises:
        OSError: For local file errors
        ClientError: For S3 errors
    """
    path_str = str(path)

    if is_s3_url(path_str):
        # S3 file
        bucket, key = parse_s3_url(path_str)
        s3_client = s3_options.s3_client
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]
    # Local file
    return Path(path_str).stat().st_size


def read_each_file_size(
    files: Mapping[str, str | Path], s3_options: S3Options | None = None
) -> tuple[Mapping[str, int], Mapping[str, str]]:
    """Read sizes of multiple files.

    Args:
        files: Mapping of identifier to file path
        s3_options: S3 options for S3 operations

    Returns:
        Tuple of (successful_results, errors) where:
        - successful_results: Mapping of identifier to file size in bytes
        - errors: Mapping of identifier to error message
    """
    if not files:
        return {}, {}

    # Check if we need S3 options
    needs_s3 = any(is_s3_url(str(path)) for path in files.values())
    if needs_s3 and s3_options is None:
        s3_options = S3Options.default()

    if s3_options and s3_options.local_parallelism and s3_options.executor:
        return _read_sizes_parallel(files, s3_options)
    return _read_sizes_sequential(files, s3_options)


def _read_contents_parallel(
    files: Mapping[str, str | Path],
    s3_options: S3Options,
) -> tuple[dict[str, bytes], dict[str, str]]:
    """Read file contents in parallel."""
    executor = s3_options.executor
    if not executor:
        return {}, {}

    futures = {
        executor.submit(_read_file_contents, path, s3_options): key
        for key, path in files.items()
    }

    result = {}
    errors = {}
    for future in concurrent.futures.as_completed(futures):
        key = futures[future]
        try:
            result[key] = future.result()
        except (OSError, ClientError) as e:
            errors[key] = str(e)

    return result, errors


def _read_contents_sequential(
    files: Mapping[str, str | Path],
    s3_options: S3Options | None,
) -> tuple[dict[str, bytes], dict[str, str]]:
    """Read file contents sequentially."""
    result = {}
    errors = {}
    for key, path in files.items():
        try:
            result[key] = _read_file_contents(path, s3_options)
        except (OSError, ClientError) as e:
            errors[key] = str(e)
    return result, errors


def _read_file_contents(path: str | Path, s3_options: S3Options | None) -> bytes:
    """Read the contents of a single file.

    Args:
        path: File path (local or S3)
        s3_options: S3 options for S3 operations

    Returns:
        File contents as bytes

    Raises:
        OSError: For local file errors
        ClientError: For S3 errors
    """
    path_str = str(path)

    if is_s3_url(path_str):
        # S3 file - use download_fileobj for S3Transfer support
        bucket, key = parse_s3_url(path_str)
        s3_client = s3_options.s3_client
        fileobj = BytesIO()
        s3_client.download_fileobj(
            bucket, key, fileobj, Config=s3_options.transfer_config
        )
        return fileobj.getvalue()
    # Local file
    return Path(path_str).read_bytes()


def read_each_file_contents(
    files: Mapping[str, str | Path], s3_options: S3Options | None = None
) -> tuple[Mapping[str, bytes], Mapping[str, str]]:
    """Read contents of multiple files.

    Args:
        files: Mapping of identifier to file path
        s3_options: S3 options for S3 operations

    Returns:
        Tuple of (successful_results, errors) where:
        - successful_results: Mapping of identifier to file contents as bytes
        - errors: Mapping of identifier to error message
    """
    if not files:
        return {}, {}

    # Check if we need S3 options
    needs_s3 = any(is_s3_url(str(path)) for path in files.values())
    if needs_s3 and s3_options is None:
        s3_options = S3Options.default()

    if s3_options and s3_options.local_parallelism and s3_options.executor:
        return _read_contents_parallel(files, s3_options)
    return _read_contents_sequential(files, s3_options)


def _validate_file_exists(path: str | Path, s3_options: S3Options | None) -> str | None:  # noqa: PLR0911
    """Validate that a file exists and is accessible.

    Args:
        path: File path (local or S3)
        s3_options: S3 options for S3 operations

    Returns:
        Error message if file doesn't exist or isn't accessible, None if valid
    """
    path_str = str(path)

    if is_s3_url(path_str):
        # S3 file - check using head_object
        if s3_options is None:
            return "S3Options required for S3 URLs but not provided"

        bucket, key = parse_s3_url(path_str)
        if not key:  # Bucket-only path isn't a file
            return f"Invalid S3 URL format: {path_str}"

        try:
            s3_options.s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            # Check for 404 error (object not found)
            if e.response.get("Error", {}).get("Code") == "404":
                return f"S3 object not found: {path_str}"
            # For other errors (permissions, etc.)
            return f"S3 access error: {e}"
        except Exception as e:  # noqa: BLE001
            return f"Error checking S3 file: {e}"
        else:
            return None  # File exists
    else:
        # Local file
        path_obj = Path(path_str)
        if not path_obj.exists():
            return f"Source file not found: {path_str}"
        if not path_obj.is_file():
            return f"Source path is not a file: {path_str}"
        return None  # File exists and is a file


def _validate_existence_parallel(
    files: Mapping[str, str | Path],
    s3_options: S3Options,
) -> dict[str, str]:
    """Validate file existence in parallel."""
    executor = s3_options.executor
    if not executor:
        return {}

    futures = {
        executor.submit(_validate_file_exists, str(path), s3_options): key
        for key, path in files.items()
    }

    errors = {}
    for future in concurrent.futures.as_completed(futures):
        key = futures[future]
        try:
            exist_error = future.result()
            if exist_error:
                errors[key] = exist_error
        except Exception as e:  # noqa: BLE001
            errors[key] = str(e)

    return errors


def _validate_existence_sequential(
    files: Mapping[str, str | Path],
    s3_options: S3Options | None,
) -> dict[str, str]:
    """Validate file existence sequentially."""
    errors = {}
    for key, path in files.items():
        path_str = str(path)
        exist_error = _validate_file_exists(path_str, s3_options)
        if exist_error:
            errors[key] = exist_error
    return errors


def _validate_s3_path_format(
    path: str | Path, s3_options: S3Options | None
) -> str | None:
    """Validate S3 path format.

    Args:
        path: File path to validate
        s3_options: S3 options for S3 operations

    Returns:
        Error message if invalid, None if valid or not an S3 path
    """
    path_str = str(path)

    # Return None immediately for local paths
    if not is_s3_url(path_str):
        return None

    # Validate S3 URL format
    parsed = parse_s3_url(path_str)
    if not parsed:
        return f"Invalid S3 URL format: {path_str}"

    bucket, key = parsed
    if not key:  # FileRowReader needs a key to read a file
        return f"Invalid S3 URL format: {path_str}"

    # Check S3Options is provided
    if s3_options is None:
        return "S3Options required for S3 URLs but not provided"

    return None


def validate_each_file(
    files: Mapping[str, str | Path],
    s3_options: S3Options | None = None,
    *,
    do_existence_checks: bool = True,
) -> dict[str, str]:
    """Validate multiple files.

    Args:
        files: Mapping of identifier to file path
        s3_options: S3 options for S3 operations
        do_existence_checks: If True, check if files exist; if False, only validate
            format

    Returns:
        Dictionary of errors indexed by file identifier (empty if all valid)
    """
    if not files:
        return {}

    errors: dict[str, str] = {}

    # First check if any S3 files exist and we have no s3_options
    has_s3_files = any(is_s3_url(str(path)) for path in files.values())
    if has_s3_files and s3_options is None:
        errors["__s3_options__"] = "S3Options required for S3 URLs but not provided"
        return errors

    # First pass: validate S3 path formats
    for key, path in files.items():
        path_str = str(path)
        format_error = _validate_s3_path_format(path_str, s3_options)
        if format_error:
            errors[key] = format_error

    # Second pass: existence checks (only if requested and no format errors)
    if do_existence_checks and not errors:
        if s3_options and s3_options.local_parallelism and s3_options.executor:
            exist_errors = _validate_existence_parallel(files, s3_options)
        else:
            exist_errors = _validate_existence_sequential(files, s3_options)
        errors.update(exist_errors)

    return errors
