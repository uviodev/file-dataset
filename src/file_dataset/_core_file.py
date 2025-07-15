"""Core file operations for file-dataset."""

import shutil
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


def do_copy(
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
    copy_errors: dict[str, str] = {}
    for i, (src, dst) in enumerate(copies):
        try:
            _copy_single_file(src, dst, s3_options)
        except (OSError, ValueError, ClientError) as e:
            copy_errors[str(i)] = str(e)

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
        s3_client.download_file(bucket, key, dst_str)
    elif not src_is_s3 and dst_is_s3:
        # Local to S3
        bucket, key = parse_s3_url(dst_str)
        s3_client = s3_options.s3_client
        s3_client.upload_file(src_str, bucket, key)
    else:
        # S3 to S3
        src_bucket, src_key = parse_s3_url(src_str)
        dst_bucket, dst_key = parse_s3_url(dst_str)
        s3_client = s3_options.s3_client

        # Use copy for S3 to S3
        copy_source = {"Bucket": src_bucket, "Key": src_key}
        s3_client.copy(copy_source, dst_bucket, dst_key)
