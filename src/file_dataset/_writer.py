"""Writer functionality for file datasets."""

import collections.abc
import logging
from pathlib import Path

import pandas as pd

from ._core_file import do_copy
from .exceptions import FileDatasetError
from .s3_options import S3Options
from .s3_utils import is_s3_url, parse_s3_url

logger = logging.getLogger(__name__)


def _validate_source_files(row: dict[str, str | Path | None]) -> dict[str, str]:
    """Validate that all non-None source files exist and are readable.

    Args:
        row: Dictionary mapping filenames to source paths or None

    Returns:
        Dictionary of filename to error message for any validation failures
    """
    file_errors: dict[str, str] = {}

    for filename, source_path in row.items():
        if source_path is None:
            continue  # Skip None values (optional files)

        source = Path(source_path)
        if not source.exists():
            file_errors[filename] = f"Source file not found: {source}"
        elif not source.is_file():
            file_errors[filename] = f"Source path is not a file: {source}"

    return file_errors


def _copy_files_to_destination(
    row: dict[str, str | Path | None], dest_dir: Path
) -> tuple[dict[str, Path | None], dict[str, str]]:
    """Copy files to destination directory.

    Args:
        row: Dictionary mapping filenames to source paths or None
        dest_dir: Destination directory path

    Returns:
        Tuple of (result mapping, error mapping)
    """
    result: dict[str, Path | None] = {}
    file_errors: dict[str, str] = {}

    # Prepare copies for do_copy
    copies = []
    file_map = {}  # Track which copy corresponds to which filename

    for filename, source_path in row.items():
        if source_path is None:
            result[filename] = None
            continue

        source = Path(source_path)
        dest = dest_dir / filename
        copies.append((str(source), str(dest)))
        file_map[str(dest)] = (filename, dest)

    # Perform all copies at once
    if copies:
        try:
            do_copy(copies, s3_options=None)
            # All copies succeeded
            for _dest_str, (filename, dest_path) in file_map.items():
                result[filename] = dest_path
        except FileDatasetError as e:
            # Extract file errors from the exception
            # do_copy uses index as key, we need to map back to filenames
            for i, (_, (filename, _)) in enumerate(file_map.items()):
                if str(i) in e.file_errors:
                    file_errors[filename] = e.file_errors[str(i)]

    return result, file_errors


def _upload_files_to_s3(
    row: dict[str, str | Path | None],
    bucket: str,
    s3_key_prefix: str,
    options: S3Options,
) -> dict[str, str | None]:
    """Upload files to S3.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        bucket: S3 bucket name
        s3_key_prefix: S3 key prefix for uploads
        options: S3Options instance with S3 client

    Returns:
        Dictionary mapping filenames to S3 URLs

    Raises:
        FileDatasetError: If any uploads fail
    """
    result = {}

    # Prepare copies for do_copy
    copies = []
    file_map = {}  # Track which copy corresponds to which filename

    for filename, source_path in row.items():
        if source_path is None:
            result[filename] = None
            continue

        source = Path(source_path)
        s3_key = f"{s3_key_prefix}/{filename}"
        s3_url = f"s3://{bucket}/{s3_key}"

        copies.append((str(source), s3_url))
        file_map[s3_url] = filename

    # Perform all uploads at once
    if copies:
        try:
            do_copy(copies, s3_options=options)
            # All uploads succeeded
            for s3_url, filename in file_map.items():
                result[filename] = s3_url
        except FileDatasetError as e:
            # Extract file errors from the exception
            # do_copy uses index as key, we need to map back to filenames
            upload_errors = {}
            for i, (_, filename) in enumerate(file_map.items()):
                if str(i) in e.file_errors:
                    upload_errors[filename] = e.file_errors[str(i)]
            raise FileDatasetError(upload_errors, "Failed to upload some files") from e

    return result


def _write_row_files(
    row: dict[str, str | Path | None],
    into_path: str | Path,
    id: str,  # noqa: A002
    options: S3Options | None = None,
) -> dict[str, Path | None | str]:
    """Write a single row of files to destination directory or S3.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory or S3 path
        id: Unique identifier for this set of files
        options: S3Options instance for S3 operations

    Returns:
        Dictionary mapping filenames to their final destination paths or S3 URLs

    Raises:
        FileDatasetError: If any required files cannot be written
    """
    # Filter out the 'id' key if present (it's metadata, not a file)
    file_row = {k: v for k, v in row.items() if k != "id"}

    # Validate inputs
    if not file_row:
        return {}

    # Validate all non-None source files exist before starting
    file_errors = _validate_source_files(file_row)
    if file_errors:
        raise FileDatasetError(file_errors, "Cannot write files to destination")

    # Check if destination is S3
    if is_s3_url(str(into_path)):
        # S3 path handling
        bucket, prefix = parse_s3_url(str(into_path))
        s3_key_prefix = f"{prefix}/{id}" if prefix else id
        return _upload_files_to_s3(file_row, bucket, s3_key_prefix, options)
    # Local path handling
    dest_base = Path(into_path)
    dest_dir = dest_base / id

    # Create destination directory
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise FileDatasetError(
            {"directory": f"Failed to create destination directory: {e}"},
            "Failed to create destination directory",
        ) from e

    # Copy files to destination
    result, copy_errors = _copy_files_to_destination(file_row, dest_dir)

    # If any copies failed, raise error
    if copy_errors:
        raise FileDatasetError(copy_errors, "Failed to copy some files")

    return result


def _write_dataframe_files(
    dataframe: pd.DataFrame,
    into_path: str | Path,
    options: S3Options | None = None,
) -> pd.DataFrame:
    """Write files from DataFrame to destination directories.

    Args:
        dataframe: DataFrame where each row contains files to write
        into_path: Base destination directory or S3 path
        options: S3Options instance for S3 operations

    Returns:
        DataFrame with id column and file columns for successful writes

    Raises:
        FileDatasetError: If all rows fail to write
        ValueError: If DataFrame doesn't have an id column
    """
    # Check if DataFrame has id column
    if "id" not in dataframe.columns:
        msg = "DataFrame must have an 'id' column"
        raise ValueError(msg)

    # Prepare result lists
    successful_rows = []
    total_rows = len(dataframe)
    successful_count = 0

    # Process each row
    for _idx, df_row in dataframe.iterrows():
        row_id = df_row["id"]

        # Convert row to dict, excluding 'id' column
        row_dict = df_row.to_dict()
        file_columns = {k: v for k, v in row_dict.items() if k != "id"}

        try:
            # Write files for this row
            result = _write_row_files(file_columns, into_path, row_id, options)

            # Build successful row data
            row_data = {"id": row_id}
            row_data.update(result)
            successful_rows.append(row_data)
            successful_count += 1

        except FileDatasetError as e:
            # Log the error with row identifier
            logger.warning(
                "Failed to write files for row %s: %s. File errors: %s",
                row_id,
                e,
                e.file_errors,
            )
            continue
        except Exception as e:  # noqa: BLE001
            # Log unexpected errors
            logger.warning("Unexpected error writing files for row %s: %s", row_id, e)
            continue

    # If all rows failed, raise an error
    if successful_count == 0 and total_rows > 0:
        raise FileDatasetError(
            {"all_rows": "All rows failed to write"},
            "All rows failed. Check error logs for details.",
        )

    # Return DataFrame with successful results
    if successful_rows:
        return pd.DataFrame(successful_rows)
    # Return empty DataFrame with expected structure
    return pd.DataFrame(columns=["id"])


def write_files(
    data: dict[str, str | Path | None] | pd.DataFrame,
    *,
    into_path: str | Path,
    id: str | None = None,  # noqa: A002
    options: S3Options | None = None,
) -> dict[str, Path | None] | pd.DataFrame:
    """Write files to destination directory with ID-based organization.

    Args:
        data: Dictionary mapping filenames to their source paths (for single row)
              or DataFrame where each row contains files to write
        into_path: Base destination directory or S3 path (keyword-only)
        id: Unique identifier for this set of files (required for dict,
            forbidden for DataFrame)
        options: S3Options instance for S3 operations (keyword-only, required for S3)

    Returns:
        Dictionary mapping filenames to destination paths (when dict is provided)
        or DataFrame with id column and file columns (when DataFrame is provided)

    Raises:
        FileDatasetError: If any required files cannot be written
        ValueError: If id is missing for dict input or provided for DataFrame input
        ValueError: If S3 path is provided without options
        TypeError: If data is neither dict nor DataFrame
    """
    # Initialize default options if S3 path is used
    if is_s3_url(str(into_path)):
        if options is None:
            options = S3Options.default()
        # Validate S3 URL format
        parsed = parse_s3_url(str(into_path))
        if not parsed or not parsed[0]:  # Check for empty bucket name
            msg = f"Invalid S3 URL format: {into_path}"
            raise ValueError(msg)

    # Type-based dispatch
    if isinstance(data, pd.DataFrame):
        # DataFrame case - id should not be provided
        if id is not None:
            msg = (
                "Cannot specify 'id' when using DataFrame - "
                "DataFrame must have 'id' column"
            )
            raise ValueError(msg)
        return _write_dataframe_files(data, into_path, options)

    if isinstance(data, collections.abc.Mapping):
        # Dict/Mapping case - id is required
        if id is None and (id := data.get("id")) is None:  # noqa: A001
            msg = "Must specify 'id' when using dict/mapping data"
            raise ValueError(msg)
        return _write_row_files(data, into_path, id, options)

    # Neither DataFrame nor Mapping
    msg = f"Data must be either a dict/mapping or DataFrame, got {type(data).__name__}"
    raise TypeError(msg)
