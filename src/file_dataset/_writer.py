"""Writer functionality for file datasets."""

import logging
import shutil
from pathlib import Path

import pandas as pd

from .exceptions import FileDatasetError
from .options import Options
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

    for filename, source_path in row.items():
        if source_path is None:
            result[filename] = None
            continue

        source = Path(source_path)
        dest = dest_dir / filename

        try:
            shutil.copy2(source, dest)
            result[filename] = dest
        except OSError as e:
            file_errors[filename] = f"Failed to copy file: {e}"

    return result, file_errors


def _upload_files_to_s3(
    row: dict[str, str | Path | None],
    bucket: str,
    s3_key_prefix: str,
    options: Options,
) -> dict[str, str | None]:
    """Upload files to S3.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        bucket: S3 bucket name
        s3_key_prefix: S3 key prefix for uploads
        options: Options instance with S3 client

    Returns:
        Dictionary mapping filenames to S3 URLs

    Raises:
        FileDatasetError: If any uploads fail
    """
    result = {}
    upload_errors = {}

    for filename, source_path in row.items():
        if source_path is None:
            result[filename] = None
            continue

        source = Path(source_path)
        s3_key = f"{s3_key_prefix}/{filename}"

        try:
            options.s3_client.upload_file(str(source), bucket, s3_key)
            result[filename] = f"s3://{bucket}/{s3_key}"
        except Exception as e:  # noqa: BLE001
            upload_errors[filename] = f"Failed to upload file: {e}"

    # If any uploads failed, raise error
    if upload_errors:
        raise FileDatasetError(upload_errors, "Failed to upload some files")

    return result


def _write_row_files(
    row: dict[str, str | Path | None],
    into_path: str | Path,
    id: str,  # noqa: A002
    options: Options | None = None,
) -> dict[str, Path | None | str]:
    """Write a single row of files to destination directory or S3.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory or S3 path
        id: Unique identifier for this set of files
        options: Options instance for S3 operations

    Returns:
        Dictionary mapping filenames to their final destination paths or S3 URLs

    Raises:
        FileDatasetError: If any required files cannot be written
    """
    # Validate inputs
    if not row:
        return {}

    # Validate all non-None source files exist before starting
    file_errors = _validate_source_files(row)
    if file_errors:
        raise FileDatasetError(file_errors, "Cannot write files to destination")

    # Check if destination is S3
    if is_s3_url(str(into_path)):
        # S3 path handling
        bucket, prefix = parse_s3_url(str(into_path))
        s3_key_prefix = f"{prefix}/{id}" if prefix else id
        return _upload_files_to_s3(row, bucket, s3_key_prefix, options)
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
    result, copy_errors = _copy_files_to_destination(row, dest_dir)

    # If any copies failed, raise error
    if copy_errors:
        raise FileDatasetError(copy_errors, "Failed to copy some files")

    return result


def _write_dataframe_files(
    dataframe: pd.DataFrame,
    into_path: str | Path,
    options: Options | None = None,
) -> pd.DataFrame:
    """Write files from DataFrame to destination directories.

    Args:
        dataframe: DataFrame where each row contains files to write
        into_path: Base destination directory or S3 path
        options: Options instance for S3 operations

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
    row: dict[str, str | Path | None] | None = None,
    *,
    into_path: str | Path,
    id: str | None = None,  # noqa: A002
    dataframe: pd.DataFrame | None = None,
    options: Options | None = None,
) -> dict[str, Path | None] | pd.DataFrame:
    """Write files to destination directory with ID-based organization.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory or S3 path (keyword-only)
        id: Unique identifier for this set of files (keyword-only, required with row)
        dataframe: DataFrame where each row contains files to write (keyword-only)
        options: Options instance for S3 operations (keyword-only, required for S3)

    Returns:
        Dictionary mapping filenames to destination paths (when row is provided)
        or DataFrame with id column and file columns (when dataframe is provided)

    Raises:
        FileDatasetError: If any required files cannot be written
        ValueError: If both row and dataframe are provided, or if neither is provided
        ValueError: If S3 path is provided without options
    """
    # Check mutual exclusivity
    if row is not None and dataframe is not None:
        msg = "Cannot specify both 'row' and 'dataframe' arguments"
        raise ValueError(msg)

    if row is None and dataframe is None:
        msg = "Must specify either 'row' or 'dataframe' argument"
        raise ValueError(msg)

    # Initialize default options if S3 path is used
    if is_s3_url(str(into_path)):
        if options is None:
            options = Options.default()
        # Validate S3 URL format
        parsed = parse_s3_url(str(into_path))
        if not parsed or not parsed[0]:  # Check for empty bucket name
            msg = f"Invalid S3 URL format: {into_path}"
            raise ValueError(msg)

    # Handle single row case
    if row is not None:
        if id is None:
            msg = "Must specify 'id' when using 'row' argument"
            raise ValueError(msg)
        return _write_row_files(row, into_path, id, options)

    # Handle DataFrame case
    return _write_dataframe_files(dataframe, into_path, options)
