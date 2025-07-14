"""Core file dataset operations."""

import logging
import re
import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from .exceptions import FileDatasetError
from .options import Options

logger = logging.getLogger(__name__)


def _parse_s3_url(url: str) -> tuple[str, str] | None:
    """Parse S3 URL into bucket and key.

    Args:
        url: URL to parse

    Returns:
        Tuple of (bucket, key) if valid S3 URL, None otherwise
    """
    match = re.match(r"^s3://([^/]+)/(.+)$", url)
    if match:
        return match.group(1), match.group(2)
    return None


def _is_s3_url(path: str | Path) -> bool:
    """Check if path is an S3 URL.

    Args:
        path: Path to check

    Returns:
        True if path starts with s3://, False otherwise
    """
    return str(path).startswith("s3://")


class Reader:
    """Handles reading files from local filesystem with context manager support."""

    def __init__(
        self, files_dict: dict[str, str | Path], options: Options | None = None
    ) -> None:
        """Initialize Reader with files dictionary.

        Args:
            files_dict: Dictionary mapping filenames to their source paths
            options: Optional Options instance for S3 operations
        """
        self.files_dict = files_dict
        self.options = options

    def _validate_s3_file(self, filename: str, source_str: str) -> str | None:  # noqa: ARG002
        """Validate a single S3 file.

        Args:
            filename: The filename being validated
            source_str: The S3 URL string

        Returns:
            Error message if validation fails, None if successful
        """
        # Validate S3 URL format
        parsed = _parse_s3_url(source_str)
        if not parsed:
            return f"Invalid S3 URL format: {source_str}"

        # Check if options are provided for S3
        if self.options is None:
            return "Options required for S3 URLs but not provided"

        # Validate S3 object exists using HEAD request
        bucket, key = parsed
        try:
            self.options.s3_client.head_object(Bucket=bucket, Key=key)
        except Exception as e:  # noqa: BLE001
            # Check for 404 error (object not found)
            if (
                hasattr(e, "response")
                and e.response.get("Error", {}).get("Code") == "404"
            ):
                return f"S3 object not found: {source_str}"
            return f"S3 access error: {e}"

        return None

    def _validate_files(self) -> dict[str, str]:
        """Validate all files exist and are accessible.

        Returns:
            Dictionary of filename to error message for any validation failures
        """
        file_errors: dict[str, str] = {}
        has_s3_files = False

        for filename, source_path in self.files_dict.items():
            source_str = str(source_path)

            if _is_s3_url(source_str):
                has_s3_files = True
                error = self._validate_s3_file(filename, source_str)
                if error:
                    file_errors[filename] = error
            else:
                # Local file validation
                source = Path(source_path)
                if not source.exists():
                    file_errors[filename] = f"Source file not found: {source}"
                elif not source.is_file():
                    file_errors[filename] = f"Source path is not a file: {source}"

        # Check if we have S3 URLs but no options
        if has_s3_files and self.options is None and not file_errors:
            file_errors["__options__"] = "Options required for S3 URLs but not provided"

        return file_errors

    @contextmanager
    def into_temp_dir(self) -> Generator[Path, None, None]:
        """Copy files to temporary directory and yield the directory path.

        Returns:
            Path to temporary directory containing copied files

        Raises:
            FileDatasetError: If any files cannot be copied
        """
        # Validate all files exist before starting copy operation
        file_errors = self._validate_files()

        if file_errors:
            raise FileDatasetError(
                file_errors, "Cannot copy files to temporary directory"
            )

        # Create temporary directory and copy files
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)

            # Copy each file to temp directory with original filename
            for filename, source_path in self.files_dict.items():
                source_str = str(source_path)
                dest = temp_dir / filename

                try:
                    if _is_s3_url(source_str):
                        # Download from S3
                        bucket, key = _parse_s3_url(source_str)
                        self.options.s3_client.download_file(bucket, key, str(dest))
                    else:
                        # Local file copy
                        source = Path(source_path)
                        shutil.copy2(source, dest)
                except OSError as e:
                    file_errors[filename] = f"Failed to copy file: {e}"
                except Exception as e:  # noqa: BLE001
                    file_errors[filename] = f"Failed to download file: {e}"

            # If any copies failed, raise error
            if file_errors:
                raise FileDatasetError(file_errors, "Failed to copy some files")

            yield temp_dir


def reader(
    *,
    row: dict[str, str | Path] | None = None,
    dataframe: pd.DataFrame | None = None,
    options: Options | None = None,
) -> "Reader | FileDataFrameReader":
    """Create a Reader instance for the given files or DataFrame.

    Args:
        row: Dictionary mapping filenames to their source paths (keyword-only)
        dataframe: DataFrame where each row contains files to process (keyword-only)
        options: Optional Options instance for S3 operations (keyword-only)

    Returns:
        Reader or FileDataFrameReader instance

    Raises:
        ValueError: If both row and dataframe are provided, or if neither is provided
    """
    # Check mutual exclusivity
    if row is not None and dataframe is not None:
        msg = "Cannot specify both 'row' and 'dataframe' arguments"
        raise ValueError(msg)

    if row is None and dataframe is None:
        msg = "Must specify either 'row' or 'dataframe' argument"
        raise ValueError(msg)

    # Return appropriate reader
    if dataframe is not None:
        return FileDataFrameReader(dataframe, options)
    return Reader(row, options)


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


def _write_row_files(
    row: dict[str, str | Path | None],
    into_path: str | Path,
    id: str,  # noqa: A002
) -> dict[str, Path | None]:
    """Write a single row of files to destination directory.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory
        id: Unique identifier for this set of files

    Returns:
        Dictionary mapping filenames to their final destination paths or None

    Raises:
        FileDatasetError: If any required files cannot be written
    """
    # Validate inputs
    if not row:
        return {}

    dest_base = Path(into_path)
    dest_dir = dest_base / id

    # Validate all non-None source files exist before starting
    file_errors = _validate_source_files(row)
    if file_errors:
        raise FileDatasetError(file_errors, "Cannot write files to destination")

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
) -> pd.DataFrame:
    """Write files from DataFrame to destination directories.

    Args:
        dataframe: DataFrame where each row contains files to write
        into_path: Base destination directory

    Returns:
        DataFrame with id column and file columns for successful writes

    Raises:
        FileDatasetError: If all rows fail to write
        ValueError: If DataFrame doesn't have an id column

    Note:
        S3 upload support is not yet implemented. The `options` parameter
        will be added to support S3 uploads in the future.
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
            result = _write_row_files(file_columns, into_path, row_id)

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
) -> dict[str, Path | None] | pd.DataFrame:
    """Write files to destination directory with ID-based organization.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory (keyword-only)
        id: Unique identifier for this set of files (keyword-only, required with row)
        dataframe: DataFrame where each row contains files to write (keyword-only)

    Returns:
        Dictionary mapping filenames to destination paths (when row is provided)
        or DataFrame with id column and file columns (when dataframe is provided)

    Raises:
        FileDatasetError: If any required files cannot be written
        ValueError: If both row and dataframe are provided, or if neither is provided
    """
    # Check mutual exclusivity
    if row is not None and dataframe is not None:
        msg = "Cannot specify both 'row' and 'dataframe' arguments"
        raise ValueError(msg)

    if row is None and dataframe is None:
        msg = "Must specify either 'row' or 'dataframe' argument"
        raise ValueError(msg)

    # Handle single row case
    if row is not None:
        if id is None:
            msg = "Must specify 'id' when using 'row' argument"
            raise ValueError(msg)
        return _write_row_files(row, into_path, id)

    # Handle DataFrame case
    return _write_dataframe_files(dataframe, into_path)


class FileDataFrameReader:
    """Handles reading files from DataFrames with row-level error handling."""

    def __init__(self, dataframe: pd.DataFrame, options: Options | None = None) -> None:
        """Initialize FileDataFrameReader with a DataFrame.

        Args:
            dataframe: DataFrame where each row represents files to process
            options: Optional Options instance for S3 operations
        """
        self.dataframe = dataframe
        self.options = options

    def into_temp_dir(self) -> Generator[Path, None, None]:
        """Yield temporary directories for each successful row in the DataFrame.

        Yields:
            Path to temporary directory for each successful row

        Raises:
            FileDatasetError: If all rows fail to process
        """
        total_rows = len(self.dataframe)
        successful_rows = 0

        for idx, row in self.dataframe.iterrows():
            # Get row identifier for logging
            row_id = row.get("id", idx)

            # Convert row to dict, excluding 'id' column if present
            row_dict = row.to_dict()
            if "id" in row_dict:
                del row_dict["id"]

            # Create Reader for this row
            row_reader = Reader(row_dict, self.options)

            # Try to process this row
            try:
                with row_reader.into_temp_dir() as temp_dir:
                    successful_rows += 1
                    yield temp_dir
            except FileDatasetError as e:
                # Log the error with row identifier
                logger.warning(
                    "Failed to process row %s: %s. File errors: %s",
                    row_id,
                    e,
                    e.file_errors,
                )
                continue
            except Exception as e:  # noqa: BLE001
                # Log unexpected errors
                logger.warning("Unexpected error processing row %s: %s", row_id, e)
                continue

        # If all rows failed, raise an error
        if successful_rows == 0 and total_rows > 0:
            raise FileDatasetError(
                {"all_rows": "All rows failed to process"},
                "All rows failed. Check error logs for details.",
            )
