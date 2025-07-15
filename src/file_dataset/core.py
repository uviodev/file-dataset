"""Core file dataset operations."""

import logging
import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pyarrow as pa

from .exceptions import FileDatasetError
from .options import Options
from .s3_utils import is_s3_url, parse_s3_url

logger = logging.getLogger(__name__)


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
        parsed = parse_s3_url(source_str)
        if not parsed:
            return f"Invalid S3 URL format: {source_str}"

        bucket, key = parsed
        if not key:  # Reader needs a key to read a file
            return f"Invalid S3 URL format: {source_str}"

        # Check if options are provided for S3
        if self.options is None:
            return "Options required for S3 URLs but not provided"

        # Validate S3 object exists using HEAD request
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

            if is_s3_url(source_str):
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
                    if is_s3_url(source_str):
                        # Download from S3
                        bucket, key = parse_s3_url(source_str)
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

    def into_size_table(self, head: int | None = None) -> pa.Table:
        """Create PyArrow table with file sizes.

        Args:
            head: Limit to first N rows (not applicable for single Reader)

        Raises:
            NotImplementedError: Size table not supported for single-row Reader
        """
        raise NotImplementedError("Size table not supported for single-row Reader")

    def into_blob_table(self, head: int | None = None) -> pa.Table:
        """Create PyArrow table with file contents as binary data.

        Args:
            head: Limit to first N rows (not applicable for single Reader)

        Raises:
            NotImplementedError: Blob table not supported for single-row Reader
        """
        raise NotImplementedError("Blob table not supported for single-row Reader")


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

    # Check if S3 path requires options
    if is_s3_url(str(into_path)):
        if options is None:
            msg = "Options required for S3 paths"
            raise ValueError(msg)
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

    def into_temp_dir(self) -> Generator[tuple[str, Path], None, None]:
        """Yield (row_id, temporary directory) tuples for each successful row.

        Yields:
            Tuple of (row_id, Path) for each successful row

        Raises:
            FileDatasetError: If all rows fail to process
        """
        total_rows = len(self.dataframe)
        successful_rows = 0

        for idx, row in self.dataframe.iterrows():
            # Get row identifier for logging
            row_id = row.get("id", str(idx))

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
                    yield row_id, temp_dir
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

    def _get_file_size(self, file_path: str) -> int:
        """Get file size for local or S3 file.

        Args:
            file_path: Path to file (local or S3 URL)

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If S3 URL is invalid or options missing
            Exception: For other errors accessing file
        """
        if is_s3_url(file_path):
            if self.options is None:
                msg = "No S3 options"
                raise ValueError(msg)

            bucket, key = parse_s3_url(file_path)
            if not bucket or not key:
                msg = "Invalid S3 URL"
                raise ValueError(msg)

            response = self.options.s3_client.head_object(Bucket=bucket, Key=key)
            return response["ContentLength"]
        return Path(file_path).stat().st_size

    def _process_row_for_sizes(
        self, row_id: str, row_dict: dict
    ) -> dict[str, str | int]:
        """Process a single row to collect file sizes.

        Args:
            row_id: Row identifier for logging
            row_dict: Row data excluding id column

        Returns:
            Dictionary with row_data and any file errors
        """
        row_data = {"id": row_id}
        file_errors = {}

        for filename, file_path in row_dict.items():
            if file_path is None:
                continue  # Skip None values (optional files)

            try:
                file_size = self._get_file_size(str(file_path))
                row_data[filename] = file_size
            except (FileNotFoundError, ValueError) as e:
                file_errors[filename] = str(e)
            except Exception as e:  # noqa: BLE001
                file_errors[filename] = f"Failed to get file size: {e}"

        # Log errors if any
        if file_errors:
            if len(row_data) > 1:  # Has some successful files
                logger.warning(
                    "Some files failed for row %s. File errors: %s",
                    row_id,
                    file_errors,
                )
            else:
                logger.warning(
                    "Failed to get sizes for any files in row %s. File errors: %s",
                    row_id,
                    file_errors,
                )

        return row_data

    def _build_size_table_schema(self, result_df: pd.DataFrame) -> pa.Schema:
        """Build PyArrow schema for size table.

        Args:
            result_df: DataFrame with results

        Returns:
            PyArrow schema
        """
        schema_fields = [("id", pa.string())]
        for col in result_df.columns:
            if col != "id":
                schema_fields.append((col, pa.int64()))
        return pa.schema(schema_fields)

    def into_size_table(self, head: int | None = None) -> pa.Table:
        """Create PyArrow table containing file size metadata.

        Args:
            head: Limit to first N rows of DataFrame, None for all rows

        Returns:
            PyArrow table with schema {id: string, filename: int64} where
            filename columns contain file sizes in bytes

        Raises:
            FileDatasetError: If all rows fail to process
        """
        # Limit rows if head parameter specified
        dataframe = self.dataframe.head(head) if head is not None else self.dataframe

        total_rows = len(dataframe)
        if total_rows == 0:
            # Return empty table with correct schema
            schema = pa.schema([("id", pa.string())])
            return pa.table({}, schema=schema)

        successful_rows = []

        for idx, row in dataframe.iterrows():
            # Get row identifier for logging
            row_id = row.get("id", str(idx))

            # Convert row to dict, excluding 'id' column if present
            row_dict = row.to_dict()
            if "id" in row_dict:
                del row_dict["id"]

            try:
                row_data = self._process_row_for_sizes(row_id, row_dict)

                # If this row had any successful file size retrievals, include it
                if len(row_data) > 1:  # More than just 'id'
                    successful_rows.append(row_data)

            except Exception as e:  # noqa: BLE001
                # Log unexpected errors
                logger.warning("Unexpected error processing row %s: %s", row_id, e)
                continue

        # If all rows failed, raise an error
        if not successful_rows and total_rows > 0:
            raise FileDatasetError(
                {"all_rows": "All rows failed to process"},
                "All rows failed. Check error logs for details.",
            )

        # Build PyArrow table from successful rows
        if not successful_rows:
            # Return empty table with just id column
            schema = pa.schema([("id", pa.string())])
            return pa.table({}, schema=schema)

        # Convert to DataFrame then to PyArrow table
        result_df = pd.DataFrame(successful_rows)
        schema = self._build_size_table_schema(result_df)

        # Convert to PyArrow table with explicit schema
        return pa.table(result_df, schema=schema)

    def _get_file_content(self, file_path: str) -> bytes:
        """Get file content for local or S3 file.

        Args:
            file_path: Path to file (local or S3 URL)

        Returns:
            File content as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If S3 URL is invalid or options missing
            Exception: For other errors accessing file
        """
        if is_s3_url(file_path):
            if self.options is None:
                msg = "No S3 options"
                raise ValueError(msg)

            bucket, key = parse_s3_url(file_path)
            if not bucket or not key:
                msg = "Invalid S3 URL"
                raise ValueError(msg)

            response = self.options.s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        return Path(file_path).read_bytes()

    def _process_row_for_blobs(
        self, row_id: str, row_dict: dict
    ) -> dict[str, str | bytes]:
        """Process a single row to collect file contents as binary data.

        Args:
            row_id: Row identifier for logging
            row_dict: Row data excluding id column

        Returns:
            Dictionary with row_data and any file errors
        """
        row_data = {"id": row_id}
        file_errors = {}

        for filename, file_path in row_dict.items():
            if file_path is None:
                continue  # Skip None values (optional files)

            try:
                file_content = self._get_file_content(str(file_path))
                row_data[filename] = file_content
            except (FileNotFoundError, ValueError) as e:
                file_errors[filename] = str(e)
            except Exception as e:  # noqa: BLE001
                file_errors[filename] = f"Failed to get file content: {e}"

        # Log errors if any
        if file_errors:
            if len(row_data) > 1:  # Has some successful files
                logger.warning(
                    "Some files failed for row %s. File errors: %s",
                    row_id,
                    file_errors,
                )
            else:
                logger.warning(
                    "Failed to get content for any files in row %s. File errors: %s",
                    row_id,
                    file_errors,
                )

        return row_data

    def _build_blob_table_schema(self, result_df: pd.DataFrame) -> pa.Schema:
        """Build PyArrow schema for blob table.

        Args:
            result_df: DataFrame with results

        Returns:
            PyArrow schema
        """
        schema_fields = [("id", pa.string())]
        for col in result_df.columns:
            if col != "id":
                schema_fields.append((col, pa.binary()))
        return pa.schema(schema_fields)

    def into_blob_table(self, head: int | None = None) -> pa.Table:
        """Create PyArrow table containing file contents as binary data.

        Args:
            head: Limit to first N rows of DataFrame, None for all rows

        Returns:
            PyArrow table with schema {id: string, filename: binary} where
            filename columns contain file contents as binary data

        Raises:
            FileDatasetError: If all rows fail to process
        """
        # Limit rows if head parameter specified
        dataframe = self.dataframe.head(head) if head is not None else self.dataframe

        total_rows = len(dataframe)
        if total_rows == 0:
            # Return empty table with correct schema
            schema = pa.schema([("id", pa.string())])
            return pa.table({"id": []}, schema=schema)

        successful_rows = []

        for idx, row in dataframe.iterrows():
            # Get row identifier for logging
            row_id = row.get("id", str(idx))

            # Convert row to dict, excluding 'id' column if present
            row_dict = row.to_dict()
            if "id" in row_dict:
                del row_dict["id"]

            try:
                row_data = self._process_row_for_blobs(row_id, row_dict)

                # If this row had any successful file content retrievals, include it
                if len(row_data) > 1:  # More than just 'id'
                    successful_rows.append(row_data)

            except Exception as e:  # noqa: BLE001
                # Log unexpected errors
                logger.warning("Unexpected error processing row %s: %s", row_id, e)
                continue

        # If all rows failed, raise an error
        if not successful_rows and total_rows > 0:
            raise FileDatasetError(
                {"all_rows": "All rows failed to process"},
                "All rows failed. Check error logs for details.",
            )

        # Build PyArrow table from successful rows
        if not successful_rows:
            # Return empty table with just id column
            schema = pa.schema([("id", pa.string())])
            return pa.table({"id": []}, schema=schema)

        # Convert to DataFrame then to PyArrow table
        result_df = pd.DataFrame(successful_rows)
        schema = self._build_blob_table_schema(result_df)

        # Convert to PyArrow table with explicit schema
        return pa.table(result_df, schema=schema)
