"""Reader functionality for file datasets."""

import logging
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pyarrow as pa

from ._core_file import (
    copy_each_file,
    read_each_file_contents,
    read_each_file_size,
    validate_each_file,
)
from .exceptions import FileDatasetError
from .s3_options import S3Options
from .s3_utils import is_s3_url

logger = logging.getLogger(__name__)


class FileRowReader:
    """Handles reading files from local filesystem with context manager support."""

    def __init__(
        self, files_dict: dict[str, str | Path], options: S3Options | None = None
    ) -> None:
        """Initialize FileRowReader with files dictionary.

        Args:
            files_dict: Dictionary mapping filenames to their source paths
            options: Optional S3Options instance for S3 operations
                (defaults to S3Options.default())
        """
        self.files_dict = files_dict
        # Initialize default options if none provided and S3 files are present
        has_s3_files = any(is_s3_url(str(path)) for path in files_dict.values())
        if options is None and has_s3_files:
            self.options = S3Options.default()
        else:
            self.options = options

    @contextmanager
    def into_temp_dir(self) -> Generator[Path, None, None]:
        """Copy files to temporary directory and yield the directory path.

        Returns:
            Path to temporary directory containing copied files

        Raises:
            FileDatasetError: If any files cannot be copied
        """
        # Validate all files exist before starting copy operation
        file_errors = validate_each_file(self.files_dict, self.options)

        if file_errors:
            raise FileDatasetError(
                file_errors, "Cannot copy files to temporary directory"
            )

        # Create temporary directory and copy files
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)

            # Prepare copies for do_copy
            copies = []
            for filename, source_path in self.files_dict.items():
                source_str = str(source_path)
                dest = temp_dir / filename
                copies.append((source_str, str(dest)))

            # Perform all copies at once
            try:
                copy_each_file(copies, s3_options=self.options)
            except FileDatasetError as e:
                # Extract file errors from the exception
                # copy_each_file uses index as key, we need to map back to filenames
                file_errors = {}
                filenames = list(self.files_dict.keys())
                for i, filename in enumerate(filenames):
                    if str(i) in e.file_errors:
                        file_errors[filename] = e.file_errors[str(i)]
                raise FileDatasetError(file_errors, "Failed to copy some files") from e

            yield temp_dir


class FileDataFrameReader:
    """Handles reading files from DataFrames with row-level error handling."""

    def __init__(
        self, dataframe: pd.DataFrame, options: S3Options | None = None
    ) -> None:
        """Initialize FileDataFrameReader with a DataFrame.

        Args:
            dataframe: DataFrame where each row represents files to process
            options: Optional S3Options instance for S3 operations
                (defaults to S3Options.default())
        """
        self.dataframe = dataframe
        # Initialize default options if none provided and S3 files are present
        has_s3_files = self._dataframe_has_s3_files(dataframe)
        if options is None and has_s3_files:
            self.options = S3Options.default()
        else:
            self.options = options

    def _dataframe_has_s3_files(self, dataframe: pd.DataFrame) -> bool:
        """Check if DataFrame contains any S3 URLs.

        Args:
            dataframe: DataFrame to check

        Returns:
            True if any file columns contain S3 URLs
        """
        for column in dataframe.columns:
            if column != "id":  # Skip the id column
                for value in dataframe[column]:
                    if value is not None and is_s3_url(str(value)):
                        return True
        return False

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

            # Create FileRowReader for this row
            row_reader = FileRowReader(row_dict, self.options)

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

    def into_temp_dirs(self) -> Generator[tuple[str, Path], None, None]:
        """Yield (row_id, temporary directory) tuples for each successful row.

        This is an alias for into_temp_dir() with a more descriptive name.

        Yields:
            Tuple of (row_id, Path) for each successful row

        Raises:
            FileDatasetError: If all rows fail to process
        """
        yield from self.into_temp_dir()

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

        # Filter out None values
        files_to_read = {
            filename: str(file_path)
            for filename, file_path in row_dict.items()
            if file_path is not None
        }

        if not files_to_read:
            return row_data

        # Make a single call to read all file sizes
        sizes, file_errors = read_each_file_size(files_to_read, s3_options=self.options)
        row_data.update(sizes)

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

        # Filter out None values
        files_to_read = {
            filename: str(file_path)
            for filename, file_path in row_dict.items()
            if file_path is not None
        }

        if not files_to_read:
            return row_data

        # Make a single call to read all file contents
        contents, file_errors = read_each_file_contents(
            files_to_read, s3_options=self.options
        )
        row_data.update(contents)

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


def row_reader(
    row: dict[str, str | Path], options: S3Options | None = None
) -> FileRowReader:
    """Create a FileRowReader instance for the given row of files.

    Args:
        row: Dictionary mapping filenames to their source paths
        options: Optional S3Options instance for S3 operations

    Returns:
        FileRowReader instance
    """
    return FileRowReader(row, options)


def file_dataframe_reader(
    dataframe: pd.DataFrame, options: S3Options | None = None
) -> FileDataFrameReader:
    """Create a FileDataFrameReader instance for the given DataFrame.

    Args:
        dataframe: DataFrame where each row contains files to process
        options: Optional S3Options instance for S3 operations

    Returns:
        FileDataFrameReader instance
    """
    return FileDataFrameReader(dataframe, options)
