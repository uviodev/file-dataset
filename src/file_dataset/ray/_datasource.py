"""Ray datasource implementation for file datasets."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

import ray.data
from ray.data.block import BlockMetadata

from file_dataset.core import reader
from file_dataset.options import Options

logger = logging.getLogger(__name__)


class FileDataFrameAsBlobDatasource(ray.data.Datasource):
    """Ray datasource that loads file datasets as blob tables.

    This datasource converts a file DataFrame into Ray dataset where each
    row contains the binary contents of files. It uses the into_blob_table()
    functionality to load file contents into PyArrow tables.

    Note:
        This datasource ignores Ray's parallelism parameter and creates
        read tasks based on the specified batch_size to prevent out-of-memory
        errors when loading large files as binary data.
    """

    def __init__(
        self,
        file_dataframe: "pd.DataFrame",
        batch_size: int,
        options: Options | None = None,
    ) -> None:
        """Initialize datasource with file DataFrame configuration.

        Args:
            file_dataframe: DataFrame with 'id' column and file columns
            batch_size: Number of rows per batch (must be specified for memory control)
            options: Optional Options for S3 configuration

        Raises:
            ValueError: If file_dataframe is invalid (e.g., missing file columns)
        """
        # Import here to avoid circular imports
        from file_dataset.pipeline import _validate_and_get_file_dataframe_columns

        # Validate the DataFrame is a proper file DataFrame
        # This will raise ValueError if invalid
        if len(file_dataframe) > 0:  # Only validate if there are rows
            _validate_and_get_file_dataframe_columns(file_dataframe)

        self.dataframe = file_dataframe
        self.batch_size = batch_size
        self.options = options

    def estimate_inmemory_data_size(self) -> int | None:
        """Estimate memory size for entire dataset by extrapolating from first batch.

        Uses into_size_table() on the first batch to estimate memory requirements
        and multiplies by the number of batches in the dataset.
        This helps Ray with resource planning and scheduling.

        Returns:
            Estimated memory size in bytes for entire dataset, or None if unable to
            estimate
        """
        try:
            # Use first batch for estimation
            sample_df = self.dataframe.head(self.batch_size)
            if len(sample_df) == 0:
                return 0

            # Create reader for size estimation
            file_reader = reader(dataframe=sample_df, options=self.options)
            size_table = file_reader.into_size_table()

            # Sum all file sizes across rows and columns for the sample
            sample_total_size = 0
            for column_name in size_table.column_names:
                if column_name != "id":  # Skip id column
                    column_data = size_table[column_name].to_numpy()
                    sample_total_size += column_data.sum()

            # Calculate total number of batches (including fractional final batch)
            total_rows = len(self.dataframe)
            # Ceiling division
            num_batches = (total_rows + self.batch_size - 1) // self.batch_size

            # Extrapolate from sample to entire dataset
            return int(sample_total_size * num_batches)

        except Exception:
            logger.exception("Failed to estimate data size")
            return None

    def get_read_tasks(self, parallelism: int) -> list["ray.data.ReadTask"]:  # noqa: ARG002
        """Create read tasks for Ray dataset.

        Creates one read task per batch_size rows. Ignores the parallelism
        parameter as blob tables can cause memory issues if batches are too large.

        Args:
            parallelism: Ray's suggested parallelism (ignored for memory safety)

        Returns:
            List of ReadTask objects, one per batch
        """
        import ray.data

        tasks = []
        total_rows = len(self.dataframe)

        # Handle empty DataFrame case by creating a single task that returns empty table
        if total_rows == 0:
            task = ray.data.ReadTask(
                read_fn=lambda: self._read_empty_batch(),
                metadata=BlockMetadata(
                    num_rows=0,
                    size_bytes=0,
                    schema=None,  # Schema will be inferred from PyArrow table
                    input_files=None,
                    exec_stats=None,
                ),
            )
            tasks.append(task)
            return tasks

        # Create tasks in batch_size chunks
        for start_idx in range(0, total_rows, self.batch_size):
            end_idx = min(start_idx + self.batch_size, total_rows)
            batch_df = self.dataframe.iloc[start_idx:end_idx].copy()

            # Create read task for this batch
            task = ray.data.ReadTask(
                read_fn=lambda df=batch_df: self._read_batch(df),
                metadata=BlockMetadata(
                    num_rows=len(batch_df),
                    size_bytes=None,  # Will be estimated during execution
                    schema=None,  # Schema will be inferred from PyArrow table
                    input_files=None,
                    exec_stats=None,
                ),
            )
            tasks.append(task)

        return tasks

    def _read_batch(self, batch_df: "pd.DataFrame") -> list["pa.Table"]:
        """Read a batch of rows into PyArrow table with binary file contents.

        Args:
            batch_df: DataFrame batch to process

        Returns:
            List containing single PyArrow table with file contents as binary data

        Raises:
            Exception: If batch processing fails entirely
        """
        # Create reader for this batch
        file_reader = reader(dataframe=batch_df, options=self.options)

        # Convert to blob table (into_blob_table handles partial failures)
        table = file_reader.into_blob_table()

        # Return as list (iterable) as required by Ray
        return [table]

    def _read_empty_batch(self) -> list["pa.Table"]:
        """Read empty batch for empty DataFrames.

        Returns:
            List containing single empty PyArrow table with correct schema
        """
        import pyarrow as pa

        # Create empty table with just id column for empty DataFrames
        # Note: We can't determine file columns from empty DataFrame, so just use id
        schema = pa.schema([("id", pa.string())])
        empty_table = pa.table({"id": []}, schema=schema)

        return [empty_table]
