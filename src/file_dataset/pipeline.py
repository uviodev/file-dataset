"""Pipeline class for chaining file dataset operations."""

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from ._reader import file_dataframe_reader
from ._writer import write_files
from .exceptions import FileDatasetError
from .file_dataframe import get_file_columns, validate_file_dataframe
from .s3_options import S3Options

logger = logging.getLogger(__name__)


class Pipeline:
    """Pipeline for batch processing of file datasets.

    Combines read → process → write workflow in a streaming manner.
    Each row is processed independently to avoid memory issues.
    Failed rows are dropped rather than failing the entire batch.

    The input DataFrame must have an 'id' column and file columns with
    file extensions (e.g., 'image.jpg', 'mask.png', 'data.csv').

    Examples:
        >>> def process_images(temp_dir: Path) -> dict[str, Path]:
        ...     # Process files in temp_dir and return results
        ...     processed_file = temp_dir / "output.jpg"
        ...     # ... processing logic ...
        ...     return {"result.jpg": processed_file}

        >>> # DataFrame with proper file columns
        >>> df = pd.DataFrame([
        ...     {
        ...         "id": "row1",
        ...         "image.jpg": "/path/to/image.jpg",
        ...         "mask.png": "/path/to/mask.png"
        ...     }
        ... ])
        >>> pipeline = Pipeline(process_images, into_path="/output", options=options)
        >>> result_df = pipeline(df)
    """

    def __init__(
        self,
        fn: Callable[[Path], dict[str, Path | None]],
        *,
        into_path: str | Path,
        options: S3Options | None = None,
    ) -> None:
        """Initialize Pipeline with processing function and write options.

        Args:
            fn: User function that takes a temp directory and returns file mapping
            into_path: Base destination directory or S3 path for outputs (keyword-only)
            options: S3Options instance for S3 operations (keyword-only).
                If None, defaults to S3Options.default() to ensure Pipeline
                can process many DataFrames without failing on S3 operations.
        """
        self.fn = fn
        self.into_path = into_path
        # Ensure options is never None to support processing many DataFrames
        self.options = options if options is not None else S3Options.default()

    def __call__(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Process DataFrame through the pipeline.

        Args:
            dataframe: DataFrame with 'id' column and file columns (with extensions)

        Returns:
            DataFrame with results for successfully processed rows

        Raises:
            ValueError: If DataFrame missing 'id' column or no valid file columns found
            FileDatasetError: If all rows fail to process
        """
        # Validate input DataFrame and get file columns
        validate_file_dataframe(dataframe)
        file_columns = get_file_columns(dataframe)

        # Create filtered DataFrame with only valid file columns and id
        filtered_columns = ["id", *file_columns]
        filtered_dataframe = dataframe[filtered_columns].copy()

        # Create reader for processing
        df_reader = file_dataframe_reader(filtered_dataframe, options=self.options)

        # Process each row
        successful_rows = []
        total_rows = len(dataframe)
        successful_count = 0

        try:
            for row_id, temp_dir in df_reader.into_temp_dir():
                try:
                    # Call user function to process files
                    result_files = self.fn(temp_dir)

                    # Write results using write_files
                    write_result = write_files(
                        result_files,
                        into_path=self.into_path,
                        id=row_id,
                        options=self.options,
                    )

                    # Build successful row data
                    row_data = {"id": row_id}
                    row_data.update(write_result)
                    successful_rows.append(row_data)
                    successful_count += 1

                except Exception as e:  # noqa: BLE001
                    # Log the error and continue with next row
                    logger.warning("Failed to process row %s: %s", row_id, e)
                    continue

        except FileDatasetError as e:
            # If reader fails completely (e.g., all rows fail to read)
            if "All rows failed" in str(e):
                raise FileDatasetError(
                    {"all_rows": "All rows failed to process"},
                    "All rows failed during reading. Check error logs for details.",
                ) from e
            raise

        # If all rows failed during processing, raise an error
        if successful_count == 0 and total_rows > 0:
            raise FileDatasetError(
                {"all_rows": "All rows failed to process"},
                "All rows failed during processing. Check error logs for details.",
            )

        # Return DataFrame with successful results
        if successful_rows:
            return pd.DataFrame(successful_rows)
        # Return empty DataFrame with expected structure
        return pd.DataFrame(columns=["id"])
