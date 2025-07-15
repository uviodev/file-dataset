"""Ray integration for file datasets."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import ray.data

from file_dataset.file_dataframe import validate_id_column, validate_unique_ids
from file_dataset.s3_options import S3Options

from ._datasource import FileDataFrameAsBlobDatasource


def read_file_dataset(
    file_dataframe: "pd.DataFrame",
    batch_size: int,
    options: S3Options | None = None,
) -> "ray.data.Dataset":
    """Create Ray dataset from file DataFrame using blob table format.

    Args:
        file_dataframe: DataFrame with 'id' column and file columns.
            Each row represents a set of files to load as binary data.
        batch_size: Number of rows to process per batch. Must be specified
            as blob tables can cause out-of-memory errors if too large.
        options: Optional S3Options for S3 configuration

    Returns:
        Ray Dataset with schema matching into_blob_table():
        {id: string, filename: binary} where filename columns contain
        file contents as binary data

    Raises:
        ValueError: If dataframe missing 'id' column or 'id' values not unique

    Note:
        This datasource ignores Ray's parallelism parameter and creates
        read tasks with the specified batch_size to prevent memory issues
        with large blob tables.
    """
    import ray.data

    # Validate DataFrame has unique 'id' column
    validate_id_column(file_dataframe)
    validate_unique_ids(file_dataframe)

    # Create and configure datasource
    datasource = FileDataFrameAsBlobDatasource(
        file_dataframe=file_dataframe,
        batch_size=batch_size,
        options=options,
    )

    # Create Ray dataset
    return ray.data.read_datasource(datasource)


__all__ = ["read_file_dataset"]
