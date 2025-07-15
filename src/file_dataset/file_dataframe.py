"""File DataFrame validation and utility functions.

This module consolidates validation logic for file DataFrames, which are
pandas DataFrames where columns represent file names and rows contain
S3 URLs pointing to files.
"""

import pandas as pd


def _has_file_extension(column_name: str) -> bool:
    """Check if column name has a valid file extension.

    Args:
        column_name: Name of the column to check

    Returns:
        True if column name has a file extension (dot followed by alphabetic characters)
    """
    return "." in column_name and column_name.split(".")[-1].isalpha()


def _get_potential_file_columns(dataframe: pd.DataFrame) -> list[str]:
    """Get all columns that could potentially be file columns (excluding 'id').

    Args:
        dataframe: DataFrame to analyze

    Returns:
        List of column names excluding 'id' column
    """
    return [col for col in dataframe.columns if col != "id"]


def get_file_columns(dataframe: pd.DataFrame) -> list[str]:
    """Get file columns from DataFrame (columns with file extensions).

    Args:
        dataframe: DataFrame to analyze

    Returns:
        List of column names that have file extensions

    Raises:
        ValueError: If no valid file columns found
    """
    potential_file_columns = _get_potential_file_columns(dataframe)

    # Filter to only columns that look like file names (have file extensions)
    file_columns = []
    for col in potential_file_columns:
        if _has_file_extension(col):
            file_columns.append(col)

    if not file_columns:
        msg = (
            "No valid file columns found in DataFrame. "
            "File columns must have file extensions "
            "(e.g., 'file.txt', 'image.jpg', 'data.csv')"
        )
        raise ValueError(msg)

    return file_columns


def validate_id_column(dataframe: pd.DataFrame) -> None:
    """Validate DataFrame has required 'id' column.

    Args:
        dataframe: DataFrame to validate

    Raises:
        ValueError: If 'id' column is missing
    """
    if "id" not in dataframe.columns:
        msg = "DataFrame must have an 'id' column"
        raise ValueError(msg)


def validate_unique_ids(dataframe: pd.DataFrame) -> None:
    """Validate DataFrame has unique values in 'id' column.

    Args:
        dataframe: DataFrame to validate

    Raises:
        ValueError: If 'id' column has duplicate values
    """
    if len(dataframe) > 0 and dataframe["id"].duplicated().any():
        msg = "DataFrame 'id' column must have unique values"
        raise ValueError(msg)


def validate_file_dataframe(dataframe: pd.DataFrame) -> None:
    """Perform complete validation of file DataFrame.

    Validates that DataFrame has:
    - Required 'id' column
    - Unique values in 'id' column
    - At least one file column (column with file extension)

    Args:
        dataframe: DataFrame to validate

    Raises:
        ValueError: If any validation check fails
    """
    validate_id_column(dataframe)
    validate_unique_ids(dataframe)
    # This will raise ValueError if no file columns found
    get_file_columns(dataframe)
