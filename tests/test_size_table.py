"""Tests for size table functionality."""

import pandas as pd
import pyarrow as pa
import pytest
from moto import mock_aws

import file_dataset
from file_dataset.options import Options


def test_dataframe_reader_into_size_table_local_files(tmp_path):
    """Test creating size table for local files."""
    # Create test files with known sizes
    test_file1 = tmp_path / "file1.txt"
    test_file2 = tmp_path / "file2.txt"
    test_file1.write_text("hello")  # 5 bytes
    test_file2.write_text("world!")  # 6 bytes

    # Create DataFrame
    df = pd.DataFrame(
        {"id": ["row1"], "data": [str(test_file1)], "config": [str(test_file2)]}
    )

    # Create reader and get size table
    reader = file_dataset.reader(dataframe=df)
    table = reader.into_size_table()

    # Verify table structure
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1

    # Convert to pandas for easier checking
    df_result = table.to_pandas()

    # Check schema
    assert "id" in df_result.columns
    assert "data" in df_result.columns
    assert "config" in df_result.columns

    # Check values
    assert df_result.iloc[0]["id"] == "row1"
    assert df_result.iloc[0]["data"] == 5
    assert df_result.iloc[0]["config"] == 6


@mock_aws
def test_dataframe_reader_into_size_table_s3_files():
    """Test creating size table for S3 files."""
    import boto3

    # Setup S3 mock
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")

    # Upload test files with known sizes
    s3_client.put_object(
        Bucket="test-bucket", Key="file1.txt", Body=b"hello"
    )  # 5 bytes
    s3_client.put_object(
        Bucket="test-bucket", Key="file2.txt", Body=b"world!"
    )  # 6 bytes

    # Create DataFrame with S3 URLs
    df = pd.DataFrame(
        {
            "id": ["row1"],
            "data": ["s3://test-bucket/file1.txt"],
            "config": ["s3://test-bucket/file2.txt"],
        }
    )

    # Create reader with options and get size table
    options = Options.default()
    reader = file_dataset.reader(dataframe=df, options=options)
    table = reader.into_size_table()

    # Verify table structure
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1

    # Convert to pandas for easier checking
    df_result = table.to_pandas()

    # Check values
    assert df_result.iloc[0]["id"] == "row1"
    assert df_result.iloc[0]["data"] == 5
    assert df_result.iloc[0]["config"] == 6


def test_dataframe_reader_into_size_table_head_parameter(tmp_path):
    """Test head parameter limits rows processed."""
    # Create test files
    test_file1 = tmp_path / "file1.txt"
    test_file2 = tmp_path / "file2.txt"
    test_file1.write_text("hello")  # 5 bytes
    test_file2.write_text("world!")  # 6 bytes

    # Create DataFrame with multiple rows
    df = pd.DataFrame(
        {
            "id": ["row1", "row2", "row3"],
            "data": [str(test_file1), str(test_file1), str(test_file1)],
            "config": [str(test_file2), str(test_file2), str(test_file2)],
        }
    )

    # Create reader and get size table with head=2
    reader = file_dataset.reader(dataframe=df)
    table = reader.into_size_table(head=2)

    # Should only have 2 rows
    assert table.num_rows == 2

    df_result = table.to_pandas()
    assert list(df_result["id"]) == ["row1", "row2"]


def test_dataframe_reader_into_size_table_missing_files(tmp_path, caplog):
    """Test error handling for missing files."""
    # Create one existing file and one missing file path
    existing_file = tmp_path / "exists.txt"
    existing_file.write_text("hello")  # 5 bytes
    missing_file = tmp_path / "missing.txt"  # This file doesn't exist

    # Create DataFrame with one good and one bad file
    df = pd.DataFrame(
        {
            "id": ["row1"],
            "good_file": [str(existing_file)],
            "bad_file": [str(missing_file)],
        }
    )

    # Create reader and get size table
    reader = file_dataset.reader(dataframe=df)
    table = reader.into_size_table()

    # Should have 1 row with only the good file
    assert table.num_rows == 1
    df_result = table.to_pandas()

    # Should have id and good_file columns
    assert "id" in df_result.columns
    assert "good_file" in df_result.columns
    assert "bad_file" not in df_result.columns  # Failed file shouldn't appear

    assert df_result.iloc[0]["good_file"] == 5

    # Should have logged warning about bad file
    assert "Some files failed for row row1" in caplog.text


def test_dataframe_reader_into_size_table_schema_validation(tmp_path):
    """Test PyArrow table schema is correct."""
    # Create test file
    test_file = tmp_path / "file.txt"
    test_file.write_text("hello")  # 5 bytes

    # Create DataFrame
    df = pd.DataFrame({"id": ["row1"], "data": [str(test_file)]})

    # Create reader and get size table
    reader = file_dataset.reader(dataframe=df)
    table = reader.into_size_table()

    # Check schema
    schema = table.schema
    assert len(schema) == 2
    assert schema.field("id").type == pa.string()
    assert schema.field("data").type == pa.int64()


def test_reader_into_size_table_not_implemented(tmp_path):
    """Test that single FileRowReader raises NotImplementedError."""
    # Create test file
    test_file = tmp_path / "file.txt"
    test_file.write_text("hello")

    # Create single-row reader
    reader = file_dataset.reader(row={"data": str(test_file)})

    # Should raise NotImplementedError
    with pytest.raises(
        NotImplementedError,
        match="Size table not supported for single-row FileRowReader",
    ):
        reader.into_size_table()
