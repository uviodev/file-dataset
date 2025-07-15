"""Tests for blob table functionality."""

import pandas as pd
import pyarrow as pa
from moto import mock_aws

import file_dataset
from file_dataset.s3_options import S3Options


def test_dataframe_reader_into_blob_table_local_files(tmp_path):
    """Test creating blob table for local files."""
    # Create test files with known content
    test_file1 = tmp_path / "file1.txt"
    test_file2 = tmp_path / "file2.txt"
    test_file1.write_text("hello")  # 5 bytes
    test_file2.write_text("world!")  # 6 bytes

    # Create DataFrame
    df = pd.DataFrame(
        {"id": ["row1"], "data": [str(test_file1)], "config": [str(test_file2)]}
    )

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

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
    assert df_result.iloc[0]["data"] == b"hello"
    assert df_result.iloc[0]["config"] == b"world!"


@mock_aws
def test_dataframe_reader_into_blob_table_s3_files():
    """Test creating blob table for S3 files."""
    import boto3

    # Setup S3 mock
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")

    # Upload test files with known content
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

    # Create reader with options and get blob table
    options = S3Options.default()
    reader = file_dataset.file_dataframe_reader(df, options=options)
    table = reader.into_blob_table()

    # Verify table structure
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1

    # Convert to pandas for easier checking
    df_result = table.to_pandas()

    # Check values
    assert df_result.iloc[0]["id"] == "row1"
    assert df_result.iloc[0]["data"] == b"hello"
    assert df_result.iloc[0]["config"] == b"world!"


def test_dataframe_reader_into_blob_table_head_parameter(tmp_path):
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

    # Create reader and get blob table with head=2
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table(head=2)

    # Should only have 2 rows
    assert table.num_rows == 2

    df_result = table.to_pandas()
    assert list(df_result["id"]) == ["row1", "row2"]


def test_dataframe_reader_into_blob_table_missing_files(tmp_path, caplog):
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

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    # Should have 1 row with only the good file
    assert table.num_rows == 1
    df_result = table.to_pandas()

    # Should have id and good_file columns
    assert "id" in df_result.columns
    assert "good_file" in df_result.columns
    assert "bad_file" not in df_result.columns  # Failed file shouldn't appear

    assert df_result.iloc[0]["good_file"] == b"hello"

    # Should have logged warning about bad file
    assert "Some files failed for row row1" in caplog.text


def test_dataframe_reader_into_blob_table_schema_validation(tmp_path):
    """Test PyArrow table schema is correct."""
    # Create test file
    test_file = tmp_path / "file.txt"
    test_file.write_text("hello")  # 5 bytes

    # Create DataFrame
    df = pd.DataFrame({"id": ["row1"], "data": [str(test_file)]})

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    # Check schema
    schema = table.schema
    assert len(schema) == 2
    assert schema.field("id").type == pa.string()
    assert schema.field("data").type == pa.binary()


def test_dataframe_reader_into_blob_table_mixed_sources(tmp_path):
    """Test blob table with mixed local and S3 sources."""
    # Create local test file
    local_file = tmp_path / "local.txt"
    local_file.write_text("local content")

    # Test would need S3 setup for full mixed test
    # For now, just test local files in this test
    df = pd.DataFrame({"id": ["row1"], "local_data": [str(local_file)]})

    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    df_result = table.to_pandas()
    assert df_result.iloc[0]["local_data"] == b"local content"


def test_dataframe_reader_into_blob_table_binary_data(tmp_path):
    """Test blob table with binary file content."""
    # Create file with binary content
    binary_file = tmp_path / "binary.dat"
    binary_content = b"\x00\x01\x02\x03\xff\xfe\xfd"
    binary_file.write_bytes(binary_content)

    # Create DataFrame
    df = pd.DataFrame({"id": ["row1"], "binary_data": [str(binary_file)]})

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    # Verify binary content is preserved
    df_result = table.to_pandas()
    assert df_result.iloc[0]["binary_data"] == binary_content


def test_dataframe_reader_into_blob_table_empty_dataframe():
    """Test blob table with empty DataFrame."""
    # Create empty DataFrame
    df = pd.DataFrame(columns=["id", "data"])

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    # Should return empty table with correct schema
    assert table.num_rows == 0
    assert table.schema.field("id").type == pa.string()


def test_dataframe_reader_into_blob_table_none_values(tmp_path):
    """Test blob table with None values (optional files)."""
    # Create one test file
    test_file = tmp_path / "file.txt"
    test_file.write_text("hello")

    # Create DataFrame with None value for optional file
    df = pd.DataFrame(
        {
            "id": ["row1"],
            "required_file": [str(test_file)],
            "optional_file": [None],
        }
    )

    # Create reader and get blob table
    reader = file_dataset.file_dataframe_reader(df)
    table = reader.into_blob_table()

    # Should have 1 row with only the required file
    assert table.num_rows == 1
    df_result = table.to_pandas()

    # Should have id and required_file columns only
    assert "id" in df_result.columns
    assert "required_file" in df_result.columns
    assert "optional_file" not in df_result.columns  # None values are skipped

    assert df_result.iloc[0]["required_file"] == b"hello"
