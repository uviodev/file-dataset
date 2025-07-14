"""Tests for DataFrame reader functionality."""

from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from file_dataset import FileDatasetError, Options, reader


class TestDataFrameReader:
    """Test cases for the DataFrame reader functionality."""

    def test_all_successful_rows(self):
        """Test reading DataFrame where all rows have valid files."""
        test_data_dir = Path(__file__).parent / "data"

        # Create DataFrame with multiple rows of valid files
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "image.txt": str(test_data_dir / "test_image.txt"),
                    "mask.txt": str(test_data_dir / "test_mask.txt"),
                },
                {
                    "id": "row2",
                    "image.txt": str(test_data_dir / "test_config.json"),
                    "mask.txt": str(test_data_dir / "test_mask.txt"),
                },
            ]
        )

        # Create reader with DataFrame
        df_reader = reader(dataframe=df)

        # Process all rows and verify each gets its own temp dir
        temp_dirs = []
        for temp_dir in df_reader.into_temp_dir():
            assert temp_dir.exists()
            assert temp_dir.is_dir()

            # Check files exist in temp dir
            assert (temp_dir / "image.txt").exists()
            assert (temp_dir / "mask.txt").exists()

            temp_dirs.append(temp_dir)

        # Should have processed both rows
        assert len(temp_dirs) == 2

    def test_partial_failure(self, caplog):
        """Test reading DataFrame with some failing rows."""
        test_data_dir = Path(__file__).parent / "data"

        # Create DataFrame with mix of valid and invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "image.txt": str(test_data_dir / "test_image.txt"),
                    "mask.txt": str(test_data_dir / "test_mask.txt"),
                },
                {
                    "id": "row2",
                    "image.txt": "/nonexistent/missing_file.txt",  # This will fail
                    "mask.txt": str(test_data_dir / "test_mask.txt"),
                },
                {
                    "id": "row3",
                    "image.txt": str(test_data_dir / "test_config.json"),
                    "mask.txt": str(test_data_dir / "test_mask.txt"),
                },
            ]
        )

        # Create reader with DataFrame
        df_reader = reader(dataframe=df)

        # Process rows - should skip failing row
        temp_dirs = []
        for temp_dir in df_reader.into_temp_dir():
            temp_dirs.append(temp_dir)

        # Should have processed 2 out of 3 rows
        assert len(temp_dirs) == 2

        # Check that error was logged for row2
        assert "Failed to process row row2" in caplog.text
        assert "missing_file.txt" in caplog.text

    def test_empty_dataframe(self):
        """Test reading empty DataFrame."""
        # Create empty DataFrame
        df = pd.DataFrame()

        # Create reader with empty DataFrame
        df_reader = reader(dataframe=df)

        # Process rows - should yield nothing
        temp_dirs = list(df_reader.into_temp_dir())

        # Should have processed no rows
        assert len(temp_dirs) == 0

    def test_all_rows_fail(self, caplog):
        """Test reading DataFrame where all rows fail."""
        # Create DataFrame with all invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "file1.txt": "/nonexistent/missing1.txt",
                    "file2.txt": "/nonexistent/missing2.txt",
                },
                {
                    "id": "row2",
                    "file1.txt": "/nonexistent/missing3.txt",
                    "file2.txt": "/nonexistent/missing4.txt",
                },
            ]
        )

        # Create reader with DataFrame
        df_reader = reader(dataframe=df)

        # Should raise error when all rows fail
        with pytest.raises(FileDatasetError) as exc_info:
            list(df_reader.into_temp_dir())

        error = exc_info.value
        assert "All rows failed" in str(error)
        assert "Check error logs" in str(error)

        # Check that errors were logged for both rows
        assert "Failed to process row row1" in caplog.text
        assert "Failed to process row row2" in caplog.text

    @mock_aws
    def test_mixed_local_s3_with_failures(self, caplog):
        """Test reading DataFrame with mixed local and S3 sources, some failing."""
        test_data_dir = Path(__file__).parent / "data"

        # Create S3 bucket and upload a test file
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        s3_client.put_object(
            Bucket="test-bucket", Key="valid-file.txt", Body=b"test content"
        )

        # Create DataFrame with mix of local, S3 valid, and invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "local_file": str(test_data_dir / "test_image.txt"),
                    "s3_file": "s3://test-bucket/valid-file.txt",
                },
                {
                    "id": "row2",
                    "local_file": "/nonexistent/missing_local.txt",  # Will fail
                    "s3_file": "s3://test-bucket/valid-file.txt",
                },
                {
                    "id": "row3",
                    "local_file": str(test_data_dir / "test_mask.txt"),
                    "s3_file": "s3://test-bucket/missing-file.txt",  # Will fail
                },
                {
                    "id": "row4",
                    "local_file": str(test_data_dir / "test_config.json"),
                    "s3_file": "s3://test-bucket/valid-file.txt",
                },
            ]
        )

        # Create reader with DataFrame and options
        options = Options.default()
        df_reader = reader(dataframe=df, options=options)

        # Process rows - should skip failing rows (row2 and row3)
        temp_dirs = []
        for temp_dir in df_reader.into_temp_dir():
            # Verify files exist in temp dir
            assert (temp_dir / "local_file").exists()
            assert (temp_dir / "s3_file").exists()
            temp_dirs.append(temp_dir)

        # Should have processed 2 out of 4 rows
        assert len(temp_dirs) == 2

        # Check that errors were logged for row2 and row3
        assert "Failed to process row row2" in caplog.text
        assert "missing_local.txt" in caplog.text

        assert "Failed to process row row3" in caplog.text
        assert "missing-file.txt" in caplog.text

    def test_mutual_exclusivity(self):
        """Test that row and dataframe arguments are mutually exclusive."""
        test_data_dir = Path(__file__).parent / "data"

        # Try to pass both row and dataframe
        row_dict = {"file.txt": str(test_data_dir / "test_image.txt")}
        df = pd.DataFrame([{"file.txt": str(test_data_dir / "test_image.txt")}])

        with pytest.raises(
            ValueError, match="Cannot specify both 'row' and 'dataframe'"
        ):
            reader(row=row_dict, dataframe=df)

        # Try to pass neither
        with pytest.raises(
            ValueError, match="Must specify either 'row' or 'dataframe'"
        ):
            reader()
