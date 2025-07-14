"""Tests for S3 write functionality."""

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from file_dataset import write_files
from file_dataset.exceptions import FileDatasetError
from file_dataset.options import Options


@mock_aws
class TestWriteS3:
    """Test S3 write functionality."""

    def setup_method(self, method):  # noqa: ARG002
        """Set up test S3 bucket."""
        # Create mock S3 bucket
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-bucket"
        self.s3_client.create_bucket(Bucket=self.bucket_name)

        # Create options
        self.options = Options.default()

    def test_single_file_s3_upload(self, tmp_path):
        """Test uploading a single file to S3."""
        # Create test file
        source_file = tmp_path / "test.txt"
        source_file.write_text("test content")

        # Write to S3
        s3_path = f"s3://{self.bucket_name}/test-prefix"
        result = write_files(
            row={"file.txt": str(source_file)},
            into_path=s3_path,
            id="test-id",
            options=self.options,
        )

        # Verify result
        assert (
            result["file.txt"]
            == f"s3://{self.bucket_name}/test-prefix/test-id/file.txt"
        )

        # Verify file was uploaded
        response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key="test-prefix/test-id/file.txt"
        )
        assert response["Body"].read().decode() == "test content"

    def test_multiple_files_s3_upload(self, tmp_path):
        """Test uploading multiple files to S3."""
        # Create test files
        file1 = tmp_path / "file1.txt"
        file1.write_text("content1")
        file2 = tmp_path / "file2.txt"
        file2.write_text("content2")

        # Write to S3
        s3_path = f"s3://{self.bucket_name}/multi"
        result = write_files(
            row={"file1.txt": str(file1), "file2.txt": str(file2)},
            into_path=s3_path,
            id="multi-id",
            options=self.options,
        )

        # Verify results
        assert (
            result["file1.txt"] == f"s3://{self.bucket_name}/multi/multi-id/file1.txt"
        )
        assert (
            result["file2.txt"] == f"s3://{self.bucket_name}/multi/multi-id/file2.txt"
        )

        # Verify files were uploaded
        obj1 = self.s3_client.get_object(
            Bucket=self.bucket_name, Key="multi/multi-id/file1.txt"
        )
        assert obj1["Body"].read().decode() == "content1"

        obj2 = self.s3_client.get_object(
            Bucket=self.bucket_name, Key="multi/multi-id/file2.txt"
        )
        assert obj2["Body"].read().decode() == "content2"

    def test_s3_upload_with_none_values(self, tmp_path):
        """Test S3 upload with None values (optional files)."""
        # Create test file
        file1 = tmp_path / "file1.txt"
        file1.write_text("content1")

        # Write to S3 with None value
        s3_path = f"s3://{self.bucket_name}/optional"
        result = write_files(
            row={
                "file1.txt": str(file1),
                "file2.txt": None,  # Optional file
            },
            into_path=s3_path,
            id="opt-id",
            options=self.options,
        )

        # Verify results
        assert (
            result["file1.txt"] == f"s3://{self.bucket_name}/optional/opt-id/file1.txt"
        )
        assert result["file2.txt"] is None

        # Verify only file1 was uploaded
        self.s3_client.get_object(
            Bucket=self.bucket_name, Key="optional/opt-id/file1.txt"
        )

        with pytest.raises(self.s3_client.exceptions.NoSuchKey):
            self.s3_client.get_object(
                Bucket=self.bucket_name, Key="optional/opt-id/file2.txt"
            )

    def test_s3_upload_without_options(self, tmp_path):
        """Test that S3 upload fails without options."""
        # Create test file
        source_file = tmp_path / "test.txt"
        source_file.write_text("test content")

        # Try to write to S3 without options
        s3_path = f"s3://{self.bucket_name}/test"
        with pytest.raises(ValueError, match="Options required for S3 paths"):
            write_files(
                row={"file.txt": str(source_file)},
                into_path=s3_path,
                id="test-id",
                # No options provided
            )

    def test_invalid_s3_url_format(self, tmp_path):
        """Test handling of invalid S3 URL formats."""
        # Create test file
        source_file = tmp_path / "test.txt"
        source_file.write_text("test content")

        # Test various invalid S3 URLs
        invalid_s3_urls = [
            "s3://",  # No bucket
            "s3:///prefix",  # No bucket name
        ]

        for invalid_url in invalid_s3_urls:
            with pytest.raises(ValueError, match="Invalid S3 URL"):
                write_files(
                    row={"file.txt": str(source_file)},
                    into_path=invalid_url,
                    id="test-id",
                    options=self.options,
                )

    def test_s3_upload_error_handling(self, tmp_path, mocker):
        """Test S3 upload error handling."""
        # Create test file
        source_file = tmp_path / "test.txt"
        source_file.write_text("test content")

        # Mock S3 upload to fail
        mocker.patch.object(
            self.options.s3_client,
            "upload_file",
            side_effect=Exception("Upload failed"),
        )
        s3_path = f"s3://{self.bucket_name}/error"
        with pytest.raises(FileDatasetError) as exc_info:
            write_files(
                row={"file.txt": str(source_file)},
                into_path=s3_path,
                id="error-id",
                options=self.options,
            )

        assert "Failed to upload file" in str(exc_info.value)

    def test_dataframe_s3_upload(self, tmp_path):
        """Test DataFrame mode with S3 uploads."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        (source_dir / "file1.txt").write_text("content1")
        (source_dir / "file2.txt").write_text("content2")
        (source_dir / "file3.txt").write_text("content3")

        # Create DataFrame
        df = pd.DataFrame(
            {
                "id": ["row1", "row2"],
                "file_a": [
                    str(source_dir / "file1.txt"),
                    str(source_dir / "file2.txt"),
                ],
                "file_b": [
                    str(source_dir / "file3.txt"),
                    str(source_dir / "file1.txt"),
                ],
            }
        )

        # Write to S3
        s3_path = f"s3://{self.bucket_name}/dataframe"
        result_df = write_files(dataframe=df, into_path=s3_path, options=self.options)

        # Verify result DataFrame
        assert len(result_df) == 2
        assert result_df.iloc[0]["id"] == "row1"
        assert (
            result_df.iloc[0]["file_a"]
            == f"s3://{self.bucket_name}/dataframe/row1/file_a"
        )
        assert (
            result_df.iloc[0]["file_b"]
            == f"s3://{self.bucket_name}/dataframe/row1/file_b"
        )

        assert result_df.iloc[1]["id"] == "row2"
        assert (
            result_df.iloc[1]["file_a"]
            == f"s3://{self.bucket_name}/dataframe/row2/file_a"
        )
        assert (
            result_df.iloc[1]["file_b"]
            == f"s3://{self.bucket_name}/dataframe/row2/file_b"
        )

        # Verify files were uploaded
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name, Key="dataframe/row1/file_a"
        )
        assert obj["Body"].read().decode() == "content1"

    def test_mixed_local_and_s3_not_supported(self, tmp_path):
        """Test that mixed local and S3 destinations are not supported in same call."""
        # For now, we'll test that a single write_files call goes to either
        # local OR S3, not both. This is a design decision - each call writes
        # to one destination type
