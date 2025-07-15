"""Tests for file dataset reader S3 functionality."""

import boto3
import pytest
from moto import mock_aws

from file_dataset import FileDatasetError, S3Options, row_reader


@pytest.fixture
def s3_setup():
    """Set up mock S3 environment with test data."""
    with mock_aws():
        # Create S3 client and bucket
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)

        # Upload test files
        test_data = b"Test file content"
        s3.put_object(Bucket=bucket_name, Key="test_file.txt", Body=test_data)

        yield {
            "bucket": bucket_name,
            "s3": s3,
            "test_data": test_data,
        }


class TestReaderS3:
    """Test cases for the reader S3 functionality."""

    def test_row_reader_single_s3_file_download(self, s3_setup):
        """Test downloading a single file from S3 using new row_reader API."""
        bucket = s3_setup["bucket"]
        test_data = s3_setup["test_data"]

        files_dict = {"downloaded.txt": f"s3://{bucket}/test_file.txt"}

        # Create reader with default options using new API
        r = row_reader(files_dict, options=S3Options.default())

        with r.into_temp_dir() as temp_dir:
            # Check temp directory exists and contains the file
            assert temp_dir.exists()
            assert temp_dir.is_dir()

            downloaded_file = temp_dir / "downloaded.txt"
            assert downloaded_file.exists()
            assert downloaded_file.is_file()

            # Check file content is correct
            assert downloaded_file.read_bytes() == test_data

    def test_single_s3_file_download(self, s3_setup):
        """Test downloading a single file from S3."""
        bucket = s3_setup["bucket"]
        test_data = s3_setup["test_data"]

        files_dict = {"downloaded.txt": f"s3://{bucket}/test_file.txt"}

        # Create reader with default options
        r = row_reader(files_dict, options=S3Options.default())

        with r.into_temp_dir() as temp_dir:
            # Check temp directory exists and contains the file
            assert temp_dir.exists()
            assert temp_dir.is_dir()

            downloaded_file = temp_dir / "downloaded.txt"
            assert downloaded_file.exists()
            assert downloaded_file.is_file()

            # Check file content is correct
            assert downloaded_file.read_bytes() == test_data

    def test_mixed_local_and_s3_files(self, s3_setup, tmp_path):
        """Test reader with both local and S3 files."""
        bucket = s3_setup["bucket"]
        s3_data = s3_setup["test_data"]

        # Create a local test file
        local_file = tmp_path / "local_test.txt"
        local_data = b"Local file content"
        local_file.write_bytes(local_data)

        # Mix local and S3 files
        files_dict = {
            "from_s3.txt": f"s3://{bucket}/test_file.txt",
            "from_local.txt": str(local_file),
        }

        # Create reader with options for S3
        r = row_reader(files_dict, options=S3Options.default())

        with r.into_temp_dir() as temp_dir:
            # Check both files exist
            s3_file = temp_dir / "from_s3.txt"
            local_file_copy = temp_dir / "from_local.txt"

            assert s3_file.exists()
            assert local_file_copy.exists()

            # Check content
            assert s3_file.read_bytes() == s3_data
            assert local_file_copy.read_bytes() == local_data

    def test_s3_file_not_found(self, s3_setup):
        """Test error handling for missing S3 objects."""
        bucket = s3_setup["bucket"]

        # Reference a non-existent S3 object
        files_dict = {"missing.txt": f"s3://{bucket}/does_not_exist.txt"}

        r = row_reader(files_dict, options=S3Options.default())

        with pytest.raises(FileDatasetError) as exc_info:
            with r.into_temp_dir():
                pass

        # Check error details
        assert "missing.txt" in exc_info.value.file_errors
        assert "S3 object not found" in exc_info.value.file_errors["missing.txt"]

    def test_s3_url_without_options(self, s3_setup):
        """Test that S3 URLs work with default options when none provided."""
        bucket = s3_setup["bucket"]

        files_dict = {"file.txt": f"s3://{bucket}/nonexistent_file.txt"}

        # Create reader without options - should now use defaults
        r = row_reader(files_dict)

        # Should fail due to file not found, not due to missing options
        with pytest.raises(FileDatasetError) as exc_info:
            with r.into_temp_dir():
                pass

        # Should not mention "Options required" anymore
        assert not any(
            "Options required" in error for error in exc_info.value.file_errors.values()
        )

    def test_invalid_s3_url_format(self):
        """Test error handling for invalid S3 URL format."""
        files_dict = {
            "bad1.txt": "s3://",  # Missing bucket and key
            "bad2.txt": "s3://bucket",  # Missing key
        }

        r = row_reader(files_dict, options=S3Options.default())

        with pytest.raises(FileDatasetError) as exc_info:
            with r.into_temp_dir():
                pass

        # Check files have errors
        assert len(exc_info.value.file_errors) == 2
        for filename in files_dict:
            assert "Invalid S3 URL format" in exc_info.value.file_errors[filename]

    def test_local_file_only_no_options(self, tmp_path):
        """Test that local files work without options."""
        # Create a local test file
        local_file = tmp_path / "local_test.txt"
        local_data = b"Local file content"
        local_file.write_bytes(local_data)

        files_dict = {"local.txt": str(local_file)}

        # Create reader without options (should work for local files)
        r = row_reader(files_dict)

        with r.into_temp_dir() as temp_dir:
            copied_file = temp_dir / "local.txt"
            assert copied_file.exists()
            assert copied_file.read_bytes() == local_data

    def test_explicit_options_vs_default(self, s3_setup):
        """Test using explicit options vs default options."""
        bucket = s3_setup["bucket"]
        test_data = s3_setup["test_data"]

        files_dict = {"file.txt": f"s3://{bucket}/test_file.txt"}

        # Test with explicit options
        explicit_opts = S3Options.default()
        r1 = row_reader(files_dict, options=explicit_opts)

        with r1.into_temp_dir() as temp_dir:
            assert (temp_dir / "file.txt").read_bytes() == test_data

        # Test with default options created inline
        r2 = row_reader(files_dict, options=S3Options.default())

        with r2.into_temp_dir() as temp_dir:
            assert (temp_dir / "file.txt").read_bytes() == test_data
