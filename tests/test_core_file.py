"""Tests for _core_file module."""

import boto3
import pytest
from moto import mock_aws

from file_dataset._core_file import copy_each_file
from file_dataset.exceptions import FileDatasetError
from file_dataset.s3_options import S3Options


class TestCoreFile:
    """Test core file operations."""

    def test_copy_each_file_local_to_local(self, tmp_path):
        """Test copying a local file to another local location."""
        # Create source file
        source = tmp_path / "source.txt"
        source.write_text("test content")

        # Define destination
        dest = tmp_path / "dest.txt"

        # Perform copy
        copies = [(str(source), str(dest))]
        copy_each_file(copies, s3_options=None)

        # Verify
        assert dest.exists()
        assert dest.read_text() == "test content"

    def test_copy_each_file_validates_source_exists(self, tmp_path):
        """Test that copy_each_file validates source files exist."""
        # Non-existent source
        source = tmp_path / "nonexistent.txt"
        dest = tmp_path / "dest.txt"

        # Should raise error
        copies = [(str(source), str(dest))]
        with pytest.raises(FileDatasetError) as exc_info:
            copy_each_file(copies, s3_options=None)

        assert "Source file not found" in str(exc_info.value)

    def test_copy_each_file_validates_unique_destinations(self, tmp_path):
        """Test that copy_each_file validates destinations are unique."""
        # Create source files
        source1 = tmp_path / "source1.txt"
        source1.write_text("content1")
        source2 = tmp_path / "source2.txt"
        source2.write_text("content2")

        # Same destination
        dest = tmp_path / "dest.txt"

        # Should raise error
        copies = [(str(source1), str(dest)), (str(source2), str(dest))]
        with pytest.raises(FileDatasetError) as exc_info:
            copy_each_file(copies, s3_options=None)

        assert "Duplicate destination" in str(exc_info.value)

    @mock_aws
    def test_copy_each_file_s3_to_local(self, tmp_path):
        """Test copying from S3 to local."""
        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Put test file in S3
        key = "test-file.txt"
        content = b"s3 content"
        s3.put_object(Bucket=bucket, Key=key, Body=content)

        # Define destination
        dest = tmp_path / "local-copy.txt"

        # Perform copy
        s3_options = S3Options.default()
        copies = [(f"s3://{bucket}/{key}", str(dest))]
        copy_each_file(copies, s3_options)

        # Verify
        assert dest.exists()
        assert dest.read_bytes() == content

    @mock_aws
    def test_copy_each_file_local_to_s3(self, tmp_path):
        """Test copying from local to S3."""
        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Create local file
        source = tmp_path / "local-file.txt"
        content = b"local content"
        source.write_bytes(content)

        # Perform copy
        key = "uploaded-file.txt"
        s3_options = S3Options.default()
        copies = [(str(source), f"s3://{bucket}/{key}")]
        copy_each_file(copies, s3_options)

        # Verify
        response = s3.get_object(Bucket=bucket, Key=key)
        assert response["Body"].read() == content

    @mock_aws
    def test_copy_each_file_s3_to_s3(self):
        """Test copying from S3 to S3."""
        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket1 = "source-bucket"
        bucket2 = "dest-bucket"
        s3.create_bucket(Bucket=bucket1)
        s3.create_bucket(Bucket=bucket2)

        # Put test file in source bucket
        src_key = "source-file.txt"
        content = b"s3 to s3 content"
        s3.put_object(Bucket=bucket1, Key=src_key, Body=content)

        # Perform copy
        dst_key = "copied-file.txt"
        s3_options = S3Options.default()
        copies = [(f"s3://{bucket1}/{src_key}", f"s3://{bucket2}/{dst_key}")]
        copy_each_file(copies, s3_options)

        # Verify
        response = s3.get_object(Bucket=bucket2, Key=dst_key)
        assert response["Body"].read() == content

    def test_read_file_sizes_local(self, tmp_path):
        """Test reading file sizes from local files."""
        # Create test files
        file1 = tmp_path / "file1.txt"
        file1.write_text("hello")  # 5 bytes

        file2 = tmp_path / "file2.txt"
        file2.write_text("hello world")  # 11 bytes

        # Read sizes
        from file_dataset._core_file import read_each_file_size

        files = {
            "f1": str(file1),
            "f2": str(file2),
        }

        sizes, errors = read_each_file_size(files, s3_options=None)

        assert sizes == {"f1": 5, "f2": 11}
        assert errors == {}

    @mock_aws
    def test_read_file_sizes_s3(self):
        """Test reading file sizes from S3 files."""
        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Put test files
        s3.put_object(Bucket=bucket, Key="file1.txt", Body=b"hello")  # 5 bytes
        s3.put_object(Bucket=bucket, Key="file2.txt", Body=b"hello world")  # 11 bytes

        # Read sizes
        from file_dataset._core_file import read_each_file_size

        files = {
            "f1": f"s3://{bucket}/file1.txt",
            "f2": f"s3://{bucket}/file2.txt",
        }

        s3_options = S3Options.default()
        sizes, errors = read_each_file_size(files, s3_options)

        assert sizes == {"f1": 5, "f2": 11}
        assert errors == {}

    def test_read_file_contents_local(self, tmp_path):
        """Test reading file contents from local files."""
        # Create test files
        file1 = tmp_path / "file1.txt"
        content1 = b"hello"
        file1.write_bytes(content1)

        file2 = tmp_path / "file2.txt"
        content2 = b"hello world"
        file2.write_bytes(content2)

        # Read contents
        from file_dataset._core_file import read_each_file_contents

        files = {
            "f1": str(file1),
            "f2": str(file2),
        }

        contents, errors = read_each_file_contents(files, s3_options=None)

        assert contents == {"f1": content1, "f2": content2}
        assert errors == {}

    @mock_aws
    def test_read_file_contents_s3(self):
        """Test reading file contents from S3 files."""
        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Put test files
        content1 = b"hello"
        content2 = b"hello world"
        s3.put_object(Bucket=bucket, Key="file1.txt", Body=content1)
        s3.put_object(Bucket=bucket, Key="file2.txt", Body=content2)

        # Read contents
        from file_dataset._core_file import read_each_file_contents

        files = {
            "f1": f"s3://{bucket}/file1.txt",
            "f2": f"s3://{bucket}/file2.txt",
        }

        s3_options = S3Options.default()
        contents, errors = read_each_file_contents(files, s3_options)

        assert contents == {"f1": content1, "f2": content2}
        assert errors == {}


class TestFileValidation:
    """Test file validation functions."""

    def test_validate_file_exists_local_file(self, tmp_path):
        """Test _validate_file_exists for existing local file."""
        from file_dataset._core_file import _validate_file_exists

        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Test file exists - should return None (no error)
        assert _validate_file_exists(str(test_file), s3_options=None) is None

    def test_validate_file_exists_local_missing(self, tmp_path):
        """Test _validate_file_exists for missing local file."""
        from file_dataset._core_file import _validate_file_exists

        # Test non-existent file
        missing_file = tmp_path / "missing.txt"
        error = _validate_file_exists(str(missing_file), s3_options=None)
        assert error is not None
        assert "not found" in error

    def test_validate_file_exists_local_directory(self, tmp_path):
        """Test _validate_file_exists for directory (should return error)."""
        from file_dataset._core_file import _validate_file_exists

        # Create a directory
        test_dir = tmp_path / "testdir"
        test_dir.mkdir()

        # Directory should return error
        error = _validate_file_exists(str(test_dir), s3_options=None)
        assert error is not None
        assert "not a file" in error

    @mock_aws
    def test_validate_file_exists_s3_file(self):
        """Test _validate_file_exists for existing S3 file."""
        from file_dataset._core_file import _validate_file_exists

        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="test.txt", Body=b"content")

        # Test file exists - should return None (no error)
        s3_options = S3Options.default()
        assert _validate_file_exists(f"s3://{bucket}/test.txt", s3_options) is None

    @mock_aws
    def test_validate_file_exists_s3_missing(self):
        """Test _validate_file_exists for missing S3 file."""
        from file_dataset._core_file import _validate_file_exists

        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        # Test missing file
        s3_options = S3Options.default()
        error = _validate_file_exists(f"s3://{bucket}/missing.txt", s3_options)
        assert error is not None
        assert "not found" in error

    def test_validate_file_exists_s3_no_options(self):
        """Test _validate_file_exists for S3 file without S3Options."""
        from file_dataset._core_file import _validate_file_exists

        # S3 file without options should return error
        error = _validate_file_exists("s3://bucket/file.txt", s3_options=None)
        assert error is not None
        assert "S3Options required" in error

    def test_validate_s3_path_format_valid(self):
        """Test _validate_s3_path_format with valid S3 URL."""
        from file_dataset._core_file import _validate_s3_path_format

        s3_options = S3Options.default()
        # Valid S3 path with options
        assert _validate_s3_path_format("s3://bucket/file.txt", s3_options) is None

    def test_validate_s3_path_format_invalid_url(self):
        """Test _validate_s3_path_format with invalid S3 URL."""
        from file_dataset._core_file import _validate_s3_path_format

        s3_options = S3Options.default()
        # Invalid S3 URL
        error = _validate_s3_path_format("s3://", s3_options)
        assert error is not None
        assert "Invalid S3 URL" in error

    def test_validate_s3_path_format_no_key(self):
        """Test _validate_s3_path_format with bucket-only URL."""
        from file_dataset._core_file import _validate_s3_path_format

        s3_options = S3Options.default()
        # Bucket-only URL should be invalid for file operations
        error = _validate_s3_path_format("s3://bucket/", s3_options)
        assert error is not None
        assert "Invalid S3 URL" in error

    def test_validate_s3_path_format_no_options(self):
        """Test _validate_s3_path_format without S3Options."""
        from file_dataset._core_file import _validate_s3_path_format

        # S3 path without options
        error = _validate_s3_path_format("s3://bucket/file.txt", s3_options=None)
        assert error is not None
        assert "S3Options required" in error

    def test_validate_s3_path_format_local_path(self):
        """Test _validate_s3_path_format with local path."""
        from file_dataset._core_file import _validate_s3_path_format

        # Local paths should return None immediately
        assert _validate_s3_path_format("/path/to/file.txt", s3_options=None) is None
        assert _validate_s3_path_format("relative/path.txt", s3_options=None) is None

    def test_validate_each_file_local_files(self, tmp_path):
        """Test validate_each_file with local files."""
        from file_dataset._core_file import validate_each_file

        # Create test files
        file1 = tmp_path / "file1.txt"
        file1.write_text("content1")
        file2 = tmp_path / "file2.txt"
        file2.write_text("content2")
        missing = tmp_path / "missing.txt"

        files = {
            "f1": str(file1),
            "f2": str(file2),
            "f3": str(missing),
        }

        # Test with existence checks
        errors = validate_each_file(files, s3_options=None, do_existence_checks=True)
        assert "f3" in errors
        assert "not found" in errors["f3"] or "not exist" in errors["f3"]
        assert "f1" not in errors
        assert "f2" not in errors

    def test_validate_each_file_no_existence_checks(self, tmp_path):
        """Test validate_each_file without existence checks."""
        from file_dataset._core_file import validate_each_file

        missing = tmp_path / "missing.txt"

        files = {
            "f1": str(missing),
            "f2": "s3://bucket/file.txt",  # Invalid without s3_options
        }

        # Without existence checks, only format validation for S3
        errors = validate_each_file(files, s3_options=None, do_existence_checks=False)
        # When S3 files exist without options, we get a special error
        assert "__s3_options__" in errors
        assert "S3Options required" in errors["__s3_options__"]

    @mock_aws
    def test_validate_each_file_s3_files(self):
        """Test validate_each_file with S3 files."""
        from file_dataset._core_file import validate_each_file

        # Set up mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="file1.txt", Body=b"content1")

        files = {
            "f1": f"s3://{bucket}/file1.txt",
            "f2": f"s3://{bucket}/missing.txt",
            "f3": "s3://invalid-url",
        }

        s3_options = S3Options.default()
        errors = validate_each_file(files, s3_options, do_existence_checks=True)

        # When there's a format error, existence checks don't run
        assert "f1" not in errors  # No error
        assert "f2" not in errors  # Existence check didn't run due to f3's format error
        assert "f3" in errors  # Invalid format
        assert "Invalid S3 URL" in errors["f3"]

        # Test again without the invalid URL
        files_valid = {
            "f1": f"s3://{bucket}/file1.txt",
            "f2": f"s3://{bucket}/missing.txt",
        }
        errors_valid = validate_each_file(
            files_valid, s3_options, do_existence_checks=True
        )
        assert "f1" not in errors_valid  # Exists
        assert "f2" in errors_valid  # Missing
        assert "not found" in errors_valid["f2"]

    def test_validate_each_file_missing_s3_options(self):
        """Test validate_each_file with S3 files but no S3Options."""
        from file_dataset._core_file import validate_each_file

        files = {
            "f1": "s3://bucket/file.txt",
        }

        # Should raise error for S3 files without options
        errors = validate_each_file(files, s3_options=None, do_existence_checks=True)
        assert len(errors) == 1
        assert "__s3_options__" in errors
        assert "S3Options required" in errors["__s3_options__"]
