"""Tests for S3 utilities module."""

from pathlib import Path

from file_dataset.s3_utils import is_s3_url, parse_s3_url


class TestParseS3Url:
    """Test cases for parse_s3_url function."""

    def test_valid_s3_url_with_key(self):
        """Test parsing valid S3 URL with bucket and key."""
        bucket, key = parse_s3_url("s3://my-bucket/path/to/file.txt")
        assert bucket == "my-bucket"
        assert key == "path/to/file.txt"

    def test_valid_s3_url_with_nested_path(self):
        """Test parsing S3 URL with deeply nested path."""
        bucket, key = parse_s3_url("s3://data-bucket/users/123/datasets/file.csv")
        assert bucket == "data-bucket"
        assert key == "users/123/datasets/file.csv"

    def test_valid_s3_url_bucket_only(self):
        """Test parsing S3 URL with bucket only."""
        bucket, key = parse_s3_url("s3://just-bucket")
        assert bucket == "just-bucket"
        assert key == ""

    def test_valid_s3_url_bucket_with_trailing_slash(self):
        """Test parsing S3 URL with bucket and trailing slash."""
        bucket, key = parse_s3_url("s3://bucket-name/")
        assert bucket == "bucket-name"
        assert key == ""

    def test_valid_s3_url_single_file(self):
        """Test parsing S3 URL with single file in bucket root."""
        bucket, key = parse_s3_url("s3://my-bucket/file.txt")
        assert bucket == "my-bucket"
        assert key == "file.txt"

    def test_invalid_url_http(self):
        """Test parsing invalid HTTP URL returns None."""
        result = parse_s3_url("http://example.com/file.txt")
        assert result is None

    def test_invalid_url_https(self):
        """Test parsing invalid HTTPS URL returns None."""
        result = parse_s3_url("https://bucket.s3.amazonaws.com/file.txt")
        assert result is None

    def test_invalid_url_missing_scheme(self):
        """Test parsing URL missing s3:// scheme returns None."""
        result = parse_s3_url("bucket/file.txt")
        assert result is None

    def test_invalid_url_malformed_s3(self):
        """Test parsing malformed S3 URL returns None."""
        result = parse_s3_url("s3:/bucket/file.txt")  # Missing one slash
        assert result is None

    def test_invalid_url_empty_string(self):
        """Test parsing empty string returns None."""
        result = parse_s3_url("")
        assert result is None

    def test_invalid_url_whitespace(self):
        """Test parsing whitespace string returns None."""
        result = parse_s3_url("   ")
        assert result is None

    def test_edge_case_bucket_with_special_chars(self):
        """Test parsing S3 URL with bucket containing valid special characters."""
        bucket, key = parse_s3_url("s3://my-bucket-123/file.txt")
        assert bucket == "my-bucket-123"
        assert key == "file.txt"


class TestIsS3Url:
    """Test cases for is_s3_url function."""

    def test_valid_s3_url(self):
        """Test detection of valid S3 URL."""
        assert is_s3_url("s3://bucket/file.txt") is True

    def test_valid_s3_url_bucket_only(self):
        """Test detection of S3 URL with bucket only."""
        assert is_s3_url("s3://my-bucket") is True

    def test_valid_s3_url_complex_path(self):
        """Test detection of S3 URL with complex path."""
        assert is_s3_url("s3://data/path/to/nested/file.json") is True

    def test_local_absolute_path(self):
        """Test detection of local absolute path."""
        assert is_s3_url("/local/path/file.txt") is False

    def test_local_relative_path(self):
        """Test detection of local relative path."""
        assert is_s3_url("relative/path/file.txt") is False

    def test_http_url(self):
        """Test detection of HTTP URL."""
        assert is_s3_url("http://example.com/file.txt") is False

    def test_https_url(self):
        """Test detection of HTTPS URL."""
        assert is_s3_url("https://bucket.s3.amazonaws.com/file.txt") is False

    def test_path_object(self):
        """Test detection with Path object."""
        path_obj = Path("/local/path/file.txt")
        assert is_s3_url(path_obj) is False

    def test_empty_string(self):
        """Test detection with empty string."""
        assert is_s3_url("") is False

    def test_malformed_s3_url(self):
        """Test detection of malformed S3 URL."""
        assert (
            is_s3_url("s3:/bucket/file.txt") is False
        )  # Missing slash, doesn't start with s3://

    def test_case_sensitivity(self):
        """Test that detection is case sensitive."""
        assert is_s3_url("S3://bucket/file.txt") is False  # Uppercase S3
