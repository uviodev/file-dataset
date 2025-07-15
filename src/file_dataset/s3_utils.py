"""S3 utility functions for file dataset operations."""

import re
from pathlib import Path


def parse_s3_url(url: str) -> tuple[str, str] | None:
    """Parse S3 URL into bucket and key.

    Args:
        url: S3 URL to parse in format s3://bucket/key

    Returns:
        Tuple of (bucket, key) if valid S3 URL, None otherwise

    Examples:
        >>> parse_s3_url("s3://my-bucket/path/to/file.txt")
        ('my-bucket', 'path/to/file.txt')
        >>> parse_s3_url("s3://bucket/")
        ('bucket', '')
        >>> parse_s3_url("http://example.com")
        None
    """
    match = re.match(r"^s3://([^/]+)/?(.*)$", url)
    if match:
        return match.group(1), match.group(2)
    return None


def is_s3_url(path: str | Path) -> bool:
    """Check if path is an S3 URL.

    Args:
        path: Path to check (string or Path object)

    Returns:
        True if path starts with s3://, False otherwise

    Examples:
        >>> is_s3_url("s3://bucket/file.txt")
        True
        >>> is_s3_url("/local/path/file.txt")
        False
        >>> is_s3_url(Path("/local/path"))
        False
    """
    return str(path).startswith("s3://")
