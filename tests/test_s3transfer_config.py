"""Test S3Transfer configuration usage in core file operations."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from boto3.s3.transfer import TransferConfig

from file_dataset._core_file import copy_each_file, read_each_file_contents
from file_dataset.s3_options import S3Options


def test_copy_file_passes_transfer_config():
    """Test that copy operations pass TransferConfig to S3 methods."""
    # Create custom transfer config
    transfer_config = TransferConfig(
        multipart_threshold=1024 * 25,  # 25KB for testing
        multipart_chunksize=1024 * 5,  # 5KB chunks
    )

    # Create S3Options with custom transfer config
    s3_options = S3Options(
        session_kwargs={}, s3_client_kwargs={}, s3_transfer_config=transfer_config
    )

    # Mock the S3 client
    mock_client = MagicMock()
    s3_options._s3_client = mock_client

    # Create a temporary file to upload
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"test content")
        tmp_path = tmp.name

    tmp2_path = None
    try:
        # Test upload_file (local to S3)
        copies = [(tmp_path, "s3://test-bucket/test-key")]
        copy_each_file(copies, s3_options)

        # Verify upload_file was called with the transfer config
        mock_client.upload_file.assert_called_once()
        args = mock_client.upload_file.call_args
        # Check if Config was passed
        assert len(args[0]) == 3  # Should have filename, bucket, key
        # Config should be passed as a keyword argument
        assert "Config" in args[1]
        assert args[1]["Config"] is s3_options.transfer_config

        # Reset mock
        mock_client.reset_mock()

        # Test download_file (S3 to local)
        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            tmp2_path = tmp2.name

        copies = [("s3://test-bucket/test-key", tmp2_path)]
        copy_each_file(copies, s3_options)

        # Verify download_file was called with the transfer config
        mock_client.download_file.assert_called_once()
        args = mock_client.download_file.call_args
        assert "Config" in args[1]
        assert args[1]["Config"] is s3_options.transfer_config

    finally:
        Path(tmp_path).unlink(missing_ok=True)
        if tmp2_path:
            Path(tmp2_path).unlink(missing_ok=True)


def test_read_file_contents_uses_transfer_config():
    """Test that read_file_contents uses download_fileobj with TransferConfig."""
    from io import BytesIO

    # Create custom transfer config
    transfer_config = TransferConfig(
        multipart_threshold=1024 * 25,
        multipart_chunksize=1024 * 5,
    )

    # Create S3Options with custom transfer config
    s3_options = S3Options(
        session_kwargs={}, s3_client_kwargs={}, s3_transfer_config=transfer_config
    )

    # Mock the S3 client
    mock_client = MagicMock()
    s3_options._s3_client = mock_client

    # Mock download_fileobj to write test content
    def mock_download(bucket, key, fileobj, **kwargs):  # noqa: ARG001
        fileobj.write(b"test content")

    mock_client.download_fileobj.side_effect = mock_download

    # Read S3 file contents
    files = {"test": "s3://test-bucket/test-key"}
    results, errors = read_each_file_contents(files, s3_options)

    # Should use download_fileobj with transfer config
    mock_client.download_fileobj.assert_called_once()
    args = mock_client.download_fileobj.call_args
    assert args[0][0] == "test-bucket"  # bucket
    assert args[0][1] == "test-key"  # key
    assert isinstance(args[0][2], BytesIO)  # fileobj
    assert "Config" in args[1]
    assert args[1]["Config"] is transfer_config

    assert results["test"] == b"test content"
    assert not errors


def test_multipart_config_behavior():
    """Test that multipart configuration is respected for large files."""
    # Create transfer config with very low multipart threshold
    transfer_config = TransferConfig(
        multipart_threshold=100,  # 100 bytes - very small for testing
        multipart_chunksize=50,  # 50 byte chunks
    )

    # Create S3Options with custom transfer config
    s3_options = S3Options(
        session_kwargs={}, s3_client_kwargs={}, s3_transfer_config=transfer_config
    )

    # Mock the S3 client
    mock_client = MagicMock()
    s3_options._s3_client = mock_client

    # Create a file larger than multipart threshold
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"x" * 200)  # 200 bytes - larger than threshold
        tmp_path = tmp.name

    try:
        # Upload the file
        copies = [(tmp_path, "s3://test-bucket/large-file")]
        copy_each_file(copies, s3_options)

        # Verify upload_file was called with our custom config
        mock_client.upload_file.assert_called_once()
        args = mock_client.upload_file.call_args

        # Check the transfer config is passed
        assert "Config" in args[1]
        config = args[1]["Config"]
        assert config.multipart_threshold == 100
        assert config.multipart_chunksize == 50

    finally:
        Path(tmp_path).unlink(missing_ok=True)


def test_default_transfer_config():
    """Test that default transfer config is used when none specified."""
    # Create S3Options without custom transfer config
    s3_options = S3Options(
        session_kwargs={},
        s3_client_kwargs={},
        s3_transfer_config=None,  # Will use default
    )

    # The default TransferConfig should be created
    assert s3_options.transfer_config is not None
    assert isinstance(s3_options.transfer_config, TransferConfig)
