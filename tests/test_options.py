"""Tests for the Options class."""

import pickle
import threading

from moto import mock_aws

from file_dataset import Options


def test_options_default_creation(mocker):
    """Test creating Options with default settings."""
    mock_session = mocker.patch("boto3.Session")
    mock_frozen_creds = mocker.MagicMock()
    mock_frozen_creds.access_key = "test_key"
    mock_frozen_creds.secret_key = "test_secret"
    mock_frozen_creds.token = "test_token"

    mock_credentials = mocker.MagicMock()
    mock_credentials.get_frozen_credentials.return_value = mock_frozen_creds
    mock_session.return_value.get_credentials.return_value = mock_credentials

    options = Options.default()

    assert options is not None
    assert hasattr(options, "_session_kwargs")
    assert hasattr(options, "_s3_client_kwargs")

    # Check that frozen credentials were used
    mock_session.return_value.get_credentials.assert_called_once()
    mock_credentials.get_frozen_credentials.assert_called_once()


@mock_aws
def test_s3_client_lazy_initialization():
    """Test that S3 client is created lazily on first access."""
    options = Options.default()

    # Client should not exist yet
    assert not hasattr(options, "_s3_client") or options._s3_client is None

    # Access client - should create it
    client = options.s3_client
    assert client is not None

    # Second access should return same client
    client2 = options.s3_client
    assert client is client2


def test_options_pickle_serialization(mocker):
    """Test that Options can be pickled and unpickled."""
    mock_session = mocker.patch("boto3.Session")
    mock_frozen_creds = mocker.MagicMock()
    mock_frozen_creds.access_key = "test_key"
    mock_frozen_creds.secret_key = "test_secret"
    mock_frozen_creds.token = "test_token"

    mock_credentials = mocker.MagicMock()
    mock_credentials.get_frozen_credentials.return_value = mock_frozen_creds
    mock_session.return_value.get_credentials.return_value = mock_credentials

    options = Options.default()

    # Pickle and unpickle
    pickled = pickle.dumps(options)
    unpickled_options = pickle.loads(pickled)

    # Should have same attributes
    assert hasattr(unpickled_options, "_session_kwargs")
    assert hasattr(unpickled_options, "_s3_client_kwargs")

    # Client should not be pickled
    assert (
        not hasattr(unpickled_options, "_s3_client")
        or unpickled_options._s3_client is None
    )


def test_options_thread_safety():
    """Test that S3 client creation is thread-safe."""
    options = Options.default()
    clients = []

    def get_client():
        client = options.s3_client
        clients.append(client)

    # Create multiple threads accessing client simultaneously
    threads = []
    for _ in range(10):
        t = threading.Thread(target=get_client)
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join()

    # All threads should get the same client instance
    assert len(clients) == 10
    assert all(c is clients[0] for c in clients)


def test_s3_transfer_config(mocker):
    """Test that s3transfer options can be configured."""
    mock_session = mocker.patch("boto3.Session")
    mock_frozen_creds = mocker.MagicMock()
    mock_frozen_creds.access_key = "test_key"
    mock_frozen_creds.secret_key = "test_secret"
    mock_frozen_creds.token = "test_token"

    mock_credentials = mocker.MagicMock()
    mock_credentials.get_frozen_credentials.return_value = mock_frozen_creds
    mock_session.return_value.get_credentials.return_value = mock_credentials

    # Test with custom transfer config
    options = Options.default(
        multipart_threshold=1024 * 1024 * 10,  # 10MB
        multipart_chunksize=1024 * 1024 * 5,  # 5MB
    )

    assert options._s3_transfer_config is not None
    assert options._s3_transfer_config.multipart_threshold == 1024 * 1024 * 10
    assert options._s3_transfer_config.multipart_chunksize == 1024 * 1024 * 5


def test_adaptive_retry_config(mocker):
    """Test that adaptive retry mode is configured with 3 retries."""
    mock_session = mocker.patch("boto3.Session")
    mock_frozen_creds = mocker.MagicMock()
    mock_frozen_creds.access_key = "test_key"
    mock_frozen_creds.secret_key = "test_secret"
    mock_frozen_creds.token = "test_token"

    mock_credentials = mocker.MagicMock()
    mock_credentials.get_frozen_credentials.return_value = mock_frozen_creds
    mock_session.return_value.get_credentials.return_value = mock_credentials

    options = Options.default()

    # Check s3_client_kwargs has correct retry config
    assert "config" in options._s3_client_kwargs
    config = options._s3_client_kwargs["config"]
    assert config.retries["mode"] == "adaptive"
    assert config.retries["max_attempts"] == 3
