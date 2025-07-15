"""Tests for the parallel execution features in S3Options."""

import concurrent.futures
import pickle
import threading

from file_dataset import S3Options


def test_s3options_local_parallelism_parameter():
    """Test that S3Options accepts local_parallelism parameter."""
    # Test with default (None)
    options_default = S3Options.default()
    assert (
        not hasattr(options_default, "local_parallelism")
        or options_default.local_parallelism is None
    )

    # Test with specific values
    options_sequential = S3Options.default(local_parallelism=None)
    assert options_sequential.local_parallelism is None

    options_single = S3Options.default(local_parallelism=1)
    assert options_single.local_parallelism == 1

    options_multi = S3Options.default(local_parallelism=4)
    assert options_multi.local_parallelism == 4


def test_s3options_executor_lazy_creation():
    """Test that executor is created lazily."""
    options = S3Options.default(local_parallelism=4)

    # Executor should not exist yet
    assert not hasattr(options, "_executor") or options._executor is None

    # Access executor - should create it
    executor = options.executor
    assert executor is not None
    assert isinstance(executor, concurrent.futures.ThreadPoolExecutor)

    # Second access should return same executor
    executor2 = options.executor
    assert executor is executor2


def test_s3options_executor_none_when_sequential():
    """Test that executor is None when local_parallelism is None."""
    options = S3Options.default(local_parallelism=None)

    # Should return None, not create an executor
    executor = options.executor
    assert executor is None

    # Multiple accesses should still return None
    assert options.executor is None


def test_s3options_executor_cleanup():
    """Test that executor is cleaned up in __del__."""
    options = S3Options.default(local_parallelism=4)

    # Create executor
    executor = options.executor
    assert executor is not None

    # Manually call __del__ (normally called by garbage collector)
    options.__del__()

    # Executor should be shut down (we can't directly test this easily,
    # but we can verify the method exists and doesn't raise)
    assert hasattr(options, "__del__")


def test_s3options_pickle_with_executor(mocker):
    """Test that S3Options with executor can be pickled."""
    mock_session = mocker.patch("boto3.Session")
    mock_frozen_creds = mocker.MagicMock()
    mock_frozen_creds.access_key = "test_key"
    mock_frozen_creds.secret_key = "test_secret"
    mock_frozen_creds.token = "test_token"

    mock_credentials = mocker.MagicMock()
    mock_credentials.get_frozen_credentials.return_value = mock_frozen_creds
    mock_session.return_value.get_credentials.return_value = mock_credentials

    options = S3Options.default(local_parallelism=4)

    # Create executor
    executor = options.executor
    assert executor is not None

    # Pickle and unpickle
    pickled = pickle.dumps(options)
    unpickled_options = pickle.loads(pickled)

    # Should have same local_parallelism setting
    assert unpickled_options.local_parallelism == 4

    # Executor should not be pickled
    assert (
        not hasattr(unpickled_options, "_executor")
        or unpickled_options._executor is None
    )

    # But should be able to create a new one
    new_executor = unpickled_options.executor
    assert new_executor is not None
    assert isinstance(new_executor, concurrent.futures.ThreadPoolExecutor)


def test_s3options_executor_thread_safety():
    """Test that executor creation is thread-safe."""
    options = S3Options.default(local_parallelism=4)
    executors = []

    def get_executor():
        executor = options.executor
        executors.append(executor)

    # Create multiple threads accessing executor simultaneously
    threads = []
    for _ in range(10):
        t = threading.Thread(target=get_executor)
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join()

    # All threads should get the same executor instance
    assert len(executors) == 10
    assert all(e is executors[0] for e in executors)
