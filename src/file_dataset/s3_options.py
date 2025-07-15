"""S3Options class for managing S3 configuration and credentials."""

import threading
from typing import Any

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


class S3Options:
    """Options for file dataset operations including S3 configuration.

    This class manages S3 credentials and client configuration with thread-safe
    caching and support for serialization (pickling) for distributed computing.

    Attributes:
        _session_kwargs: Keyword arguments for creating boto3 Session
        _s3_client_kwargs: Keyword arguments for creating S3 client
        _s3_transfer_config: Configuration for S3 transfers
        _s3_client: Cached S3 client instance (not serialized)
        _lock: Thread lock for safe client initialization
    """

    def __init__(
        self,
        session_kwargs: dict[str, Any],
        s3_client_kwargs: dict[str, Any],
        s3_transfer_config: TransferConfig | None = None,
    ) -> None:
        """Initialize S3Options with configuration parameters.

        Args:
            session_kwargs: Keyword arguments for boto3.Session
            s3_client_kwargs: Keyword arguments for S3 client creation
            s3_transfer_config: Optional S3 transfer configuration
        """
        self._session_kwargs = session_kwargs
        self._s3_client_kwargs = s3_client_kwargs
        self._s3_transfer_config = s3_transfer_config or TransferConfig()
        self._s3_client: Any | None = None
        self._lock = threading.Lock()

    @classmethod
    def default(
        cls,
        multipart_threshold: int | None = None,
        multipart_chunksize: int | None = None,
    ) -> "S3Options":
        """Create S3Options with default settings using boto3 default session.

        Uses frozen credentials from the default boto3 session and configures
        adaptive retry mode with 3 retries.

        Args:
            multipart_threshold: Threshold for multipart uploads in bytes
            multipart_chunksize: Size of chunks for multipart uploads in bytes

        Returns:
            S3Options instance with default configuration
        """
        # Get frozen credentials from default session
        session = boto3.Session()
        frozen_creds = session.get_credentials().get_frozen_credentials()

        # Create session kwargs with frozen credentials
        session_kwargs = {
            "aws_access_key_id": frozen_creds.access_key,
            "aws_secret_access_key": frozen_creds.secret_key,
        }

        if frozen_creds.token:
            session_kwargs["aws_session_token"] = frozen_creds.token

        # Configure adaptive retry mode with 3 retries
        config = Config(
            retries={
                "mode": "adaptive",
                "max_attempts": 3,
            }
        )

        s3_client_kwargs = {
            "config": config,
        }

        # Create transfer config if custom values provided
        transfer_config_kwargs = {}
        if multipart_threshold is not None:
            transfer_config_kwargs["multipart_threshold"] = multipart_threshold
        if multipart_chunksize is not None:
            transfer_config_kwargs["multipart_chunksize"] = multipart_chunksize

        transfer_config = (
            TransferConfig(**transfer_config_kwargs) if transfer_config_kwargs else None
        )

        return cls(
            session_kwargs=session_kwargs,
            s3_client_kwargs=s3_client_kwargs,
            s3_transfer_config=transfer_config,
        )

    @property
    def s3_client(self) -> Any:
        """Get or create S3 client with thread-safe lazy initialization.

        Returns:
            Boto3 S3 client instance
        """
        if self._s3_client is None:
            with self._lock:
                # Double-check pattern for thread safety
                if self._s3_client is None:
                    session = boto3.Session(**self._session_kwargs)
                    self._s3_client = session.client("s3", **self._s3_client_kwargs)
        return self._s3_client

    def __getstate__(self) -> dict[str, Any]:
        """Get state for pickling.

        Excludes the S3 client and lock which cannot be pickled.

        Returns:
            Dictionary of pickleable attributes
        """
        state = self.__dict__.copy()
        # Remove unpickleable objects
        state.pop("_s3_client", None)
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Set state after unpickling.

        Restores all attributes and creates a new lock.

        Args:
            state: Dictionary of attributes from pickling
        """
        self.__dict__.update(state)
        self._s3_client = None
        self._lock = threading.Lock()
