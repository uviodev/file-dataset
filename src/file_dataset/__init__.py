"""File Dataset - A library for managing file datasets in cloud storage."""

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("file-dataset")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"

# Export core functionality
from .core import reader, write_files
from .exceptions import FileDatasetError
from .options import Options

__all__ = ["FileDatasetError", "Options", "__version__", "reader", "write_files"]
