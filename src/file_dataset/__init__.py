"""File Dataset - A library for managing file datasets in cloud storage."""

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("file-dataset")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"

# Export core functionality
from ._reader import reader
from ._writer import write_files
from .exceptions import FileDatasetError
from .options import Options
from .pipeline import Pipeline

__all__ = [
    "FileDatasetError",
    "Options",
    "Pipeline",
    "__version__",
    "reader",
    "write_files",
]
