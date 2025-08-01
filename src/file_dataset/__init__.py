"""File Dataset - A library for managing file datasets in cloud storage."""

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("file-dataset")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"

# Export core functionality
from ._reader import file_dataframe_reader, row_reader
from ._writer import write_files
from .exceptions import FileDatasetError
from .pipeline import Pipeline
from .s3_options import S3Options

__all__ = [
    "FileDatasetError",
    "Pipeline",
    "S3Options",
    "__version__",
    "file_dataframe_reader",
    "row_reader",
    "write_files",
]
