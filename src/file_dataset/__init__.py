"""File Dataset - A library for managing file datasets in cloud storage."""

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("file-dataset")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown"
