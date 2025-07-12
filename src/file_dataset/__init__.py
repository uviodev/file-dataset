try:
    from importlib.metadata import version, PackageNotFoundError
    __version__ = version("file-dataset")
except (ImportError, PackageNotFoundError):
    __version__ = None
