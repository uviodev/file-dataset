try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        __version__ = version("file-dataset")
    except PackageNotFoundError:
        __version__ = None
except ImportError:
    __version__ = None

def main() -> None:
    print("Hello from file-dataset!")
