"""Core file dataset operations."""

import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from .exceptions import FileDatasetError


class Reader:
    """Handles reading files from local filesystem with context manager support."""

    def __init__(self, files_dict: dict[str, str | Path]) -> None:
        """Initialize Reader with files dictionary.

        Args:
            files_dict: Dictionary mapping filenames to their source paths
        """
        self.files_dict = files_dict

    @contextmanager
    def into_temp_dir(self) -> Generator[Path, None, None]:
        """Copy files to temporary directory and yield the directory path.

        Returns:
            Path to temporary directory containing copied files

        Raises:
            FileDatasetError: If any files cannot be copied
        """
        # Validate all files exist before starting copy operation
        file_errors: dict[str, str] = {}

        for filename, source_path in self.files_dict.items():
            source = Path(source_path)
            if not source.exists():
                file_errors[filename] = f"Source file not found: {source}"
            elif not source.is_file():
                file_errors[filename] = f"Source path is not a file: {source}"

        if file_errors:
            raise FileDatasetError(
                file_errors, "Cannot copy files to temporary directory"
            )

        # Create temporary directory and copy files
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)

            # Copy each file to temp directory with original filename
            for filename, source_path in self.files_dict.items():
                source = Path(source_path)
                dest = temp_dir / filename

                try:
                    shutil.copy2(source, dest)
                except OSError as e:
                    file_errors[filename] = f"Failed to copy file: {e}"

            # If any copies failed, raise error
            if file_errors:
                raise FileDatasetError(file_errors, "Failed to copy some files")

            yield temp_dir


def reader(*, row: dict[str, str | Path]) -> Reader:
    """Create a Reader instance for the given files.

    Args:
        row: Dictionary mapping filenames to their source paths (keyword-only)

    Returns:
        Reader instance
    """
    return Reader(row)
