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


def _validate_source_files(row: dict[str, str | Path | None]) -> dict[str, str]:
    """Validate that all non-None source files exist and are readable.

    Args:
        row: Dictionary mapping filenames to source paths or None

    Returns:
        Dictionary of filename to error message for any validation failures
    """
    file_errors: dict[str, str] = {}

    for filename, source_path in row.items():
        if source_path is None:
            continue  # Skip None values (optional files)

        source = Path(source_path)
        if not source.exists():
            file_errors[filename] = f"Source file not found: {source}"
        elif not source.is_file():
            file_errors[filename] = f"Source path is not a file: {source}"

    return file_errors


def _copy_files_to_destination(
    row: dict[str, str | Path | None], dest_dir: Path
) -> tuple[dict[str, Path | None], dict[str, str]]:
    """Copy files to destination directory.

    Args:
        row: Dictionary mapping filenames to source paths or None
        dest_dir: Destination directory path

    Returns:
        Tuple of (result mapping, error mapping)
    """
    result: dict[str, Path | None] = {}
    file_errors: dict[str, str] = {}

    for filename, source_path in row.items():
        if source_path is None:
            result[filename] = None
            continue

        source = Path(source_path)
        dest = dest_dir / filename

        try:
            shutil.copy2(source, dest)
            result[filename] = dest
        except OSError as e:
            file_errors[filename] = f"Failed to copy file: {e}"

    return result, file_errors


def write_files(
    row: dict[str, str | Path | None],
    *,
    into_path: str | Path,
    id: str,  # noqa: A002
) -> dict[str, Path | None]:
    """Write files to destination directory with ID-based organization.

    Args:
        row: Dictionary mapping filenames to their source paths or None
        into_path: Base destination directory (keyword-only)
        id: Unique identifier for this set of files (keyword-only)

    Returns:
        Dictionary mapping filenames to their final destination paths or None

    Raises:
        FileDatasetError: If any required files cannot be written
    """
    # Validate inputs
    if not row:
        return {}

    dest_base = Path(into_path)
    dest_dir = dest_base / id

    # Validate all non-None source files exist before starting
    file_errors = _validate_source_files(row)
    if file_errors:
        raise FileDatasetError(file_errors, "Cannot write files to destination")

    # Create destination directory
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise FileDatasetError(
            {"directory": f"Failed to create destination directory: {e}"},
            "Failed to create destination directory",
        ) from e

    # Copy files to destination
    result, copy_errors = _copy_files_to_destination(row, dest_dir)

    # If any copies failed, raise error
    if copy_errors:
        raise FileDatasetError(copy_errors, "Failed to copy some files")

    return result
