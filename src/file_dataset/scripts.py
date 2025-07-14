"""Script utilities for file-dataset project."""

import subprocess  # nosec B404
import sys
from pathlib import Path


def pre_commit() -> None:
    """Run pre-commit with all files."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent

    print(f"🔍 Running pre-commit on all files (path={project_root})...")
    print("-" * 60)

    # Run pre-commit with all files
    result = subprocess.run(  # nosec B603
        [sys.executable, "-m", "pre_commit", "run", "--all-files"],
        cwd=project_root,
        check=False,
    )

    # Exit with the same code as pre-commit
    sys.exit(result.returncode)


def test() -> None:
    """Run pytest tests/ with forwarded arguments."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent

    # Build command with pytest tests/ and any additional arguments
    cmd = [sys.executable, "-m", "pytest", "tests/"] + sys.argv[1:]

    # Run pytest
    result = subprocess.run(  # nosec B603
        cmd,
        cwd=project_root,
        check=False,
    )

    # Exit with the same code as pytest
    sys.exit(result.returncode)


if __name__ == "__main__":
    pre_commit()
