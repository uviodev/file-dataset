"""Script utilities for file-dataset project."""

import subprocess  # nosec B404
import sys
from pathlib import Path

# Exit code for safety when vulnerabilities are found
SAFETY_VULNERABILITY_EXIT_CODE = 64


def run_all_linting() -> None:
    """Run all linting checks (ruff, bandit, mypy, safety)."""
    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent
    src_dir = project_root / "src"
    tests_dir = project_root / "tests"

    print("🔍 Running all linting checks...")
    print("-" * 60)

    # Track if any check fails
    failed = False

    # Run ruff linting
    print("\n📋 Running ruff linting...")
    result = subprocess.run(  # nosec B603
        [sys.executable, "-m", "ruff", "check", str(src_dir), str(tests_dir)],
        cwd=project_root,
        check=False,
    )
    if result.returncode != 0:
        failed = True

    # Run ruff formatting check
    print("\n🎨 Checking code formatting...")
    result = subprocess.run(  # nosec B603
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            str(src_dir),
            str(tests_dir),
        ],
        cwd=project_root,
        check=False,
    )
    if result.returncode != 0:
        failed = True
        print("💡 Tip: Run 'uv run ruff format src/ tests/' to auto-format")

    # Run bandit security checks
    print("\n🔒 Running security checks (bandit)...")
    result = subprocess.run(  # nosec B603
        [sys.executable, "-m", "bandit", "-r", str(src_dir), "-c", "pyproject.toml"],
        cwd=project_root,
        check=False,
    )
    if result.returncode != 0:
        failed = True

    # Run mypy type checking
    print("\n🔤 Running type checking (mypy)...")
    result = subprocess.run(  # nosec B603
        [sys.executable, "-m", "mypy", str(src_dir)],
        cwd=project_root,
        check=False,
    )
    if result.returncode != 0:
        failed = True

    # Run safety dependency checks
    print("\n📦 Checking dependencies for vulnerabilities (safety)...")
    result_text: subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]
    result_text = subprocess.run(  # nosec B603
        [sys.executable, "-m", "safety", "check", "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result_text.returncode not in [0, SAFETY_VULNERABILITY_EXIT_CODE]:
        failed = True
    elif result_text.returncode == SAFETY_VULNERABILITY_EXIT_CODE:
        print("⚠️  Some dependency vulnerabilities found")

    # Summary
    print("\n" + "-" * 60)
    if failed:
        print("❌ Some linting checks failed. Please fix the issues above.")
        sys.exit(1)
    else:
        print("✅ All linting checks passed!")
        sys.exit(0)


if __name__ == "__main__":
    run_all_linting()
