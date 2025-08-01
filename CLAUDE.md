# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`file-dataset` is a Python library that simplifies working with file datasets stored in cloud blob storage (like S3). A file dataset is a pandas DataFrame where columns represent file names and rows contain S3 URLs pointing to files. This library is optimized for large files (100MB+ images, blobs) but works well for smaller files too.

The library provides:
- Explicit S3 read/write operations (eager, not lazy)
- Integration with Ray Data for distributed processing
- Pipeline abstraction for batch transformations
- Support for both local and S3 storage paths

### Comparison with Other Libraries

- **vs s3fs/s3 mountpoint**: file-dataset makes S3 operations explicit rather than blurring the line between local and remote storage
- **vs s3 mountpoint**: No custom setup required; uses standard boto3 features
- **vs LanceDB**: Doesn't create a new data format; focuses on adapting existing "file dataset" formats

## Development workflow
To develop a new feature:

1. Read from tasks.md to find the next task to do, which is not yet done.
2. Erase everything in feature_plan.md.
3. Copy the task in progress (from step 1) into `feature_plan.md`.
4. Plan the functionality to be added and update `feature_plan.md` accordingly.
    * Taking into account understanding of the codebase and README.md, plan the code changes to make at a high level.
    * Think of a few high level test cases. Add these to a section called `testing`
5. Ask any clarifying questions. Only continue once questions are answered. Ensure feature_plan.md has removed questions and replaced the questions with answers.
6. Explicitly confirm that the feature is ready to be worked on.
6. Reread feature_plan.md, and implement the feature using red-green refactor loop.
    a. Add a simple test for the code. Run the test and confirm it is failing due to code within the file_dataset library.
    b. Implement a code change to make the test pass.
    c. Run the linter to ensure there are no basic mistakes
    d. Rerun the failing test to make sure it passes; continue changing the code until it passes.
    e. Confirm all tests pass (not just the previously failing one); fix any failing ones.
    f. Repeat the loop by adding a new test and then going through each instruction.
7. Run `git add .` and then do one last confirmation that the linting passes (`uv run lint`) and tests all pass (`uv run test`).
8. Update any documentation in the src/ folder with hints or tips about the software usage.
9. In tasks.md mark the task as done.

## Development Commands
The project uses the `uv` command. Prepend typical commands with `uv run` such as `uv run pytest` or `uv run python`.

### Running Tests
The project provides a convenient `test` command that runs pytest:

```bash
# Run all tests (equivalent to `uv run pytest tests/`)
uv run test

# Run tests with additional pytest arguments
uv run test -v              # Verbose output
uv run test --lf            # Run only last failed tests
uv run test --lf -v         # Combine flags
uv run test -k "test_s3"    # Run specific tests by pattern
```

You can also run pytest directly:

```bash
uv run pytest tests/
```

During development, to avoid too many log messages, rerun only failing tests with the last failed `--lf` flag:

```bash
uv run test --lf -v
```

Once the tests are passing, rerun all tests to confirm nothing else is broken.

### Linting and Formatting

The project uses `ruff` for linting and formatting with extensive security and docstring checks:

```bash
# Run ALL linting checks at once (recommended)
uv run lint

# Or run individual checks:
# Run linter
uv run ruff check src/ tests/

# Run linter with auto-fix
uv run ruff check --fix src/ tests/

# Format code
uv run ruff format src/ tests/

# Run security checks with bandit
uv run bandit -r src/

# Run type checking
uv run mypy src/
```

### Pre-commit Hooks

Install and run pre-commit hooks to ensure code quality:

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run all hooks manually
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff --all-files
```

The pre-commit configuration includes:
- **Ruff**: Linting and formatting with security rules (S), docstring checks (D), and many other quality checks
- **Bandit**: Additional security vulnerability scanning
- **Safety**: Checks dependencies for known security vulnerabilities
- **MyPy**: Static type checking
- **Various checks**: Trailing whitespace, YAML validation, merge conflicts, etc.

## Architecture & Structure

### Project Layout
- `src/file_dataset/` - Main package directory (currently only contains version info)
- `tests/` - Test directory with pytest configuration
- Uses `uv` as package manager (indicated by `uv.lock`)
- Python 3.12+ required

### Core Architecture

The library provides several key components:

1. **Core File Operations** (`file_dataset._core_file`)
   - Unified interface for local and S3 file operations
   - Batch operations: `copy_each_file()`, `read_each_file_size()`, `read_each_file_contents()`, `validate_each_file()`
   - Parallel execution support with configurable thread pools
   - Uses S3Transfer API for multipart uploads, automatic retries, and optimized transfers
   - Returns both successful results and errors for graceful partial failure handling

2. **Reader API** (`file_dataset.file_dataframe_reader()`)
   - Downloads S3 files to temporary directories via `into_temp_dir()`
   - Loads data into PyArrow tables via `into_blob_table()` or `into_size_table()`
   - Leverages core file operations for parallel processing
   - Supports both single rows and batch operations

3. **Writer API** (`file_dataset.write_files()`)
   - Uploads local files to S3 with user-defined IDs
   - Supports both S3 and local path destinations
   - Uses batch operations from core file module
   - Returns mapping of filenames to their final locations

4. **Pipeline Class** (`file_dataset.Pipeline`)
   - Combines read → process → write workflow
   - Processes pandas DataFrames row by row
   - Pickle-able for use with Ray's `map_batches()`
   - Drops failed rows rather than failing entire batch

5. **Ray Integration**
   - `file_dataset.ray.blob_reader()` for loading file datasets into Ray
   - Automatic parallelism configuration based on number of file columns
   - Pipelines work seamlessly with `ray.data.Dataset.map_batches()`
   - Optimized credential handling to minimize EC2 metadata server load

6. **S3Options Management** (`file_dataset.S3Options`)
   - Handles S3 credentials and client configuration
   - Configurable local parallelism with thread pool management
   - S3Transfer configuration for optimized uploads/downloads
   - Thread-safe lazy initialization with proper locking
   - Frozen credentials caching to reduce AWS API calls
   - Fully serializable for distributed computing

7. **File DataFrame Utilities** (`file_dataset.file_dataframe`)
   - Validation functions for file columns and ID uniqueness
   - Centralized DataFrame validation logic
   - Helper functions for identifying file columns by extension

### Key Design Principles

- **Explicit Operations**: All S3 operations are eager and explicit
- **Bytes Only**: Library deals only in bytes; serialization is user's responsibility
- **Separation of Concerns**: User functions work with local directories, unaware of S3
- **Memory Efficiency**: Each row gets its own temp directory to avoid disk errors
- **Distributed-Ready**: All components are pickle-able for Ray/multiprocessing

### Implementation Guidelines

When implementing features:
- Focus on simple and clear code
- Always support both S3 (`s3://bucket/path`) and local paths
- Use context managers for temp directory cleanup
- Ensure thread-safety for shared resources (like S3 clients)
- Make components serializable for distributed processing
- Handle failures gracefully (drop rows, don't crash pipelines)
- Keep user functions independent of file-dataset internals
- Always make sure linting checks pass
- Ensure good logging practices


When implementing tests:
- Rely on the moto library to mock all s3 calls
- use small files for testing; all files should be less than 1kb in size
