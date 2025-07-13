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

## Development Commands
The project uses the `uv` command. Prepend typical commands with `uv run` such as `uv run pytest` or `uv run python`.

### Running Tests
Please run `pytest` command via `uv run`:

```bash
uv run pytest tests/
```

During development, to avoid too many log messages, rerun only failing tests with the last failed `--lf` flag:

```bash
uv run pytest tests/ --lf -v
```

Once the tests are passing, rerun all tests to confirm nothing else is broken.

## Architecture & Structure

### Project Layout
- `src/file_dataset/` - Main package directory (currently only contains version info)
- `tests/` - Test directory with pytest configuration
- Uses `uv` as package manager (indicated by `uv.lock`)
- Python 3.12+ required

### Core Architecture

The library provides several key components:

1. **Reader API** (`file_dataset.reader()`)
   - Downloads S3 files to temporary directories via `into_temp_dir()`
   - Loads data into PyArrow tables via `into_blob_table()` or `into_size_table()`
   - Supports both single rows and batch operations

2. **Writer API** (`file_dataset.write_files()`)
   - Uploads local files to S3 with user-defined IDs
   - Supports both S3 and local path destinations
   - Returns mapping of filenames to their final locations

3. **Pipeline Class** (`file_dataset.Pipeline`)
   - Combines read → process → write workflow
   - Processes pandas DataFrames row by row
   - Pickle-able for use with Ray's `map_batches()`
   - Drops failed rows rather than failing entire batch

4. **Ray Integration**
   - `file_dataset.ray.blob_reader()` for loading file datasets into Ray
   - Pipelines work seamlessly with `ray.data.Dataset.map_batches()`
   - Optimized for small dataset metadata (URLs) with large underlying files

5. **Options Management** (`file_dataset.Options`)
   - Handles S3 credentials and client configuration
   - Serializable for distributed computing
   - Thread-safe lazy initialization of boto3 clients

### Key Design Principles

- **Explicit Operations**: All S3 operations are eager and explicit
- **Bytes Only**: Library deals only in bytes; serialization is user's responsibility
- **Separation of Concerns**: User functions work with local directories, unaware of S3
- **Memory Efficiency**: Each row gets its own temp directory to avoid disk errors
- **Distributed-Ready**: All components are pickle-able for Ray/multiprocessing

### Implementation Guidelines

When implementing features:
- Always support both S3 (`s3://bucket/path`) and local paths
- Use context managers for temp directory cleanup
- Ensure thread-safety for shared resources (like S3 clients)
- Make components serializable for distributed processing
- Handle failures gracefully (drop rows, don't crash pipelines)
- Keep user functions independent of file-dataset internals