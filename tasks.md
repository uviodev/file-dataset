# File Dataset Implementation Tasks

This document outlines the implementation plan for the file-dataset library, progressing from simple local operations to full S3 and Ray integration.

## Task 1: Implement reader into_temp_dir for local directories
**Status**: ✅ Completed
**Description**: Create basic reader functionality that copies files from a local directory to a temporary directory.

**Requirements**:
- Implement `file_dataset.reader(row=files_dict)` that accepts a dictionary mapping filenames to local paths
- Implement `.into_temp_dir()` context manager that copies files to a temporary directory
- Support cleanup of temporary directories on context exit
- Handle file copying errors gracefully
- When a file is missing FileDatasetError is raised; FileDatasetError contains a mapping of filename to error message.

**Testing**:
- Test with single file and multiple files
- Test context manager cleanup
- Test error handling for missing source files. When source files are not found then a clear exception should be raised.

## Task 2: Implement writer into_path for local directories
**Status**: ✅ Completed
**Description**: Create basic writer functionality that copies files from one local directory to another with ID-based organization.

**Requirements**:
- Implement `file_dataset.write_files(row=files_dict, into_path, id)` ; into-path and id are kwarg only.
- Create directory structure: `{into_path}/{id}/filename`
- Return mapping of filenames to their final paths
- Handle directory creation and file copying
- Handle errors: if an expected file is missing then a clear FileDatasetError error should be raised with the missing file
- When an output file is optional, then it will be populated with a value of `None`. Ensure this None is propagated to the output.

**Testing**:
- Test writing single and multiple files
- Test directory creation
- Test return value format
- Test error handling for write permissions
- Test failures; if files can't be found then FileDatasetError is raised and no files are written
- Test optional output (None value in output map) does not trigger

## Task 3: Implement simple Options class with S3 client caching
**Status**: ✅ Completed
**Description**: Create Options class to manage S3 credentials and client creation with thread-safe caching.

**Requirements**:
- Implement `file_dataset.Options` class
- Support `Options.default()` class method using boto3 default session frozen_credentials. use adapative client retries config retry mode is "adaptive" with 3 retries.
- Lazy initialization of S3 client with thread-safe caching
- Make Options serializable (pickle-able) for distributed computing
- Store session_kwargs and s3_client_kwargs instead of live clients

**Testing**:
- Test default options creation
- Test S3 client lazy initialization
- Test thread safety with concurrent access
- Test pickle serialization/deserialization
- Test s3transfer options (multipart threshold and multipart size can be configured)

## Task 4: Implement reader support for S3 data
**Status**: ✅ Completed
**Description**: Extend reader functionality to download files from S3 to temporary directories.

**Requirements**:
- Modify `file_dataset.reader()` to accept S3 URLs (s3://bucket/key format) in the files dict values
- Add `options` parameter to reader for S3 credentials
- Implement S3 download logic in `.into_temp_dir()`; ensure s3_client uses s3transfer methods (s3_client.download_file; do not use get_object directly)
- Maintain same interface as local file reader
- Handle S3 errors (permissions, missing files, network issues)

**Testing**:
- Test S3 file downloads using moto library for mocking
- Test mixed local and S3 file sources
- Test error handling for missing S3 objects; raise standard FileDatasetError for the files that can't be accessed
- Test with different S3 credentials configurations

## Task 5: Implement writer support for S3 uploads
**Status**: ✅ Completed
**Description**: Extend writer functionality to upload files to S3 with proper path organization.

**Requirements**:
- Modify `file_dataset.write_files()` to detect S3 paths (s3://bucket/prefix format)
- Implement S3 upload logic with path structure: `s3://bucket/prefix/id/filename`
- Add `options` parameter for S3 credentials
- Return S3 URLs in the result mapping
- Handle S3 upload errors gracefully
- Ensure use of s3_client.upload_file to ensure multipart uploads

**Testing**:
- Test S3 uploads using moto library
- Test mixed local and S3 destinations
- Test error handling for S3 permissions
- Test return value format with S3 URLs

## Task 6: Implement reader support for DataFrames with partial failure handling
**Status**: ✅ Completed
**Description**: Extend reader functionality to process pandas DataFrames with graceful handling of row-level failures.

**Requirements**:
- Implement FileDataFrameReader. In its constructor it takes in a pandas DataFrame, which is saved as an instance variable.
- Implement `into_temp_dir()` which yields a stream of temporary directories. Each DataFrame row represents a set of files to be processed together. It opens a Reader for each row then delegates to `into_temp_dir()` to the reader for each row. If the reader fails to read the row then the row is skipped and an error message is logged.
    - Log which rows failed and why (for debugging). Use the user-defined `id` column if present or else fall back to the index column.
- Modify `core.reader()` function to add a new kwarg, `dataframe`. This kwarg should be mutually exclusive with `row` (otherwise ValueError is raised). Exactly one should be specified. When `dataframe` arg is specified then  FileDataFrameReader is returned.

**Testing**:
- Test `reader(dataframe=all_successful_rows, options=defaults)` with all successful rows and confirm each file was downloaded to a temp dir.
- Test `reader(dataframe=partial_failure, options=defaults)` with some failing rows (missing files, permission errors)
- Test mixed local and S3 sources with partial failures
- Verify failed rows are properly dropped from results
- Test empty DataFrame and DataFrame with all failed rows; when all rows fail then the reader should fail with a message to check error logs.

## Task 7: Implement write_files support for DataFrames with partial failure handling
**Status**: ✅ Completed
**Description**: Extend write_files functionality to process multiple rows from DataFrames with graceful partial failure handling.

**Requirements**:
- Modify `file_dataset.write_files()` to accept a `dataframe` kwarg, in addition to the `row` kwarg. The DataFrame is a new kwarg mutually exclusive with row kwarg.
    - Refactor existing logic to `_write_row()`
    - Process each row independently: if one row fails, continue with others
    - Return results only for successfully written rows, maintaining row correspondence
- Handle partial failures gracefully: if some files in a row fail, skip entire row
- Maintain same output path structure: each row gets its own ID-based directory structure

**Testing**:
- Test batch writing with all successful rows
- Test batch writing with some failing rows (permission errors, disk space)
- Test mixed local and S3 destinations with partial failures
- Verify failed rows are dropped and successful rows have correct output paths
- Test row-level failure isolation (one bad row doesn't affect others)
- Test empty input and all-failed scenarios

## Task 8a: Ensure FileDataFrameReader will return "id" for each row (fixing bug)
**Status**: ✅ Completed
**Description**: Ensure FileDataFrameReader will return "id" for each row

**Requirements**
- Since `into_temp_dir()` generator drops failing rows, the `into_temp_dir()` function will need to be modified to return the ID of the row.
- Do the refactor to yield a row and temp dir for the streaming `into_temp_dir()` implementation.
- Modify code usage of this API accordingly in the documentation.

**Testing**:
- Update existing tests to test for the id.
- In particular, for the partial failure case, make sure the id of the successful rows match the expected ids.


## Task 8b: Implement Pipeline class
**Status**: Not started
**Description**: Create Pipeline class that combines read → process → write workflow for batch processing of file datasets.

**Requirements**:
- Implement `file_dataset.Pipeline(fn, write_options)` class
- Add the code to a new file, pipeline.py. Use core and options primitives to implement
- Make Pipeline callable with pandas DataFrame with column for `id` and files. Raise ValueError if not provided.
- Process each row in a streaming manner: for each row, download files → call user function → upload results; use the streaming `into_temp_dir()` implementation for data frame
- Use separate temporary directories for each row to avoid disk space issues
- Drop failed rows rather than failing entire batch
- Make Pipeline pickle-able for distributed computing
- Return output DataFrame with result file URLs

**Testing**:
- Test Pipeline with simple user functions
- Test with pandas DataFrame containing multiple rows
- Test for clear errors when the user's data frame doesn't provide an ID column, or no file names are found in the columns.
- Test error handling and row dropping behavior
- Test pickle serialization of Pipeline objects
- Test temporary directory cleanup


## Task 9: Implement reader support for PyArrow size table
**Status**: Not started
**Description**: Add functionality to create PyArrow tables containing file size metadata without downloading the actual files.

**Requirements**:
- Implement `.into_size_table()` method on Reader and FileDataFrameReader; it returns a PyArrow table.
    - For local files: use file system stat operations
    - For S3 files: use S3 HEAD operations to get object metadata on every row. Concatenate the outputs. Important: do not download the files in this case! Drop error rows.
- Return PyArrow table with schema: `{id: pa.string(), filename: pa.int64()}` (where filename gives the size in bytes for each file column)
- Handle errors for inaccessible files; drop error rows but add log statements with the issues.

**Testing**:
- Test size table creation for local files
- Test size table creation for S3 files using moto
- Test error handling for missing files
- Test PyArrow table schema and data types

## Task 10: Implement reader support for PyArrow blob table
**Status**: Not started
**Description**: Add functionality to load file contents directly into PyArrow tables as binary data.

**Requirements**:
- Implement `.into_blob_table()` method on Reader and FileDataFrameReader.
- Load file contents into memory as binary data.
- Return PyArrow table with schema: `{id: pa.string(), filename: pa.binary()}`
- Handle memory management for large files
- Support both local and S3 files

**Testing**:
- Test blob table creation with small files (< 1KB as per guidelines)
- Test mixed local and S3 sources
- Test PyArrow table schema and binary data integrity
- Test memory usage with multiple files


## Task 11: Test Pipeline with local Ray data integration
**Status**: Not started
**Description**: Verify Pipeline class works correctly with Ray's map_batches functionality for distributed processing.

**Requirements**:
- Create tests using Ray's `map_batches` with Pipeline objects
- Test with pandas batch format
- Verify distributed execution across multiple workers
- Test with various batch sizes and concurrency settings
- Ensure proper resource management (memory, disk space)

**Testing**:
- Test Ray dataset creation from CSV with file URLs
- Test Pipeline execution with Ray's map_batches
- Test scaling with different concurrency levels (keep the concurrency levels small though for unit testing)
- Test resource cleanup after Ray jobs complete
- Test error handling in distributed environment

## Task 12: Implement custom Ray data source for blob tables
**Status**: Not started
**Description**: Create Ray data source that efficiently loads file datasets into cluster memory as blob tables.

**Requirements**:
- Implement `file_dataset.ray.blob_reader(dataframe, batch_size, options)`
- Raise ValueError if dataframe does not have an `id` column that is unique
- Create custom Ray Datasource following Ray's API
- Use `into_size_table()` for memory estimation on first batch (do not call this function for the whole dataset but use self.dataframe.head(batch_size))`
- For `get_read_tasks()`: Create one read task that will yield `batch_size` rows for each dataset that returns `into_blob_table()` for each dataframe. Ignore the parallelism command and require the user to specify `batch_size` as the blob tables can out-of-memory if they exceed `batch_size` rows.
- Return Ray dataset with binary data that can be further processed
- Support zero-copy operations if possible

**Testing**:
- Test blob_reader with various batch sizes
- Test memory usage and estimation accuracy
- Test integration with Ray's dataset library pipeline
- Test zero-copy operations when available
- Add an integration test with a pseudo-realistic dataset with 1000 rows (keeping individual files < 1KB)

## Implementation Notes

### Testing Strategy
- Use moto library to mock all S3 operations
- Keep all testdata files under 1KB in size; put them in the folder tests/data/
- Focus on integration tests that verify end-to-end workflows
- Test error conditions and edge cases thoroughly

### Code Quality
- Follow established linting rules and security practices
- Ensure all code passes `uv run lint` checks
- Add comprehensive docstrings following Google convention
- Use type hints throughout the codebase

### Dependencies
Key dependencies to add as implementation progresses:
- `boto3` for S3 operations
- `pyarrow` for table operations
- `pandas` for DataFrame handling
- `ray` for distributed computing
- `moto` for S3 testing (dev dependency)
