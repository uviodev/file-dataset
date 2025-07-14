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
**Status**: Not started
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
**Status**: Not started
**Description**: Extend reader functionality to process pandas DataFrames with graceful handling of row-level failures.

**Requirements**:
- Modify `file_dataset.reader()` to accept pandas DataFrame as input, in addition to a single row dict
- Each DataFrame row represents a set of files to be processed together
- Implement partial success: if some rows fail to load, drop them and continue with successful rows
- Return results only for successfully processed rows
- Log or track which rows failed and why (for debugging)
- Maintain same `.into_temp_dir()`, `.into_size_table()`, `.into_blob_table()` interface
- For `into_temp_dir()`: yield one temporary directory per successful row

**Testing**:
- Test DataFrame with all successful rows
- Test DataFrame with some failing rows (missing files, permission errors)
- Test mixed local and S3 sources with partial failures
- Verify failed rows are properly dropped from results
- Test error logging/tracking functionality
- Test empty DataFrame and DataFrame with all failed rows

## Task 7: Implement write_files support for DataFrames with partial failure handling
**Status**: Not started
**Description**: Extend write_files functionality to process multiple rows from DataFrames with graceful partial failure handling.

**Requirements**:
- Modify `file_dataset.write_files()` to accept list of file dictionaries (from DataFrame rows)
- Process each row independently: if one row fails, continue with others
- Return results only for successfully written rows, maintaining row correspondence
- Handle partial failures gracefully: if some files in a row fail, skip entire row
- Support batch operations for efficiency while maintaining row-level error isolation
- Maintain same interface: each row gets its own ID-based directory structure

**Testing**:
- Test batch writing with all successful rows
- Test batch writing with some failing rows (permission errors, disk space)
- Test mixed local and S3 destinations with partial failures
- Verify failed rows are dropped and successful rows have correct output paths
- Test row-level failure isolation (one bad row doesn't affect others)
- Test empty input and all-failed scenarios

## Task 8: Implement reader support for PyArrow size table
**Status**: Not started
**Description**: Add functionality to create PyArrow tables containing file size metadata without downloading the actual files.

**Requirements**:
- Implement `.into_size_table()` method on reader
- For local files: use file system stat operations
- For S3 files: use S3 HEAD operations to get object metadata
- Return PyArrow table with schema: `{filename: pa.int64()}` (size in bytes)
- Handle errors for inaccessible files

**Testing**:
- Test size table creation for local files
- Test size table creation for S3 files using moto
- Test error handling for missing files
- Test PyArrow table schema and data types

## Task 9: Implement reader support for PyArrow blob table
**Status**: Not started
**Description**: Add functionality to load file contents directly into PyArrow tables as binary data.

**Requirements**:
- Implement `.into_blob_table()` method on reader
- Load file contents into memory as binary data
- Return PyArrow table with schema: `{filename: pa.binary()}`
- Handle memory management for large files
- Support both local and S3 files

**Testing**:
- Test blob table creation with small files (< 1KB as per guidelines)
- Test mixed local and S3 sources
- Test PyArrow table schema and binary data integrity
- Test memory usage with multiple files

## Task 10: Implement Pipeline class
**Status**: Not started
**Description**: Create Pipeline class that combines read → process → write workflow for batch processing of file datasets.

**Requirements**:
- Implement `file_dataset.Pipeline(fn, write_options)` class
- Make Pipeline callable with pandas DataFrame containing file URLs and IDs
- Process each row: download files → call user function → upload results
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
- Create custom Ray Datasource following Ray's API
- Use `into_size_table()` for memory estimation on first batch
- Implement efficient batching strategy for memory management
- Return Ray dataset with binary data that can be further processed
- Support zero-copy operations where possible

**Testing**:
- Test blob_reader with various batch sizes
- Test memory usage and estimation accuracy
- Test integration with Ray's processing pipeline
- Test zero-copy operations when available
- Test with realistic file dataset sizes (keeping individual files < 1KB)

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
