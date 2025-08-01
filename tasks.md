# File Dataset Implementation Tasks
High level goal is to optimize read/write parallelism when there are multiple files in the dataframe.
We are going to implement Local Parallelism.
Boto3 S3Transfer already has its own threadpool to copy data, so the main goal is to be able to initiate several transfers in parallel.

## Task 1: Create Core File Operations Library ✓

### Summary
Create a new library `_core_file.py` that centralizes all file operations (copy, read) with support for both local and S3 paths. This will provide a unified interface for file operations across the codebase.

### Implementation Notes
- Create `do_copy(copies: list[tuple[str | Path, str | Path]], s3_options: S3Options)` function
  - Validates each copy operation (source exists, destinations are unique)
  - Ensures s3_options is not None when S3 paths are involved
  - Executes copies sequentially (for now)
- Create `_do_single_copy(src: str | Path, dst: str | Path, s3_options: S3Options)` helper
  - Delegates to shutil.copy2() for local-to-local copies
  - Uses s3_options.s3_client for S3 operations
  - Handles all combinations: local→local, local→S3, S3→local, S3→S3
- Refactor existing code:
  - `write_files()` in writer module to use `do_copy()`
  - `into_temp_dir()` in reader module to use `do_copy()`

### Testing (High Level)
- Test all copy combinations (local→local, local→S3, S3→local, S3→S3)
- Test validation: non-existent sources, duplicate destinations
- Test error handling: permission errors, network failures
- Mock S3 operations using moto
- Verify that refactored writer and reader still work correctly

## Task 2: Add File Size and Content Reading APIs ✓

### Summary
Extend `_core_file.py` with functions to read file sizes and contents efficiently for both local and S3 files. These will support batch operations on multiple files.

### Implementation Notes
- Create high-level APIs:
  - `read_file_sizes(files: Mapping[str, str | Path], s3_options: S3Options) -> Mapping[str, int]`
  - `read_file_contents(files: Mapping[str, str | Path], s3_options: S3Options) -> Mapping[str, bytes]`
- Create low-level helpers:
  - `_read_file_size(path: str | Path, s3_options: S3Options) -> int`
    - Use os.path.getsize() for local files
    - Use HeadObject for S3 files
  - `_read_file_contents(path: str | Path, s3_options: S3Options) -> bytes`
    - Use open/read for local files
    - Use GetObject for S3 files
- Refactor existing code:
  - `into_size_table()` to use `read_file_sizes()`
  - `into_blob_table()` to use `read_file_contents()`

### Testing (High Level)
- Test reading sizes/contents from local and S3 files
- Test batch operations with mixed local/S3 paths
- Test error cases: missing files, access denied
- Verify memory efficiency with large files
- Test that refactored table functions maintain compatibility

## Task 3: Migrate to S3Transfer API ✓

### Summary
Replace low-level boto3 S3 API calls with S3Transfer methods for better performance and configuration options. S3Transfer provides built-in retry logic, multipart uploads, and bandwidth throttling.

### Implementation Notes
- Replace in `_core_file.py`, where applicable:
  - `put_object()` → `upload_file()` or `upload_fileobj()`
  - `get_object()` → `download_file()` or `download_fileobj()`
  - Keep `head_object()` for existence/size checks
- Ensure `s3_options.transfer_config` is passed to all S3Transfer calls
- Handle transfer configuration:
  - Multipart threshold and chunk size
  - Max concurrency for individual transfers
  - Bandwidth limits if configured
- Update error handling for S3Transfer-specific exceptions
- Ensure the user may still opt out of this behavior if they explicitly opt-out of it.


### Testing (High Level)
- Test file uploads trigger multipart behavior (pytest-mock is OK)
- Test transfer config options are respected
- Test retry behavior on transient failures
- Verify backwards compatibility is maintained


## Task 4: Ensure all s3_client operations are performed in `_core_file.py` ✓
### Summary
Move `validate_files` from the RowReader to `_core_file.py` with functions. This centralizes all S3 client operations in one module for better maintainability and consistency.

### Implementation Notes

#### 1. Create new validation functions in `_core_file.py`:
- `validate_each_file(files: Mapping[str, str | Path], s3_options: S3Options | None = None, do_existence_checks: bool = True) -> dict[str, str]`
  - Main entry point that validates multiple files
  - Returns dict of errors (empty if all valid)
  - If `do_existence_checks=False`, only does basic validation (URL format, etc.)

- `_validate_s3_path_format(path: str | Path, s3_options: S3Options | None) -> str | None`
  - Only validates S3 paths - returns None immediately for local paths
  - Validates S3 URL format and that s3_options is provided when needed
  - Returns error message for invalid S3 paths or None if valid

- `_file_exists(path: str | Path, s3_options: S3Options | None) -> bool`
  - For S3 files: uses `s3_options.s3_client.head_object()` to check existence
  - For local files: uses `Path.exists()` and `Path.is_file()`
  - Returns True if file exists and is accessible

#### 2. Refactor FileRowReader in `_reader.py`:
- Remove `_validate_files()` and `_validate_s3_file()` methods
- Replace with call to `validate_each_file()` from `_core_file.py`
- Update `into_temp_dir()` to use the new validation function

#### 3. Migration pattern:
```python
# Old code in _reader.py:
file_errors = self._validate_files()

# New code:
from file_dataset._core_file import validate_each_file
file_errors = validate_each_file(self.files_dict, self.options)
```

### Testing (High Level)
- Add new tests in `test_core_file.py`:
  - `test_validate_each_file_local_files()` - validates local files exist
  - `test_validate_each_file_s3_files()` - validates S3 files with mocked head_object
  - `test_validate_each_file_mixed()` - mix of local and S3 files
  - `test_validate_each_file_no_existence_checks()` - only format validation
  - `test_validate_each_file_missing_s3_options()` - error when S3 options missing
  - `test_file_exists_local()` - test _file_exists for local files
  - `test_file_exists_s3()` - test _file_exists for S3 files with mocked client
- Ensure all existing tests in `test_reader.py` still pass after refactoring
- Verify that FileRowReader still properly validates files before operations



## Task 5: Implement Parallel Operations ✓

### Summary
Add parallelism to all batch operations in `_core_file.py` using a ThreadPoolExecutor. This will significantly improve performance when working with multiple files.

### Implementation Notes
- Extend S3Options:
  - Add `local_parallelism: int | None` parameter (default: None)
  - Add lazy `_executor` property that creates ThreadPoolExecutor on first use
  - Implement `__del__` to clean up executor (no context manager)
  - Executor is reused across operations for efficiency
- Parallelize operations:
  - `do_each_copy()`: Submit each copy to executor, wait for all. Make sure the fast checks (like unique destinations) are done synchronously beforehand though.
  - `read_each_file_size()`: Submit each size read to executor
  - `read_each_file_contents()`: Submit each content read to executor
  - `validate_each_file()`: Submit each existence check to executor. Make sure the fast checks (like is_s3 and has s3_options) are done synchronously beforehand though.
- Error handling:
  - Collect exceptions from parallel operations
  - Return partial results where appropriate
  - Provide clear error reporting for failed operations
- Thread safety:
  - Ensure S3 client creation is thread-safe
  - Handle concurrent access to shared resources
- Ensure the user may still opt out of this parallelism behavior if they explicitly opt-out of it.


### Testing (High Level)
- Test parallel operations are faster than sequential
- Test with various parallelism levels (1, 4, 16, None)
- Test error handling with partial failures
- Test thread safety with concurrent operations
- Test executor cleanup on S3Options deletion
- Verify no resource leaks with repeated operations


## Implementation Notes

### Testing Strategy
- Use moto library to mock all S3 operations
- Keep all testdata files under 1KB in size; put them in the folder tests/data/
- Focus on integration tests that verify end-to-end workflows
- Test error conditions and edge cases thoroughly

### Code Quality
- Follow established linting rules and security practices
- Ensure all code passes `uv run lint` checks and `uv run test` unit tests
- Add comprehensive docstrings following Google convention
- Use type hints throughout the codebase

### Dependencies
Key dependencies to add as implementation progresses:
- `boto3` for S3 operations
- `pyarrow` for table operations
- `pandas` for DataFrame handling
- `ray` for distributed computing
- `moto` for S3 testing (dev dependency)
