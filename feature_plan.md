# Task 4: Ensure all s3_client operations are performed in `_core_file.py`

## Summary
Move `validate_files` from the RowReader to `_core_file.py` with functions. This centralizes all S3 client operations in one module for better maintainability and consistency.

## Implementation Notes

### 1. Create new validation functions in `_core_file.py`:
- `validate_each_file(files: Mapping[str, str | Path], s3_options: S3Options | None = None, do_existence_checks: bool = True) -> dict[str, str]`
  - Main entry point that validates multiple files
  - Returns dict of errors (empty if all valid)
  - If `do_existence_checks=False`, only does basic validation (URL format, etc.)
  - If there are any S3 files but there is no s3_options provided, raise an error (do not use S3Options.default()).

- `_validate_s3_path_format(path: str | Path, s3_options: S3Options | None) -> str | None`
  - Only validates S3 paths - returns None immediately for local paths
  - Validates S3 URL format and that s3_options is provided when needed
  - Returns error message for invalid S3 paths or None if valid

- `_file_exists(path: str | Path, s3_options: S3Options | None) -> bool`
  - For S3 files: uses `s3_options.s3_client.head_object()` to check existence
  - For local files: uses `Path.exists()` and `Path.is_file()`
  - Returns True if file exists and is accessible

### 2. Refactor FileRowReader in `_reader.py`:
- Remove `_validate_files()` and `_validate_s3_file()` methods
- Replace with call to `validate_each_file()` from `_core_file.py`
- Update `into_temp_dir()` to use the new validation function
- Remove check for has_s3_files (it is moved into `validate_each_file()` already).

### 3. Migration pattern:
```python
# Old code in _reader.py:
file_errors = self._validate_files()

# New code:
from file_dataset._core_file import validate_each_file
file_errors = validate_each_file(self.files_dict, self.options)
```

## Testing (High Level)
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

## Implementation Plan

### Step 1: Analyze current validation logic
- Look at `_validate_files()` in FileRowReader
- Understand the validation flow and error handling
- Note special cases like S3 options check

### Step 2: Implement functions using TDD
1. Write test for `_file_exists` with local file
2. Implement `_file_exists` for local files
3. Write test for `_file_exists` with S3 file
4. Implement `_file_exists` for S3 files
5. Write test for `_validate_s3_path_format`
6. Implement `_validate_s3_path_format`
7. Write test for `validate_each_file`
8. Implement `validate_each_file`

### Step 3: Refactor FileRowReader
- Remove old validation methods
- Update to use new functions
- Ensure error handling remains the same

### Step 4: Verify everything works
- Run all tests
- Check linting
- Update any documentation if needed
