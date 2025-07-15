# Task 1: Create Core File Operations Library

## Summary
Create a new library `_core_file.py` that centralizes all file operations (copy, read) with support for both local and S3 paths. This will provide a unified interface for file operations across the codebase.

## Implementation Plan

### 1. Understand Current Codebase
- Examine existing writer module to understand `write_files()` implementation
- Examine existing reader module to understand `into_temp_dir()` implementation
- Understand S3Options structure and usage
- Identify all file operation patterns currently used

### 2. Create _core_file.py Module
- Implement `_do_single_copy(src: str | Path, dst: str | Path, s3_options: S3Options)`:
  - Parse source and destination to determine if they are S3 or local paths
  - Handle all four combinations: local→local, local→S3, S3→local, S3→S3
  - Use shutil.copy2() for local operations
  - Use boto3 S3 client for S3 operations

- Implement `do_copy(copies: list[tuple[str | Path, str | Path]], s3_options: S3Options)`:
  - Validate all source paths exist
  - Validate all destination paths are unique
  - Check that s3_options is not None when S3 paths are involved
  - Execute copies sequentially using `_do_single_copy`
  - Provide clear error messages for validation failures

### 3. Refactor Existing Code
- Update writer module's `write_files()` to use `do_copy()`
- Update reader module's `into_temp_dir()` to use `do_copy()`
- Ensure backwards compatibility is maintained

## Testing

### Test Cases
1. **Local to Local Copy**
   - Create test file
   - Copy to new location
   - Verify content matches

2. **Local to S3 Copy**
   - Create local test file
   - Copy to mocked S3 bucket
   - Verify file exists in S3 with correct content

3. **S3 to Local Copy**
   - Put test file in mocked S3 bucket
   - Copy to local temp directory
   - Verify content matches

4. **S3 to S3 Copy**
   - Put test file in mocked S3 bucket
   - Copy to different S3 location
   - Verify copy exists with correct content

5. **Validation Tests**
   - Non-existent source file → should raise error
   - Duplicate destinations → should raise error
   - S3 path with None s3_options → should raise error
   - Empty copies list → should succeed

6. **Integration Tests**
   - Test refactored writer still works correctly
   - Test refactored reader still works correctly
   - Test with multiple files in batch operations

## Questions/Clarifications Needed
None - the task is clear and ready to implement.
