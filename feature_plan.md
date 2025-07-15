# Task 2: Add File Size and Content Reading APIs

## Summary
Extend `_core_file.py` with functions to read file sizes and contents efficiently for both local and S3 files. These will support batch operations on multiple files.

## Implementation Notes
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

## Testing (High Level)
- Test reading sizes/contents from local and S3 files
- Test batch operations with mixed local/S3 paths
- Test error cases: missing files, access denied
- Verify memory efficiency with large files
- Test that refactored table functions maintain compatibility

## Implementation Plan

### 1. Analyze Current Implementation
- Look at how `into_size_table()` currently reads file sizes
- Look at how `into_blob_table()` currently reads file contents
- Understand the error handling patterns they use

### 2. Implement Low-Level Functions
- `_read_file_size()`: Single file size reading
- `_read_file_contents()`: Single file content reading
- Both should handle local and S3 paths

### 3. Implement High-Level Batch Functions
- `read_file_sizes()`: Batch size reading with error collection
- `read_file_contents()`: Batch content reading with error collection
- Use similar error handling pattern as `do_copy()` with FileDatasetError

### 4. Refactor Existing Code
- Update `into_size_table()` in the reader module
- Update `into_blob_table()` in the reader module
- Ensure backwards compatibility

### 5. Testing Strategy
Following red-green refactor:
1. Write test for reading local file size → implement
2. Write test for reading S3 file size → implement
3. Write test for batch size reading → implement
4. Write test for reading local file content → implement
5. Write test for reading S3 file content → implement
6. Write test for batch content reading → implement
7. Write tests for error cases → implement error handling
8. Test refactored table functions maintain compatibility
