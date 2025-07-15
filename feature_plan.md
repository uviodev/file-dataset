# Task 3: Migrate to S3Transfer API

## Summary
Replace low-level boto3 S3 API calls with S3Transfer methods for better performance and configuration options. S3Transfer provides built-in retry logic, multipart uploads, and bandwidth throttling.

## Implementation Notes
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

## Testing (High Level)
- Test file uploads trigger multipart behavior (pytest-mock is OK)
- Test transfer config options are respected
- Test retry behavior on transient failures
- Verify backwards compatibility is maintained

## Implementation Plan

### 1. Analysis of Current Implementation
Looking at `_core_file.py`:
- Currently uses `s3_client.download_file()` and `s3_client.upload_file()` for file operations
- Uses `s3_client.copy()` for S3-to-S3 transfers
- Uses `s3_client.get_object()` for reading file contents
- S3Options already has `_s3_transfer_config` attribute

The code already uses S3Transfer methods (`download_file`, `upload_file`) for file operations! However:
- For content reading, it uses `get_object()` which could be replaced with `download_fileobj()`
- The `transfer_config` is not being passed to the transfer operations

### 2. Changes Required

#### In `_copy_single_file()`:
- Pass `Config` parameter to `download_file()` and `upload_file()` calls
- Keep using `copy()` for S3-to-S3 (S3Transfer doesn't have a better alternative)

#### In `_read_file_contents()`:
- Replace `get_object()` with `download_fileobj()` using BytesIO
- Pass transfer config to the operation

### 3. Test Cases
1. Test that transfer config is passed correctly
2. Test multipart upload behavior with large files
3. Test error handling remains consistent
4. Test backwards compatibility
