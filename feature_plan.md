# Task 5: Implement writer support for S3 uploads

## Overview
Extend writer functionality to upload files to S3 with proper path organization, supporting both row and dataframe modes.

## Requirements
1. Modify `file_dataset.write_files()` to detect S3 paths (s3://bucket/prefix format)
2. Implement S3 upload logic with path structure: `s3://bucket/prefix/id/filename`
3. Add `options` parameter for S3 credentials
4. Return S3 URLs in the result mapping
5. Handle S3 upload errors gracefully
6. Ensure use of s3_client.upload_file to ensure multipart uploads

## Implementation Plan

### Step 1: Add options parameter to write_files signature
- Add `options` parameter to `write_files()` function
- Pass options through to internal functions that need it

### Step 2: Modify _copy_files_to_destination to support S3
- Detect if destination is S3 URL using existing `_is_s3_url()` helper
- If S3, upload file instead of copying
- Return S3 URL in result mapping

### Step 3: Create S3 upload logic
- Use s3_client.upload_file for multipart upload support
- Handle S3-specific errors gracefully
- Return proper S3 URLs in result

### Step 4: Update _write_row_files to support S3
- Pass options parameter through
- Modify to return S3 URLs when uploading to S3

### Step 5: Update _write_dataframe_files to support S3
- Pass options parameter through
- Ensure S3 URLs are returned in DataFrame results

## Testing

### Test Cases
1. **Single file S3 upload** - Test uploading a single file to S3
2. **Multiple files S3 upload** - Test uploading multiple files to S3
3. **Mixed local and S3 destinations** - Test that we can write to both local and S3
4. **S3 error handling** - Test handling of S3 permission errors, etc.
5. **DataFrame S3 uploads** - Test DataFrame mode with S3 destinations
6. **Partial failures with S3** - Test some rows succeeding and others failing
7. **S3 URL format validation** - Test invalid S3 URLs are rejected
8. **Options requirement** - Test that options are required for S3 uploads
