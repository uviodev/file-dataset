# Task 4: Implement reader support for S3 data

**Status**: In progress

**Description**: Extend reader functionality to download files from S3 to temporary directories.

**Requirements**:
- Modify `file_dataset.reader()` to accept S3 URLs (s3://bucket/key format) in the files dict values
- Add `options` parameter to reader; the options allows us to use an s3 client for downloading files.
- Implement S3 download logic in `.into_temp_dir()`; ensure s3_client uses s3transfer methods (s3_client.download_file; do not use get_object directly)
- Maintain same interface as local file reader
- Handle S3 errors (permissions, missing files, network issues)

**Testing**:
- Test S3 file downloads using moto library for mocking
- Test mixed local and S3 file sources
- Test error handling for missing S3 objects; raise standard FileDatasetError for the files that can't be accessed
- Test with different S3 credentials configurations

## Planning

### High-level implementation plan:

1. Review the existing reader implementation to understand current structure
2. Modify the reader class to:
   - Accept an optional `options` parameter (default to None, keyword-only)
   - Detect S3 URLs in the files dict (s3:// prefix)
   - Validate S3 URL format before processing
   - For S3 URLs, require options parameter (raise error if None)
   - Maintain support for local files (existing behavior)

3. Update `into_temp_dir()` method to:
   - First validate all files (local existence check, S3 HEAD request)
   - Create temp directory as before
   - For each file, check if it's local or S3
   - For S3 files: use s3_client.download_file() method
   - Handle errors and raise FileDatasetError for missing files

4. Key design considerations:
   - S3 URLs format: s3://bucket/key
   - Raise error if S3 URLs present but no options provided
   - Ensure proper error handling for S3 errors (403, 404, network issues)
   - Maintain backward compatibility with local files
   - Validate S3 URLs with regex before attempting operations

### Questions:
1. Should we allow mixed local and S3 files in the same reader instance?
   - Answer: Yes, based on the testing requirements mentioning "Test mixed local and S3 file sources"

2. How should we handle S3 credentials when options is None?
   - Answer: Use Options.default() to get default boto3 session credentials

3. Should download errors be aggregated or fail fast?
   - Answer: Based on Task 1 requirements, raise FileDatasetError with mapping of filename to error message

4. Should the `options` parameter be keyword-only or positional?
   - Answer: Keyword-only and optional, consistent with the `row` parameter

5. Should we validate S3 URL format before attempting download?
   - Answer: Yes, validate S3 URLs before attempting operations

6. Should we use transfer configuration from Options?
   - Answer: Use defaults from s3_client, don't access private variables

7. Error aggregation behavior for multiple files?
   - Answer: Validate all S3 URLs exist first (using HEAD requests), consistent with local file behavior

8. Options.default() behavior when options=None?
   - Answer: Raise an error if no options are passed but user passes S3 URLs; don't create defaults

9. Backward compatibility for reader() function?
   - Answer: Yes, maintain compatibility by making options optional with default of None

### Testing plan:
1. Test S3 file download with moto mocking
2. Test mixed local and S3 sources in same reader
3. Test error handling for missing S3 objects (404)
4. Test error handling for permission denied (403)
5. Test with explicit options vs default options
6. Ensure FileDatasetError contains proper error mapping
