# Task 1: Extract S3 Utilities Module
**Status:** Not Started
**Priority:** High
**Dependencies:** None

**Summary:** Extract S3-related helper functions into a dedicated module for better code organization.

**Description:**
Move s3-related helper functions like `parse_url` from their current locations into a separate `s3_utils.py` file. This will improve code organization by centralizing S3-specific utilities and making them easier to test and maintain independently.

**High-Level Test Descriptions:**
- Test `parse_url` function with various S3 URL formats (valid and invalid)
- Test S3 URL validation edge cases (empty strings, malformed URLs, non-S3 URLs)
- Integration tests to ensure existing functionality still works after the move
- Test import paths to ensure S3 utilities are accessible from expected locations

## Implementation Plan

Based on analyzing the codebase, I found two S3-related functions in `src/file_dataset/core.py`:

1. `_parse_s3_url(url: str)` - line 20-32: Parses S3 URL into bucket and key
2. `_is_s3_url(path: str | Path)` - line 35-44: Checks if path is an S3 URL

**Code Changes:**
1. Create `src/file_dataset/s3_utils.py` with these functions (removing leading underscore to make them public)
2. Update `src/file_dataset/core.py` to import from s3_utils
3. Update any tests that might directly import these functions
4. Add comprehensive tests for the S3 utilities

**Testing:**
- Test `parse_s3_url` with valid URLs: `s3://bucket/key`, `s3://bucket/`, `s3://bucket`
- Test with invalid URLs: `http://example.com`, `s3:/bucket`, empty strings
- Test `is_s3_url` with various path types including Path objects
- Integration tests to ensure all existing functionality continues to work
