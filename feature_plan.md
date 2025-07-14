# Task 8a: Ensure FileDataFrameReader will return "id" for each row (fixing bug)

## Overview
Fix a bug in FileDataFrameReader where the into_temp_dir() method drops failing rows but doesn't return the ID of successfully processed rows. This makes it impossible to know which rows succeeded and which failed.

## Requirements
1. Since `into_temp_dir()` generator drops failing rows, the `into_temp_dir()` function will need to be modified to return the ID of the row.
2. Do the refactor to yield a row and temp dir for the streaming `into_temp_dir()` implementation.
3. Modify code usage of this API accordingly in the documentation.

## Implementation Plan

### Step 1: Examine current FileDataFrameReader implementation
- Understand how into_temp_dir() currently works
- Identify where row IDs are available but not returned
- See what code currently uses this API

### Step 2: Refactor into_temp_dir() method
- Change the yield to return both row ID and temp directory
- Ensure the ID comes from user-defined 'id' column if present, or falls back to index
- Update any internal logic that processes the yielded values

### Step 3: Update code that uses into_temp_dir() API
- Find all places where into_temp_dir() is called
- Update them to handle the new tuple return format (id, temp_dir)
- Ensure backward compatibility where possible

### Step 4: Update tests
- Modify existing tests to verify ID is returned correctly
- Test the partial failure case to ensure successful row IDs match expected IDs
- Add specific tests for ID handling edge cases

## Testing

### Test Cases
1. **ID return with successful rows** - Verify that IDs are returned for all successful rows
2. **Partial failure ID tracking** - Test that only successful row IDs are returned, failed rows are dropped
3. **User-defined ID column** - Test using custom 'id' column vs falling back to index
4. **Mixed local and S3 sources** - Ensure ID tracking works with different file sources
5. **Empty DataFrame handling** - Test edge cases with no rows or all failed rows
