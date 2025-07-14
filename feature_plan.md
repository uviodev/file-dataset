# Task 7: Implement write_files support for DataFrames with partial failure handling

## Overview
Extend `write_files()` functionality to process multiple rows from DataFrames with graceful partial failure handling.

## Requirements
1. Modify `file_dataset.write_files()` to accept a `dataframe` kwarg, in addition to the `row` kwarg
2. The DataFrame is a new kwarg mutually exclusive with row kwarg
3. Refactor existing logic to `_write_row_files()`; when `row` is specified, just call that.
4. Otherwise for dataframe input, call `_write_row_files()` for each row independently in sequence. If one row fails, continue with others
5. Return results only for successfully written rows in a DataFrame. Provide an `id` column to match data.
6. Ensure the row ID is logged for failed rows.
7. Maintain same output path structure: each row gets its own ID-based directory structure

## Implementation Plan

### Step 1: Refactor existing write_files logic
- Extract the core logic from `write_files()` into a new `_write_row()` function
- This function will handle writing a single row of files
- Keep the same validation and error handling logic

### Step 2: Modify write_files() signature
- Add `dataframe` as a new kwarg parameter
- Make `row` and `dataframe` mutually exclusive (similar to reader())
- Add proper validation for mutual exclusivity
- When a dataframe is passed in, the output signature is a dataframe

### Step 3: Implement DataFrame processing logic
- When `dataframe` is provided, iterate through each row
- For each row, call `_write_row()` with appropriate parameters
- Handle errors gracefully - log failures and continue with next row
- Collect successful results maintaining row correspondence

### Step 4: Handle S3 support
- Ensure S3 upload functionality works with DataFrame input
- Pass options parameter through to support S3 credentials
- Maintain proper path structure for S3 uploads

### Step 5: Error handling and logging
- Log which rows failed and why (use 'id' column from DataFrame)
- If all rows fail, raise FileDatasetError with appropriate message
- Return a DataFrame that has an id column and all the successful files.

## Testing

### Test Cases
1. **All successful rows** - Test writing DataFrame where all rows succeed
2. **Partial failures** - Test with some rows having missing files or permission errors
3. **Mixed destinations** - Test with both local and S3 destinations
4. **Row isolation** - Verify one bad row doesn't affect others
5. **Empty/all-failed** - Test empty DataFrame and all rows failing scenarios
6. **S3 uploads** - Test S3 upload functionality with mocked S3 using moto
7. **Return value format** - Verify correct mapping structure is returned
