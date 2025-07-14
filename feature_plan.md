# Task 6: Implement reader support for DataFrames with partial failure handling

**Status**: In progress

**Description**: Extend reader functionality to process pandas DataFrames with graceful handling of row-level failures.

**Requirements**:
- Implement FileDataFrameReader. In its constructor it takes in a pandas DataFrame, which is saved as an instance variable.
- Implement `into_temp_dir()` which yields a stream of temporary directories. Each DataFrame row represents a set of files to be processed together. It opens a Reader for each row then delegates to `into_temp_dir()` to the reader for each row. If the reader fails to read the row then the row is skipped and an error message is logged.
    - Log which rows failed and why (for debugging). Use the user-defined `id` column if present or else fall back to the index column.
- Modify `core.reader()` function to add a new kwarg, `dataframe`. This kwarg should be mutually exclusive with `row` (otherwise ValueError is raised). Exactly one should be specified. When `dataframe` arg is specified then  FileDataFrameReader is returned.

**Testing**:
- Test `reader(dataframe=all_successful_rows, options=defaults)` with all successful rows and confirm each file was downloaded to a temp dir.
- Test `reader(dataframe=partial_failure, options=defaults)` with some failing rows (missing files, permission errors)
- Test mixed local and S3 sources with partial failures
- Verify failed rows are properly dropped from results
- Test empty DataFrame and DataFrame with all failed rows; when all rows fail then the reader should fail with a message to check error logs.

## Planning

### High-level implementation plan:

1. **Create FileDataFrameReader class**:
   - Constructor takes a pandas DataFrame and optional Options instance
   - Store DataFrame as instance variable
   - Implement `into_temp_dir()` method that yields temp directories

2. **FileDataFrameReader.into_temp_dir() implementation**:
   - Iterate through DataFrame rows
   - For each row, convert to dict (excluding 'id' column if present)
   - Create a Reader instance with the row dict
   - Use Reader's into_temp_dir() in a try-except block
   - If successful, yield the temp directory
   - If fails, log error with row identifier (id column or index) and continue
   - Track total rows vs successful rows
   - If all rows fail, raise an exception with message to check error logs

3. **Update core.reader() function**:
   - Add `dataframe` kwarg (keyword-only, optional)
   - Check mutual exclusivity: exactly one of `row` or `dataframe` must be provided
   - If `dataframe` is provided, return FileDataFrameReader instance
   - If `row` is provided, return Reader instance (current behavior)

4. **Logging strategy**:
   - Use Python's logging module
   - Log at WARNING level for row failures
   - Include row identifier (id or index) in log messages
   - Include the specific error message from FileDatasetError

5. **Error handling**:
   - Gracefully handle FileDatasetError from Reader.into_temp_dir()
   - Continue processing other rows on failure
   - Track failures and report summary at the end
   - Raise exception only if ALL rows fail

### Key design considerations:
- FileDataFrameReader should accept same `options` parameter as Reader
- The yielded temp directories should be handled as context managers
- Each row should get its own isolated temporary directory
- Maintain backward compatibility with existing reader() function
- Use clear logging messages that help with debugging

### Testing plan:
1. Test with DataFrame where all rows have valid files
2. Test with DataFrame containing some rows with missing files
3. Test mixed local and S3 sources with some failures
4. Test empty DataFrame (should work but yield nothing)
5. Test DataFrame where all rows fail (should raise exception)
6. Test that row identifiers (id column vs index) are logged correctly
7. Test that Options are properly passed through to Reader instances
