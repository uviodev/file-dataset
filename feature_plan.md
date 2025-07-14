# Task 9: Implement reader support for PyArrow size table

**Status**: Not started
**Description**: Add functionality to create PyArrow tables containing file size metadata without downloading the actual files.

**Requirements**:
- Implement `.into_size_table(head: int|None = None)` method on FileDataFrameReader; it returns a PyArrow table.
    - For local files: use file system stat operations
    - For S3 files: use S3 HEAD operations to get object metadata on every row. Concatenate the outputs. Important: do not download the files in this case! Drop error rows.
    - If the user passes head=N then only the first N rows is converted into the table; default is None so return the whole table
- Return PyArrow table with schema: `{id: pa.string(), filename: pa.int64()}` (where id is the id, and filename gives the size in bytes for each file column)
- Handle errors for inaccessible files; drop error rows but add log statements with the issues.
- Add `into_size_table(head: int|None)` for the single-row Reader but have it raise a NotImplementedError

**Testing**:
- Test size table creation for local files
- Test size table creation for S3 files using moto
- Test error handling for missing files
- Test PyArrow table schema and data types

## Implementation Plan

### Understanding the codebase
Need to examine the current reader implementation to understand:
1. How FileDataFrameReader is currently structured
2. How the single-row Reader works
3. Current error handling patterns
4. PyArrow usage patterns in the codebase

### High-level implementation steps
1. Add PyArrow dependency if not already present
2. Implement `into_size_table()` method on FileDataFrameReader that:
   - Iterates through each row in the DataFrame
   - For each file in the row, gets size without downloading:
     - Local files: use `os.path.getsize()` or `pathlib.Path.stat()`
     - S3 files: use `s3.head_object()` to get ContentLength
   - Collects results into PyArrow table with correct schema
   - Handles errors gracefully by dropping problematic rows
3. Add stub method to single-row Reader that raises NotImplementedError
4. Handle the `head` parameter to limit rows processed

### Testing strategy
- Create test files of known sizes in tests/data/
- Use moto to mock S3 HEAD operations
- Test error conditions (missing files, permission errors)
- Verify PyArrow table schema matches specification
- Test head parameter functionality

### Questions and Answers
1. **Should the PyArrow table include all file columns from the DataFrame or only those that exist?**
   - Include all file columns from the DataFrame (excluding 'id'). This matches the existing pattern in the codebase where operations handle all columns.

2. **How should we handle optional files (None values in DataFrame)?**
   - Skip None values like the existing write operations do. For file sizes, this means the column won't appear in the result table for that row.

3. **What should be the exact logging format for errors?**
   - Follow existing patterns: Use `logger.warning()` with row identifier, error message, and file errors details matching the format in `into_temp_dir()`.

### Implementation Details
1. **Add PyArrow dependency** to pyproject.toml
2. **FileDataFrameReader.into_size_table()** implementation:
   - Iterate through DataFrame rows (respecting `head` parameter)
   - For each row, collect file sizes for non-None files
   - Use `os.path.getsize()` for local files
   - Use `s3.head_object()['ContentLength']` for S3 files
   - Build PyArrow table with `{id: pa.string(), filename: pa.int64()}` schema
   - Drop failed rows and log errors
3. **Reader.into_size_table()** stub raises NotImplementedError
