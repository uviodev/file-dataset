## Task 10: Implement reader support for PyArrow blob table

**Description**: Add functionality to load file contents directly into PyArrow tables as binary data.

**Requirements**:
- Implement `.into_blob_table(head: int|None)` method on Reader and FileDataFrameReader.
    - If the user passes head=N then only the first N rows is converted into the table; default is None so return the whole table
    - This method is provided for debugging
- Load file contents into memory as binary data.
- Return PyArrow table with schema: `{id: pa.string(), filename: pa.binary()}` for each file column
- Support both local and S3 files
- Add `into_blob_table(head: int|None)` for the single-row Reader but have it raise a NotImplementedError

## Code changes needed:

1. **Modify `src/file_dataset/core.py`**:
   - Add `into_blob_table(head: int|None = None)` method to the `Reader` class that raises NotImplementedError
   - Add `into_blob_table(head: int|None = None)` method to the `FileDataFrameReader` class

2. **Implementation details**:
   - The method should iterate through the DataFrame rows (up to `head` limit if specified)
   - For each row, download/read the files to get their binary content
   - Create PyArrow table with `id: pa.string()` and one column per file with `pa.binary()` type
   - Handle both local and S3 files by using the existing `into_temp_dir()` functionality
   - Drop failed rows and log errors (similar to `into_size_table()`)

3. **Schema structure**:
   - `id` column: string containing the row ID
   - File columns: each file column from the DataFrame becomes a binary column in the table

## Testing:
- Test blob table creation with small files (< 1KB as per guidelines)
- Test mixed local and S3 sources
- Test PyArrow table schema and binary data integrity
- Test memory usage with multiple files
- Test head parameter functionality
- Test error handling for missing files
- Test NotImplementedError for single-row Reader
