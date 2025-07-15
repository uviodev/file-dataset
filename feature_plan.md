# Task 6: Split Reader API for Better Separation of Concerns

**Status:** Not Started
**Priority:** High

**Summary:** Split the overloaded reader() function into row_reader() and file_dataframe_reader() with distinct interfaces.

**Description:**
The current reader() API tries to handle both single row and DataFrame scenarios with kwargs, making it confusing and error-prone. Split into two dedicated functions:
- `row_reader(row, options=None)` returns FileRowReader with `into_temp_dir()` context manager
- `file_dataframe_reader(dataframe, options=None)` returns FileDataFrameReader with `into_temp_dirs()` generator

Remove kwargs for row= and dataframe= from reader(). Make FileRowReader and FileDataFrameReader have distinct, focused interfaces. Drop methods on FileRowReader that raise NotImplementedError and simply don't implement them.

## Implementation Plan

Based on analyzing the current codebase, I need to split the overloaded `reader()` function in `_reader.py` into two dedicated functions with distinct interfaces.

**Current API Analysis:**

The current `reader()` function (lines 573-604 in `_reader.py`) uses kwargs with mutual exclusivity:
```python
def reader(*, row=None, dataframe=None, options=None) -> FileRowReader | FileDataFrameReader
```

**Problems with Current Design:**
1. **Interface Confusion**: `FileRowReader.into_temp_dir()` is a context manager, but `FileDataFrameReader.into_temp_dir()` is a generator
2. **NotImplementedError Methods**: `FileRowReader.into_size_table()` and `FileRowReader.into_blob_table()` raise NotImplementedError
3. **Confusing API**: Users must remember which kwargs to use for which scenario

**New API Design:**

1. **`row_reader(row, options=None)`** → Returns `FileRowReader`
   - Clean interface: only `into_temp_dir()` context manager
   - Remove `into_size_table()` and `into_blob_table()` methods entirely

2. **`file_dataframe_reader(dataframe, options=None)`** → Returns `FileDataFrameReader`
   - Rename `into_temp_dir()` to `into_temp_dirs()` to clarify it's a generator
   - Keep `into_size_table()` and `into_blob_table()` methods

**Code Changes Required:**

1. **Create new functions in `_reader.py`:**
   - Add `row_reader(row, options=None)` function
   - Add `file_dataframe_reader(dataframe, options=None)` function
   - Remove the old `reader()` function

2. **Modify `FileRowReader` class:**
   - Remove `into_size_table()` method (lines 156-167)
   - Remove `into_blob_table()` method (lines 169-180)

3. **Modify `FileDataFrameReader` class:**
   - Rename `into_temp_dir()` method to `into_temp_dirs()` for clarity

4. **Update `__init__.py`:**
   - Export `row_reader` and `file_dataframe_reader` instead of `reader`

5. **Update all internal imports:**
   - `pipeline.py` - switch to specific reader functions
   - `ray/_datasource.py` - use `file_dataframe_reader`

6. **Update all tests:**
   - `test_reader.py` - use `row_reader()`
   - `test_reader_s3.py` - use `row_reader()`
   - `test_dataframe_reader.py` - use `file_dataframe_reader()` with `into_temp_dirs()`
   - `test_size_table.py` & `test_blob_table.py` - use `file_dataframe_reader()`
   - Remove tests that check NotImplementedError on single-row readers

**Testing:**
- Update existing tests named test_reader_s3.py and test_reader.py to use row_reader()
- Update DataFrame tests to use file_dataframe_reader() and into_temp_dirs()
- Test row_reader() works with single row dictionaries
- Test file_dataframe_reader() works with DataFrames
- Test FileRowReader.into_temp_dir() context manager behavior
- Test FileDataFrameReader.into_temp_dirs() generator behavior
- Test old reader() function is removed properly
- Test FileRowReader doesn't have DataFrame-specific methods
- Test clean separation between single-row and DataFrame workflows
