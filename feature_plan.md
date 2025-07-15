# Task 4: Split Core Module

**Status:** Not Started
**Priority:** High
**Dependencies:** Task 3 (Options default behavior)

**Summary:** Split core.py into separate reader and writer modules for better separation of concerns.

**Description:**
Split `core.py` into two files: `_reader.py` (containing `reader()` function and related classes) and `_writer.py` (containing the writing functions). The Pipeline class is already in its own file (pipeline.py). Update `__init__.py` to maintain the same public API so users see no change. Update all related tests to reflect the new structure.

**High-Level Test Descriptions:**
- Test that public API remains unchanged after the split
- Test import statements in `__init__.py` correctly expose reader and writer functionality
- Test that `_reader.py` contains all reader-related classes and functions
- Test that `_writer.py` contains all writer-related functionality
- Integration tests to ensure reader and writer work together properly
- Test that internal imports between `_reader.py` and `_writer.py` work correctly

## Implementation Plan

Based on analyzing the current core.py file, I need to:

**Code to move to `_reader.py`:**
- `FileRowReader` class (lines 20-181)
- `reader()` function (lines 183-214)
- `FileDataFrameReader` class (lines 506-893)

**Code to move to `_writer.py`:**
- `_validate_source_files()` function (lines 217-238)
- `_copy_files_to_destination()` function (lines 241-270)
- `_upload_files_to_s3()` function (lines 273-314)
- `_write_row_files()` function (lines 317-372)
- `_write_dataframe_files()` function (lines 375-447)
- `write_files()` function (lines 450-503)

**Shared imports/dependencies:**
Both modules will need:
- Standard library imports (logging, shutil, tempfile, etc.)
- pandas, pyarrow
- Local imports (exceptions, options, s3_utils)

**Steps:**
1. Create `_reader.py` with reader functionality
2. Create `_writer.py` with writer functionality
3. Update `__init__.py` to import from both modules
4. Remove `core.py`
5. Update tests to ensure no breaking changes

**Testing:**
- Run all existing tests to ensure no regressions
- Test that imports work correctly from both internal modules
- Verify public API remains exactly the same
