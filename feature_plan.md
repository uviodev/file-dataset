# Task 2: Rename Reader Class

**Status:** Not Started
**Priority:** Medium
**Dependencies:** Task 1 (S3 utilities extraction)

**Summary:** Rename `core.Reader` class to `core.FileRowReader` for clarity.

**Description:**
The class `core.Reader` should be renamed to `core.FileRowReader` to better reflect its purpose of reading individual file rows. This change requires updating all references in tests and documentation to maintain consistency.

**High-Level Test Descriptions:**
- Test that `FileRowReader` class maintains all functionality of the original `Reader` class
- Test import statements and public API access to ensure no breaking changes
- Test class instantiation and method calls with the new name
- Regression tests to verify all existing Reader functionality works with new class name

## Implementation Plan

Based on analyzing the codebase:

1. `core.Reader` class (line 23 in core.py) needs to be renamed to `FileRowReader`
2. All references to `Reader` in core.py need to be updated to `FileRowReader`
3. The `reader()` function should still return `FileRowReader` but the public API should remain the same
4. Tests that instantiate or reference `Reader` directly need to be updated
5. The import in `__init__.py` may need updating to maintain public API

**Code Changes:**
1. Rename the `Reader` class to `FileRowReader` in `src/file_dataset/core.py`
2. Update all internal references within core.py
3. Update tests that directly import or use the `Reader` class
4. Verify public API through `__init__.py` remains unchanged
5. Add tests to verify the new class name works correctly

**Testing:**
- Test that all existing Reader functionality works with FileRowReader
- Test that the reader() function returns FileRowReader instances
- Test import paths and public API access
- Regression tests to ensure no functionality is broken
