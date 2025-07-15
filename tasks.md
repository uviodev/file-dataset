# File Dataset Implementation Tasks

A proof of concept library has been implemented. Now, some refactoring needs to be done to take it to the next level and clean up the codebase.

## Task 1: Extract S3 Utilities Module
**Status:** Done
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

## Task 2: Rename Reader Class
**Status:** Done
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

## Task 3: Improve Options Default Behavior
**Status:** Done
**Priority:** Medium
**Dependencies:** Task 2 (Reader class rename)

**Summary:** Change None options behavior to use default Options instead of raising errors.

**Description:**
Modify the behavior when an `options` value of None is passed. Instead of raising an error, the options should be automatically initialized to `Options.default()`. This provides better user experience by offering sensible defaults.

**High-Level Test Descriptions:**
- Test that passing `options=None` initializes `Options.default()` without errors
- Test that the default options provide expected S3 client configuration
- Test that explicit options still override defaults properly
- Test edge cases where Options.default() might fail and ensure graceful handling

## Task 4: Split Core Module
**Status:** Done
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

## Task 5: Consolidate File DataFrame Concept
**Status:** Done
**Priority:** Medium
**Dependencies:** Task 4 (Core module split)

**Summary:** Create dedicated file_dataframe.py module to consolidate DataFrame validation logic there.

**Description:**
Consolidate the concept of a `file_dataframe` into a new file, `file_dataframe.py`. Move the logic for getting the file_dataframe's columns there. Replace scattered validation logic in `pipeline.py` with centralized validation functions. Ray code should remain in separate ray modules though it can now import the validation logic from file_dataframe.py.

**High-Level Test Descriptions:**
- Test file_dataframe column detection and validation logic
- Test that DataFrame validation catches invalid column names, missing columns, etc.
- Test integration with pipeline.py to ensure validation works in processing workflows
- Test edge cases: empty DataFrames, DataFrames with unusual column types
- Regression tests to ensure existing pipeline functionality remains intact


## Task 6: Split Reader API for Better Separation of Concerns
**Status:** Done
**Priority:** High

**Summary:** Split the overloaded reader() function into row_reader() and file_dataframe_reader() with distinct interfaces.

**Description:**
The current reader() API tries to handle both single row and DataFrame scenarios with kwargs, making it confusing and error-prone. Split into two dedicated functions:
- `row_reader(row, options=None)` returns FileRowReader with `into_temp_dir()` context manager
- `file_dataframe_reader(dataframe, options=None)` returns FileDataFrameReader with `into_temp_dirs()` generator

Remove kwargs for row= and dataframe= from reader(). Make FileRowReader and FileDataFrameReader have distinct, focused interfaces. Drop methods on FileRowReader that raise NotImplementedError and simply don't implement them.

**High-Level Test Descriptions:**
- Update existing tests named test_reader_s3.py and test_reader.py to see if any of them can be updated.
- Test row_reader() works with single row dictionaries
- Test file_dataframe_reader() works with DataFrames
- Test FileRowReader.into_temp_dir() context manager behavior
- Test FileDataFrameReader.into_temp_dirs() generator behavior
- Test old reader() function is removed/deprecated properly
- Test FileRowReader doesn't have DataFrame-specific methods
- Test clean separation between single-row and DataFrame workflows
- Test backward compatibility migration path for existing code


## Task 7: Simplify write_files API with Type-Based Detection
**Status:** Done
**Priority:** High
**Dependencies:** (Split Reader API)

**Summary:** Simplify write_files() API by automatically detecting single row vs DataFrame based on first argument type.

**Description:**
Currently write_files() has confusing kwargs for row= and dataframe= scenarios. Simplify the API to have a single function that automatically detects the operation type based on the first argument:
- `write_files(data: DataFrame | Mapping[str, str | Path | None], *, into_path, id=None, options=None)`
- If `data` is a dict/mapping → single row operation (id required)
- If `data` is a DataFrame → file dataframe operation (id not allowed, uses DataFrame 'id' column)

Remove the separate kwargs and make the API more intuitive by using type-based dispatch. This creates a cleaner interface while maintaining all existing functionality.

**High-Level Test Descriptions:**
- Update existing tests named test_writer.py to see if they can be refactored to test the new API.
- Test write_files() with dict input works as single row operation
- Test write_files() with DataFrame input works as file dataframe operation
- Test id parameter is required for dict input and forbidden for DataFrame input
- Test proper error messages when id parameter is used incorrectly
- Test type hints properly reflect the union type for the data parameter
- Test backward compatibility with existing write_files usage patterns
- Test integration with Pipeline class still works correctly
- Test both local and S3 destinations work with the unified API


## Task 8: Add Parallel File Operations Support
**Status:** Not Started
**Priority:** Low

**Summary:** Implement optional parallel processing for file downloads and uploads.

**Description:**
Add parallel processing capabilities for file operations using ThreadPoolExecutor with configurable worker count. This should be optional and configurable to balance performance with resource usage.

**High-Level Test Descriptions:**
- Test parallel downloads complete faster than sequential
- Test parallel uploads maintain data integrity
- Test worker count limits are respected
- Test graceful fallback to sequential processing on errors
- Test resource cleanup when parallel operations complete



## Task 9: Improve Memory Management for Large Files
**Status:** Not Started
**Priority:** High
**Dependencies:** Task 7 (Progress reporting)

**Summary:** Implement streaming and memory budget controls for processing large files efficiently.

**Description:**
Currently large files are loaded entirely into memory which can cause OOM errors. Implement chunked/streaming reading for large files and add memory budget parameters to control concurrent file loading. This is critical for production workloads with large image/blob files.

**High-Level Test Descriptions:**
- Test memory usage stays within configured limits for large files
- Test streaming file operations work correctly
- Test concurrent file loading respects memory budget
- Test performance is maintained with memory management enabled
- Test graceful degradation when memory limits are hit


## Implementation Notes

### Testing Strategy
- Use moto library to mock all S3 operations
- Keep all testdata files under 1KB in size; put them in the folder tests/data/
- Focus on integration tests that verify end-to-end workflows
- Test error conditions and edge cases thoroughly

### Code Quality
- Follow established linting rules and security practices
- Ensure all code passes `uv run lint` checks
- Add comprehensive docstrings following Google convention
- Use type hints throughout the codebase

### Dependencies
Key dependencies to add as implementation progresses:
- `boto3` for S3 operations
- `pyarrow` for table operations
- `pandas` for DataFrame handling
- `ray` for distributed computing
- `moto` for S3 testing (dev dependency)
