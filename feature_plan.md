# Task 3: Improve Options Default Behavior

**Status:** Not Started
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

## Implementation Plan

Based on analyzing the codebase, I need to:

1. **Identify where None options cause errors** - Search for places that check `if options is None` and raise errors
2. **Understand Options.default()** - Check how the default Options are created
3. **Update error-raising code** - Replace error raising with `Options.default()` initialization
4. **Update tests** - Modify tests that expect errors to now expect successful execution with defaults
5. **Add new tests** - Test the new default behavior

**Areas to investigate:**
- `core.py` - FileRowReader and FileDataFrameReader classes
- `write_files()` function for S3 operations
- Any S3-related operations that require options

**Code Changes:**
1. Replace `if options is None: raise ValueError(...)` with `if options is None: options = Options.default()`
2. Update related tests to expect default behavior instead of errors
3. Add tests to verify default options work correctly for S3 operations

**Testing:**
- Test S3 operations work with None options using default credentials
- Test that explicit options still override defaults
- Test error handling when default options fail to initialize
