# Task 8: Rename Options to S3Options for Clarity

**Status:** Not Started
**Priority:** Medium
**Dependencies:** Task 7 (Simplify write_files API)

**Summary:** Rename Options class to S3Options and rename options.py to s3_options.py for clarity.

**Description:**
The generic name "Options" doesn't clearly indicate its purpose for S3 configuration. Rename the Options class to S3Options to make its purpose explicit. Also rename the options.py file to s3_options.py for consistency. Update all imports and references throughout the codebase to use the new naming.

## Implementation Plan

Based on analyzing the current codebase, I need to rename the Options class to S3Options and update all references.

**Current Analysis:**

The Options class is defined in `src/file_dataset/options.py` and is used throughout the codebase for S3 configuration. It contains S3 client configuration like credentials, region, and endpoint URL.

**Changes Required:**

1. **Rename the file:**
   - Rename `src/file_dataset/options.py` to `src/file_dataset/s3_options.py`

2. **Rename the class:**
   - Change `class Options` to `class S3Options` in the new s3_options.py file

3. **Update imports throughout the codebase:**
   - Update `__init__.py` to import from s3_options and export S3Options
   - Update all files that import Options to import S3Options
   - Search for all references to Options and update them to S3Options

4. **Update documentation and examples:**
   - Update README.md examples to use S3Options instead of Options

**Files to Update:**

Based on the codebase structure, I need to check and update:
- `src/file_dataset/__init__.py` - public API exports
- `src/file_dataset/_reader.py` - likely imports Options
- `src/file_dataset/_writer.py` - likely imports Options
- `src/file_dataset/pipeline.py` - likely imports Options
- `src/file_dataset/ray/` modules - likely import Options
- `tests/` - all test files that use Options
- `README.md` - examples using Options

**Testing:**
- Test that S3Options class maintains all functionality of the original Options class
- Test import statements and public API access work with new naming
- Test class instantiation and method calls with S3Options name
- Test that s3_options.py module can be imported correctly
- Regression tests to verify all existing Options functionality works with new class name
- Test integration with reader, writer, and pipeline components using S3Options
