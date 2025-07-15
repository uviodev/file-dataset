# Task 5: Consolidate File DataFrame Concept

**Status:** Not Started
**Priority:** Medium
**Dependencies:** Task 4 (Core module split)

**Summary:** Create dedicated file_dataframe.py module to consolidate DataFrame validation logic there.

**Description:**
Consolidate the concept of a `file_dataframe` into a new file, `file_dataframe.py`. Move the logic for getting the file_dataframe's columns there. Replace scattered validation logic in `pipeline.py` with centralized validation functions. Ray code should remain in separate ray modules though it can now import the validation logic from file_dataframe.py.

## Implementation Plan

Based on analyzing the current codebase, I need to consolidate DataFrame validation logic scattered across multiple files into a new `file_dataframe.py` module.

**Current Validation Logic to Consolidate:**

1. **From `pipeline.py`:**
   - `_validate_and_get_file_dataframe_columns()` function (lines 17-52)
   - Logic for checking 'id' column existence
   - File column detection (columns with file extensions)
   - Error handling for missing file columns

2. **From `ray/__init__.py`:**
   - Unique 'id' column validation (lines 43-50)
   - Additional DataFrame constraints for Ray processing

3. **From `ray/_datasource.py`:**
   - Import and usage of validation function (avoiding circular imports)

**New `file_dataframe.py` Module Structure:**

```python
# Core validation functions:
def validate_file_dataframe(dataframe: pd.DataFrame) -> None
def get_file_columns(dataframe: pd.DataFrame) -> list[str]
def validate_id_column(dataframe: pd.DataFrame) -> None
def validate_unique_ids(dataframe: pd.DataFrame) -> None

# Helper functions:
def _has_file_extension(column_name: str) -> bool
def _get_potential_file_columns(dataframe: pd.DataFrame) -> list[str]
```

**Code Changes Required:**

1. **Create `src/file_dataset/file_dataframe.py`:**
   - Move validation logic from pipeline.py
   - Add comprehensive DataFrame validation functions
   - Include proper type hints and docstrings

2. **Update `src/file_dataset/pipeline.py`:**
   - Remove `_validate_and_get_file_dataframe_columns()` function
   - Import validation functions from file_dataframe module
   - Update usage in `Pipeline.__call__()` method

3. **Update `src/file_dataset/ray/__init__.py`:**
   - Import validation from file_dataframe module instead of duplicating logic
   - Use centralized unique ID validation

4. **Update `src/file_dataset/ray/_datasource.py`:**
   - Import from file_dataframe module (solves circular import issue)
   - Use centralized validation functions

5. **Update `src/file_dataset/__init__.py`:**
   - Add any necessary exports from file_dataframe module

**Testing:**
- Test file_dataframe column detection and validation logic
- Test that DataFrame validation catches invalid column names, missing columns, etc.
- Test integration with pipeline.py to ensure validation works in processing workflows
- Test edge cases: empty DataFrames, DataFrames with unusual column types
- Test unique ID validation for Ray usage
- Test file extension detection logic
- Regression tests to ensure existing pipeline functionality remains intact
