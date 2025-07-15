# Task 7: Simplify write_files API with Type-Based Detection

**Status:** Not Started
**Priority:** High
**Dependencies:** (Split Reader API)

**Summary:** Simplify write_files() API by automatically detecting single row vs DataFrame based on first argument type.

**Description:**
Currently write_files() has confusing kwargs for row= and dataframe= scenarios. Simplify the API to have a single function that automatically detects the operation type based on the first argument:
- `write_files(data: DataFrame | Mapping[str, str | Path | None], *, into_path, id=None, options=None)`
- If `data` is a dict/mapping → single row operation (id required)
- If `data` is a DataFrame → file dataframe operation (id not allowed, uses DataFrame 'id' column)

Remove the separate kwargs and make the API more intuitive by using type-based dispatch. This creates a cleaner interface while maintaining all existing functionality.

## Implementation Plan

Based on analyzing the current codebase, I need to simplify the `write_files()` function in `_writer.py` to use type-based detection instead of kwargs.

**Current API Analysis:**

The current `write_files()` function (lines 249-302 in `_writer.py`) uses kwargs with mutual exclusivity:
```python
def write_files(
    row: dict[str, str | Path | None] | None = None,
    *,
    into_path: str | Path,
    id: str | None = None,
    dataframe: pd.DataFrame | None = None,
    options: Options | None = None,
) -> dict[str, Path | None] | pd.DataFrame
```

**Problems with Current Design:**
1. **Confusing kwargs**: Users must remember to use `row=` vs `dataframe=`
2. **Complex validation**: Requires mutual exclusivity checks and conditional id validation
3. **Type confusion**: Return type changes based on which kwarg is used

**New API Design:**

```python
def write_files(
    data: dict[str, str | Path | None] | pd.DataFrame,
    *,
    into_path: str | Path,
    id: str | None = None,
    options: Options | None = None,
) -> dict[str, Path | None] | pd.DataFrame
```

**Logic:**
- If `data` is `dict/Mapping` → single row operation (id required)
- If `data` is `pd.DataFrame` → file dataframe operation (id forbidden, uses DataFrame 'id' column)

**Code Changes Required:**

1. **Modify `write_files()` function signature:**
   - Change to single `data` parameter instead of `row=` and `dataframe=` kwargs
   - Update type hints to use Union type
   - Remove mutual exclusivity validation
   - Add type-based dispatch logic

2. **Update validation logic:**
   - Check `isinstance(data, pd.DataFrame)` for DataFrame operations
   - Check `isinstance(data, collections.abc.Mapping)` for row operations
   - Require `id` for dict/mapping input, forbid for DataFrame input

3. **Update internal function calls:**
   - `_write_row_files()` gets called for dict/mapping data
   - `_write_dataframe_files()` gets called for DataFrame data

4. **Update `pipeline.py`:**
   - Change `write_files(row=result_files, ...)` to `write_files(result_files, ...)`

5. **Update all tests:**
   - `test_write_files.py` - change `write_files(row=..., ...)` to `write_files(..., ...)`
   - `test_write_dataframe.py` - change `write_files(dataframe=df, ...)` to `write_files(df, ...)`
   - Update validation tests for new error messages
   - Add tests for type-based detection edge cases

**Testing:**
- Update existing tests in test_write_files.py and test_write_dataframe.py to use new API
- Test write_files() with dict input works as single row operation
- Test write_files() with DataFrame input works as file dataframe operation
- Test id parameter is required for dict input and forbidden for DataFrame input
- Test proper error messages when id parameter is used incorrectly
- Test type hints properly reflect the union type for the data parameter
- Test integration with Pipeline class still works correctly
- Test both local and S3 destinations work with the unified API
