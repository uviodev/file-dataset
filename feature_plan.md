# Task 2: Implement writer into_path for local directories

## Overview
Implement basic writer functionality that copies files from one local directory to another with ID-based organization.

## High-Level Design

### Core Components
1. **write_files() Function**: Main function for writing files with ID-based organization
2. **Directory Management**: Automatic creation of output directory structure
3. **Error Handling**: Graceful handling of missing files and write failures
4. **None Value Support**: Proper handling of optional output files (None values)

### API Design
```python
# Usage pattern
files_dict = {"image.mha": Path("/source/image.mha"), "mask.mha": Path("/source/mask.mha")}
result = file_dataset.write_files(
    row=files_dict, 
    into_path="/output/base",
    id="sample_001"
)
# Returns: {"image.mha": Path("/output/base/sample_001/image.mha"), ...}
```

### Key Design Decisions
- Use keyword-only arguments for `into_path` and `id`
- Create directory structure: `{into_path}/{id}/filename`
- Use `pathlib.Path` for all path operations
- Use `shutil.copy2()` to preserve file metadata during copying
- Raise `FileDatasetError` for missing files with detailed error mapping
- Support None values in input dict (optional files) and propagate to output
- Atomic operation: if any required file fails, no files are written

## Implementation Plan

### 1. write_files Function
```python
def write_files(
    row: dict[str, str | Path | None], 
    *, 
    into_path: str | Path, 
    id: str
) -> dict[str, Path | None]:
    # Validate inputs
    # Check source files exist (skip None values)
    # Create output directory structure
    # Copy files to destination
    # Return mapping of filenames to final paths
```

### 2. Key Implementation Details
- **Validation Phase**: Check all non-None source files exist before starting
- **Directory Creation**: Use `Path.mkdir(parents=True, exist_ok=True)`
- **File Copying**: Use `shutil.copy2()` for metadata preservation
- **Error Handling**: Collect all errors and raise single FileDatasetError
- **None Handling**: Skip None values in validation and copying, propagate to output

### 3. Error Scenarios
- Missing source files (non-None values)
- Permission errors on destination directory
- Disk space issues during copying
- Invalid path formats

## Testing Plan

### Test Cases
1. **Single file write**: Test with one file
2. **Multiple file write**: Test with several files  
3. **Missing file error**: Test FileDatasetError is raised with proper mapping
4. **Directory creation**: Verify output directory structure is created
5. **File content integrity**: Verify copied files have same content
6. **None value handling**: Test optional files (None values) are handled correctly
7. **Edge cases**: Empty dict, all None values, invalid paths
8. **Atomic operation**: Verify no files written if any required file fails

### Test Data Structure
```
tests/
  data/
    test_image.txt     # < 1KB test file
    test_mask.txt      # < 1KB test file
    test_config.json   # < 1KB test file
```

### Test Implementation
- Use pytest fixtures for test data creation and temp directory cleanup
- Test both success and failure paths
- Verify proper directory creation and file placement
- Test with various file types and sizes (all < 1KB)
- Test None value propagation through the function

## File Structure Changes
```
src/file_dataset/
  __init__.py          # Export reader, write_files functions and FileDatasetError
  exceptions.py        # FileDatasetError class (already exists)
  core.py             # Reader class, reader function, and write_files function
```

## Dependencies
- `pathlib` (standard library)
- `shutil` (standard library) 
- `tempfile` (standard library)
- `typing` (standard library)

No new external dependencies required for this task.