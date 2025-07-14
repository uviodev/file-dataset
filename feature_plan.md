# Task 1: Implement reader into_temp_dir for local directories

## Overview
Implement basic reader functionality that copies files from local directories to temporary directories using a context manager pattern.

## High-Level Design

### Core Components
1. **FileDatasetError Exception**: Custom exception class for file dataset operations
2. **Reader Class**: Handles file copying operations with context manager support
3. **reader() Function**: Factory function that creates Reader instances

### API Design
```python
# Usage pattern
files_dict = {"image.mha": "/path/to/image.mha", "mask.mha": "/path/to/mask.mha"}
with file_dataset.reader(row=files_dict).into_temp_dir() as temp_dir:
    # temp_dir contains copied files: image.mha, mask.mha
    process_files(temp_dir)
# temp_dir is automatically cleaned up
```

### Key Design Decisions
- Use `pathlib.Path` for all path operations
- Use `shutil.copy2()` to preserve file metadata during copying
- Use `tempfile.TemporaryDirectory` for automatic cleanup
- Raise `FileDatasetError` for missing files with detailed error mapping
- Copy files with original names to temp directory (no path structure)

## Implementation Plan

### 1. Exception Classes
Create `FileDatasetError` exception class:
- Store mapping of filename to error message
- Inherit from standard Exception
- Include helpful string representation

### 2. Reader Class  
```python
class Reader:
    def __init__(self, files_dict: Dict[str, Union[str, Path]]):
        # Store files mapping
        # Validate input format
    
    def into_temp_dir(self) -> ContextManager[Path]:
        # Return context manager for temporary directory
        # Copy files on enter, cleanup on exit
```

### 3. Factory Function
```python
def reader(*, row: Dict[str, Union[str, Path]]) -> Reader:
    # Create and return Reader instance
    # Validate row parameter is provided as keyword
```

## Testing Plan

### Test Cases
1. **Single file copy**: Test with one file
2. **Multiple file copy**: Test with several files  
3. **Missing file error**: Test FileDatasetError is raised with proper mapping
4. **Context manager cleanup**: Verify temp directory is removed
5. **File content integrity**: Verify copied files have same content
6. **Edge cases**: Empty dict, None values, invalid paths

### Test Data Structure
```
tests/
  data/
    test_image.txt     # < 1KB test file
    test_mask.txt      # < 1KB test file
    test_config.json   # < 1KB test file
```

### Test Implementation
- Use pytest fixtures for test data creation
- Test both success and failure paths
- Verify proper cleanup in all scenarios
- Test with various file types and sizes (all < 1KB)

## File Structure Changes
```
src/file_dataset/
  __init__.py          # Export reader function and FileDatasetError
  exceptions.py        # FileDatasetError class
  core.py             # Reader class and reader function
```

## Dependencies
- `pathlib` (standard library)
- `shutil` (standard library) 
- `tempfile` (standard library)
- `typing` (standard library)

No new external dependencies required for this task.