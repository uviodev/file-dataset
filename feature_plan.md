# Task 8b: Implement Pipeline class

## Overview
Create a Pipeline class that combines read → process → write workflow for batch processing of file datasets. This provides a convenient way to chain file dataset operations together in a streaming manner.

## Requirements
1. Implement `file_dataset.Pipeline(fn, write_options)` class
2. Add the code to a new file, pipeline.py. Use core and options primitives to implement
3. Make Pipeline callable with pandas DataFrame with column for `id` and files. Raise ValueError if not provided.
4. Process each row in a streaming manner: for each row, download files → call user function → upload results; use the streaming `into_temp_dir()` implementation for data frame
5. Use separate temporary directories for each row to avoid disk space issues
6. Drop failed rows rather than failing entire batch
7. Make Pipeline pickle-able for distributed computing
8. Return output DataFrame with result file URLs

## Implementation Plan

### Step 1: Examine existing codebase architecture
- Review how FileDataFrameReader works with the new into_temp_dir() API
- Understand how write_files works with DataFrames
- Study the Options class for S3 configuration

### Step 2: Create pipeline.py file
- Implement Pipeline class constructor that takes user function and write options
- Ensure the class is pickle-able by storing serializable state
- Add proper docstrings and type hints

### Step 3: Implement __call__ method
- Validate DataFrame has 'id' column and file columns
- Use FileDataFrameReader.into_temp_dir() to process each row
- Call user function on each temp directory
- Use write_files() to upload results
- Handle errors gracefully by dropping failed rows

### Step 4: Add comprehensive error handling
- Validate input DataFrame structure
- Handle user function failures
- Handle write operation failures
- Log appropriate error messages

### Step 5: Implement tests following red-green refactor
- Start with simple test cases and build up complexity
- Test all error conditions
- Test pickle serialization
- Test with various user functions

## Testing

### Test Cases
1. **Simple Pipeline execution** - Test with basic user function that processes files
2. **DataFrame validation** - Test that missing 'id' column raises ValueError
3. **File column detection** - Test error when no file columns found
4. **Partial failure handling** - Test that failed rows are dropped, successful ones processed
5. **Pickle serialization** - Test that Pipeline objects can be pickled/unpickled
6. **Temporary directory cleanup** - Verify temp dirs are cleaned up properly
7. **Various user functions** - Test with different types of user processing functions
8. **Mixed local and S3** - Test with different file sources and destinations
