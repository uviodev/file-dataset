# Task 11: Test Pipeline with local Ray data integration

**Status**: In Progress
**Description**: Verify Pipeline class works correctly with Ray's map_batches functionality for distributed processing.

## Requirements
- Create tests using Ray's `map_batches` with Pipeline objects
- Test with pandas batch format
- Verify distributed execution across multiple workers
- Test with various batch sizes and concurrency settings
- Ensure proper resource management (memory, disk space)

## Testing
- Test Ray dataset creation from CSV with file URLs
- Test Pipeline execution with Ray's map_batches
- Test scaling with different concurrency levels (keep the concurrency levels small though for unit testing)
- Test resource cleanup after Ray jobs complete
- Test error handling in distributed environment

## Implementation Plan

Based on the existing codebase structure and pipeline implementation, I need to:

1. **Understand current Pipeline implementation**: Review how Pipeline works with pandas DataFrames
2. **Set up Ray integration tests**: Create tests that use Ray's map_batches with Pipeline objects
3. **Test pandas batch format**: Ensure Pipeline can handle pandas DataFrames passed from Ray
4. **Test distributed execution**: Verify Pipeline works across multiple Ray workers
5. **Test resource management**: Ensure temp directories and memory are properly cleaned up
6. **Test error handling**: Verify graceful failure handling in distributed environment

## Code Changes Needed

1. **Test file creation**: Create a new test file specifically for Ray integration
2. **Pipeline serialization verification**: Ensure Pipeline objects can be pickled/unpickled for Ray
3. **Batch processing tests**: Test various batch sizes and concurrency levels
4. **Resource cleanup tests**: Verify temp directories are cleaned up across workers

## Testing Strategy

The tests will be limited to small concurrency levels and small file sizes (< 1KB) as per project guidelines. I'll use moto for S3 mocking and ensure all tests pass the existing linting requirements.
