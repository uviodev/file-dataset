# Task 5: Implement Parallel Operations

## Summary
Add parallelism to all batch operations in `_core_file.py` using a ThreadPoolExecutor. This will significantly improve performance when working with multiple files.

## Implementation Plan

### 1. Extend S3Options Class
- Add `local_parallelism: int | None` parameter to `__init__` (default: None)
  - None means no parallelism (sequential execution)
  - Integer specifies the number of worker threads
- Add lazy `_executor` property that creates ThreadPoolExecutor on first use
- Implement `__del__` method to clean up executor (no context manager pattern)
- Update `__getstate__` and `__setstate__` to handle executor serialization
- Ensure executor is reused across operations for efficiency

### 2. Parallelize Operations in _core_file.py

#### 2.1 copy_each_file()
- Keep synchronous validation (sources exist, destinations unique)
- Submit each `_copy_single_file()` call to executor if parallelism enabled
- Use `concurrent.futures.as_completed()` to collect results
- Maintain error tracking with proper index mapping

#### 2.2 read_each_file_size()
- Keep synchronous S3 options check
- Submit each `_read_file_size()` call to executor if parallelism enabled
- Collect results maintaining key mapping

#### 2.3 read_each_file_contents()
- Keep synchronous S3 options check
- Submit each `_read_file_contents()` call to executor if parallelism enabled
- Collect results maintaining key mapping

#### 2.4 validate_each_file()
- Keep synchronous format validation (_validate_s3_path_format)
- Only parallelize existence checks (_validate_file_exists) if enabled
- Maintain error dictionary structure

### 3. Implementation Details

#### Thread Safety Considerations
- S3 client creation in S3Options is already thread-safe (uses lock)
- boto3 clients are thread-safe for read operations
- Each thread will use the same shared S3 client instance

#### Error Handling Pattern
```python
if s3_options and s3_options.local_parallelism:
   executor = s3_options.executor
   futures = {executor.submit(func, arg): key for key, arg in items}
   for future in as_completed(futures):
      key = futures[future]
      try:
         result[key] = future.result()
      except Exception as e:
         errors[key] = str(e)
else:
    # Existing sequential code
```

#### Executor Lifecycle
- Create executor lazily when first needed
- Reuse same executor for multiple operations
- Clean up in `__del__` method
- Handle pickle/unpickle by recreating executor

### 4. Testing Strategy

#### Test Cases
1. **test_parallel_copy_performance** - Verify parallel is faster than sequential
2. **test_parallel_with_various_levels** - Test with parallelism 1, 4, 16, None
3. **test_parallel_error_handling** - Test partial failures are handled correctly
4. **test_thread_safety** - Concurrent operations don't interfere
5. **test_executor_cleanup** - No resource leaks after S3Options deletion
6. **test_pickle_with_executor** - Verify S3Options pickles correctly with executor
7. **test_sequential_fallback** - Verify None parallelism uses sequential code

#### Mock Strategy
- Use moto for S3 operations
- Add artificial delays in mocked operations to test performance
- Create files that trigger specific errors for error handling tests

### 5. Questions/Clarifications Needed
None - the requirements are clear from the task description.

## Testing

### High Level Test Cases
1. **Performance Tests**
   - Compare execution time of parallel vs sequential for 20+ files
   - Verify speedup scales with parallelism level
   - Test with mixed local and S3 files

2. **Error Handling Tests**
   - Some files exist, some don't
   - Some operations succeed, some fail with permissions
   - Network errors during S3 operations
   - Verify errors are properly mapped to original keys

3. **Resource Management Tests**
   - Create and destroy S3Options multiple times
   - Verify no thread/resource leaks
   - Test pickle/unpickle with active executor

4. **Thread Safety Tests**
   - Multiple operations using same S3Options
   - Concurrent reads and writes
   - Verify S3 client sharing works correctly

5. **Configuration Tests**
   - Test with local_parallelism=None (sequential)
   - Test with local_parallelism=1 (single thread pool)
   - Test with high local_parallelism (4+)
   - Verify behavior matches expectations
