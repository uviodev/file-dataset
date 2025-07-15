# File Dataset Implementation Tasks
High level goal is to optimize read/write parallelism when there are multiple files in the dataframe.
We are going to implement Local Parallelism.
Boto3 S3Transfer already has its own threadpool to copy data, so the main goal is to be able to initiate several transfers in parallel.

Tasks
1. Create a new library, `_core_file.py`. This library defines an interface to copy data, `do_copy(copies: list[str | Path, str | Path], s3_options: S3Options)`. Given a list of (src, dest) pairs and S3Options, this  validates each copy (ensure src exists, ensure unique destinations, and if s3 is used then s3 options must not be None) then executes each copy sequentially. There needs to be also some low-level function, `_do_single_copy(src, dst, s3_options: S3Options)` that delegates to either shutil or the `s3_options.s3_client`. Refactor existing logic in the writer (write_files) and reader (into_temp_dir) to delegate to this new _core_file.py `do_copy` function.
2. To `_core_file.py` add the capability to get file sizes `read_file_sizes(files: Mapping[str, str | Path]) -> Mapping[str, int]` as well as the the file content `read_file_contents(files: Mapping[str, str | Path]) -> Mapping[str, bytes]`. Under the hood, these will call into two corresponding low-level functions, `_read_file_size(str | Path, s3_options: S3Options) -> int` and `_read_file_contents(str|Path, s3_options: S3Options) -> bytes`. Refactor `into_size_table()` and `into_blob_table()` to use the new top-level APIs that operate on mappings.
3. Refactor boto3 s3 API calls in `_core_file.py` to use S3Transfer methods (upload_file/upload_fileobj instead of put_object, download_file/download_fileobj). (HeadObject is stil OK for existence checks and size checks). Ensure `s3_options.transfer_config` is passed to all s3-transfer compatible method calls on the s3 client.
4. Parallelize `_core_file.py` operations. Each exposed function `do_copy`, `read_file_sizes` and `read_file_contents` will execute in parallel. Notes: Introduce a `local_parallelism` parameter to S3Options. S3Options will now have a lazy ThreadPoolExecutor is created and reused, similar to how the s3client is created/used. The ThreadPoolExecutor will be cleaned up when the S3Options are deleted and it will NOT be used in a context manager so that it is reused. When local_parallelism is configured, `_core_file.py` will use the S3Options ThreadPoolExecutor to implement parallel calls


## Implementation Notes

### Testing Strategy
- Use moto library to mock all S3 operations
- Keep all testdata files under 1KB in size; put them in the folder tests/data/
- Focus on integration tests that verify end-to-end workflows
- Test error conditions and edge cases thoroughly

### Code Quality
- Follow established linting rules and security practices
- Ensure all code passes `uv run lint` checks and `uv run test` unit tests
- Add comprehensive docstrings following Google convention
- Use type hints throughout the codebase

### Dependencies
Key dependencies to add as implementation progresses:
- `boto3` for S3 operations
- `pyarrow` for table operations
- `pandas` for DataFrame handling
- `ray` for distributed computing
- `moto` for S3 testing (dev dependency)
