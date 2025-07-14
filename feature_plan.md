# Task 12: Implement custom Ray data source for blob tables

**Status**: Not started
**Description**: Create Ray data source that efficiently loads file datasets into cluster memory as blob tables.

## Requirements
- Implement `file_dataset.ray.read_file_dataset(file_dataframe, batch_size, options)` where file_datasource.Options provides boto3 configuration
    - file_dataframe is a pandas dataframe, that has an id column and certain file columns
    - the ray dataset schema should be that of `into_blob_table()`. The columns will be the file columns as well as id. The values will be the file contents.
    - the ray dataset will be potentially 1000s-1,000,000,000s of times larger than `file_dataframe` as in production each row in the file dataframe will have potentially several s3 urls that expand to large objects
- Raise ValueError if dataframe does not have an `id` column that is unique
- On the implementation of read_file_dataset(), create custom Ray Datasource named FileDataFrameAsBlobDatasource. Then read it usin g[ray.data.read_datasource](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html)
    - Extend ray.data.Datasource` (do NOT extend file based datasource as the datasource here itself is a DataFrame that's already been loaded).
    - Define `FileDataFrameAsBlobDatasource.__init__` to take in (file_dataframe: pd.DataFrame, batch_size, and options) per above. The file_dataframe should be a valid file dataframe as defined in pipeline.py
    - Implement `estimate_inmemory_data_size` with `into_size_table()` for memory estimation on first batch (do not call this function for the whole dataset but use self.dataframe.head(batch_size))`
    - Implement `get_read_tasks(parallelism: int)`: Create one read task that will yield `batch_size` rows for each dataset that returns `into_blob_table()` for each dataframe. Ignore the parallelism command and require the user has specified `self.batch_size` as the blob tables can out-of-memory if they exceed `batch_size` rows. Provide documentation of this behavior in the docstring.

NOTE: no need to implement a Datasink as the user may write the dataset here to a different format like parquet, or use map_batches to write data to s3 on a rolling basis during processing

## Testing
- Test file_dataset.ray.read_file_dataset() with integration testing that the expected rows return the relevant file content
- Test zero-copy operations when available
- Add an integration test with a pseudo-realistic dataset with 1000 rows (keeping individual files < 1KB)

## Implementation Plan

Based on the existing codebase structure and Ray requirements, I need to:

1. **Understand existing code structure**: Review current reader implementations and pipeline code
2. **Create ray submodule**: Create `src/file_dataset/ray/__init__.py` with the public API
3. **Implement FileDataFrameAsBlobDatasource**: Custom Ray datasource extending `ray.data.Datasource`
4. **Implement read_file_dataset function**: Public API that validates input and creates Ray dataset
5. **Write comprehensive tests**: Integration tests with moto for S3 mocking

## Code Changes Needed

1. **New module structure**: Create `src/file_dataset/ray/` directory with proper `__init__.py`
2. **Custom datasource implementation**: Implement `FileDataFrameAsBlobDatasource` class
3. **Public API function**: Implement `read_file_dataset()` with validation
4. **Test file creation**: Create comprehensive tests for the Ray integration
5. **Documentation**: Add docstrings following Google convention

## Testing Strategy

The tests will use small file sizes (< 1KB) and moto for S3 mocking. I'll test with pseudo-realistic datasets while keeping individual files small as per project guidelines.
