# File Dataset Implementation Tasks

A proof of concept library has been implemented. Now, some refactoring needs to be done to take it to the next level and clean up the codebase.

There are some file refactors to make.
1. Move s3-related helper functions like parse_url into a separate `s3_utils.py` file.
2. The class `core.Reader` should be renamed to `core.FileRowReader`. Tests should be accordingly updated. Documentation should be updated.
3. Modify the behavior of an `options` value of None. Instead of raising an error, the options are initialized to Options.default() instead.
4. Split core.py into two files. One named `_reader.py` (containing reader() and related classes) and one named `_writer.py` (containing the writing functions). Update `__init__.py` so the user doesn't see any change. Update the related tests.
5. Consolidate the concept of a `file_dataframe` into a new file, `file_dataframe.py`. Move the logic for getting the file_dataframe's columns there. Replace logic in pipeline.py and ray dataset code that validates.



## Implementation Notes

### Testing Strategy
- Use moto library to mock all S3 operations
- Keep all testdata files under 1KB in size; put them in the folder tests/data/
- Focus on integration tests that verify end-to-end workflows
- Test error conditions and edge cases thoroughly

### Code Quality
- Follow established linting rules and security practices
- Ensure all code passes `uv run lint` checks
- Add comprehensive docstrings following Google convention
- Use type hints throughout the codebase

### Dependencies
Key dependencies to add as implementation progresses:
- `boto3` for S3 operations
- `pyarrow` for table operations
- `pandas` for DataFrame handling
- `ray` for distributed computing
- `moto` for S3 testing (dev dependency)
