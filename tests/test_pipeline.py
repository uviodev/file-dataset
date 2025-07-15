"""Tests for Pipeline class."""

import pickle
from pathlib import Path

import pandas as pd
import pytest

from file_dataset import FileDatasetError, Pipeline


def pickle_test_function(temp_dir: Path) -> dict[str, Path]:
    """Global function for pickle testing."""
    output_file = temp_dir / "result.txt"
    output_file.write_text("processed")
    return {"result.txt": output_file}


class TestPipeline:
    """Test cases for the Pipeline class."""

    def test_simple_pipeline_execution(self, tmp_path):
        """Test Pipeline with basic user function that processes files."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input1.txt").write_text("content1")
        (source_dir / "input2.txt").write_text("content2")

        # Create DataFrame with test data (using file extensions in column names)
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "input_a.txt": str(source_dir / "input1.txt"),
                    "input_b.txt": str(source_dir / "input2.txt"),
                }
            ]
        )

        # Define user function that creates output files
        def process_files(temp_dir: Path) -> dict[str, Path]:
            # Verify input files exist in temp dir
            assert (temp_dir / "input_a.txt").exists()
            assert (temp_dir / "input_b.txt").exists()

            # Create output file
            output_file = temp_dir / "result.txt"
            output_file.write_text("processed content")

            return {"result.txt": output_file}

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(process_files, into_path=dest_dir)

        # Execute pipeline
        result_df = pipeline(df)

        # Verify result
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert "id" in result_df.columns
        assert "result.txt" in result_df.columns
        assert result_df.iloc[0]["id"] == "row1"

        # Verify output file was created
        result_path = Path(result_df.iloc[0]["result.txt"])
        assert result_path.exists()
        assert result_path.read_text() == "processed content"

    def test_dataframe_missing_id_column(self, tmp_path):
        """Test that missing 'id' column raises ValueError."""
        # Create DataFrame without 'id' column
        df = pd.DataFrame(
            [
                {
                    "file_a.txt": "/path/to/file1.txt",
                    "file_b.txt": "/path/to/file2.txt",
                }
            ]
        )

        def dummy_process(temp_dir: Path) -> dict[str, Path]:  # noqa: ARG001
            return {}

        # Create pipeline
        pipeline = Pipeline(dummy_process, into_path=tmp_path)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame must have an 'id' column"):
            pipeline(df)

    def test_dataframe_no_file_columns(self, tmp_path):
        """Test that DataFrame with only 'id' column raises ValueError."""
        # Create DataFrame with only 'id' column
        df = pd.DataFrame([{"id": "row1"}])

        def dummy_process(temp_dir: Path) -> dict[str, Path]:  # noqa: ARG001
            return {}

        # Create pipeline
        pipeline = Pipeline(dummy_process, into_path=tmp_path)

        # Should raise ValueError
        with pytest.raises(
            ValueError, match="No valid file columns found in DataFrame"
        ):
            pipeline(df)

    def test_partial_failure_handling(self, tmp_path, caplog):
        """Test that failed rows are dropped while successful ones are processed."""
        # Create test files (only for successful rows)
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "good_file.txt").write_text("good content")

        # Create DataFrame with mixed valid and invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "good_row",
                    "input.txt": str(source_dir / "good_file.txt"),
                },
                {
                    "id": "bad_row",
                    "input.txt": "/nonexistent/missing_file.txt",  # This will fail
                },
            ]
        )

        # Define user function
        def process_files(temp_dir: Path) -> dict[str, Path]:
            # Create output file
            output_file = temp_dir / "result.txt"
            output_file.write_text("processed")
            return {"result.txt": output_file}

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(process_files, into_path=dest_dir)

        # Execute pipeline
        result_df = pipeline(df)

        # Should only have the successful row
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == "good_row"

        # Check that error was logged for the failed row
        assert (
            "Failed to process row bad_row" in caplog.text
            or "Failed to process row bad_row" in str(caplog.records)
        )

    def test_pickle_serialization(self, tmp_path):
        """Test that Pipeline objects can be pickled and unpickled."""
        # Create pipeline with global function
        pipeline = Pipeline(pickle_test_function, into_path=tmp_path)

        # Pickle and unpickle
        pickled_data = pickle.dumps(pipeline)
        unpickled_pipeline = pickle.loads(pickled_data)

        # Verify the unpickled pipeline has the same attributes
        assert unpickled_pipeline.into_path == pipeline.into_path
        # Both should have S3Options instances (not None)
        assert unpickled_pipeline.options is not None
        assert pipeline.options is not None
        # Options should have equivalent session configuration
        assert (
            unpickled_pipeline.options._session_kwargs
            == pipeline.options._session_kwargs
        )
        assert callable(unpickled_pipeline.fn)

    def test_pipeline_options_never_none(self, tmp_path):
        """Test that Pipeline.options is never None."""
        from file_dataset.s3_options import S3Options

        # Test with options=None
        pipeline1 = Pipeline(pickle_test_function, into_path=tmp_path, options=None)
        assert pipeline1.options is not None
        assert isinstance(pipeline1.options, S3Options)

        # Test with no options argument
        pipeline2 = Pipeline(pickle_test_function, into_path=tmp_path)
        assert pipeline2.options is not None
        assert isinstance(pipeline2.options, S3Options)

        # Test with explicit options
        custom_options = S3Options.default()
        pipeline3 = Pipeline(
            pickle_test_function, into_path=tmp_path, options=custom_options
        )
        assert pipeline3.options is custom_options

    def test_all_rows_fail(self, tmp_path):
        """Test that all rows failing raises FileDatasetError."""
        # Create DataFrame with all invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "bad_row1",
                    "input.txt": "/nonexistent/missing1.txt",
                },
                {
                    "id": "bad_row2",
                    "input.txt": "/nonexistent/missing2.txt",
                },
            ]
        )

        def process_files(temp_dir: Path) -> dict[str, Path]:
            return {"result.txt": temp_dir / "result.txt"}

        # Create pipeline
        pipeline = Pipeline(process_files, into_path=tmp_path)

        # Should raise FileDatasetError when all rows fail
        with pytest.raises(FileDatasetError) as exc_info:
            pipeline(df)

        error = exc_info.value
        assert "All rows failed" in str(error)

    def test_user_function_failure(self, tmp_path, caplog):  # noqa: ARG002
        """Test that user function failures are handled gracefully."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input.txt").write_text("content")

        # Create DataFrame
        df = pd.DataFrame(
            [
                {
                    "id": "failing_row",
                    "input.txt": str(source_dir / "input.txt"),
                }
            ]
        )

        # Define user function that always fails
        def failing_process(temp_dir: Path) -> dict[str, Path]:  # noqa: ARG001
            msg = "User function error"
            raise RuntimeError(msg)

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(failing_process, into_path=dest_dir)

        # Should raise FileDatasetError when all processing fails
        with pytest.raises(FileDatasetError) as exc_info:
            pipeline(df)

        error = exc_info.value
        assert "All rows failed during processing" in str(error)

    def test_invalid_column_names_no_extensions(self, tmp_path):
        """Test that columns without file extensions raise ValueError."""
        # Create DataFrame with columns that don't have file extensions
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "file_a": "/path/to/file1.txt",  # Column name has no extension
                    "result": "/path/to/file2.txt",  # Column name has no extension
                }
            ]
        )

        def dummy_process(temp_dir: Path) -> dict[str, Path]:  # noqa: ARG001
            return {}

        # Create pipeline
        pipeline = Pipeline(dummy_process, into_path=tmp_path)

        # Should raise ValueError for invalid column names
        with pytest.raises(
            ValueError, match="No valid file columns found in DataFrame"
        ):
            pipeline(df)

    def test_mixed_valid_invalid_column_names(self, tmp_path):
        """Test that only columns with file extensions are considered valid."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input.txt").write_text("content")

        # Create DataFrame with mix of valid and invalid column names
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "image.jpg": str(source_dir / "input.txt"),  # Valid: has extension
                    "mask": str(source_dir / "input.txt"),  # Invalid: no extension
                    "data.csv": str(source_dir / "input.txt"),  # Valid: has extension
                    "output": str(source_dir / "input.txt"),  # Invalid: no extension
                }
            ]
        )

        def process_files(temp_dir: Path) -> dict[str, Path]:
            # Should only have files from valid columns
            assert (temp_dir / "image.jpg").exists()
            assert (temp_dir / "data.csv").exists()
            # Invalid columns should not be present
            assert not (temp_dir / "mask").exists()
            assert not (temp_dir / "output").exists()

            output_file = temp_dir / "result.txt"
            output_file.write_text("processed")
            return {"result.txt": output_file}

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(process_files, into_path=dest_dir)

        # Should work with only the valid file columns
        result_df = pipeline(df)
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == "row1"

    def test_valid_file_extensions(self, tmp_path):
        """Test that various valid file extensions work correctly."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input.txt").write_text("content")

        # Create DataFrame with various valid file extensions
        df = pd.DataFrame(
            [
                {
                    "id": "row1",
                    "image.jpg": str(source_dir / "input.txt"),
                    "data.csv": str(source_dir / "input.txt"),
                    "script.py": str(source_dir / "input.txt"),
                    "archive.tar.gz": str(source_dir / "input.txt"),
                    "config.json": str(source_dir / "input.txt"),
                }
            ]
        )

        def process_files(temp_dir: Path) -> dict[str, Path]:
            # All files should be present
            assert (temp_dir / "image.jpg").exists()
            assert (temp_dir / "data.csv").exists()
            assert (temp_dir / "script.py").exists()
            assert (temp_dir / "archive.tar.gz").exists()
            assert (temp_dir / "config.json").exists()

            output_file = temp_dir / "result.txt"
            output_file.write_text("processed")
            return {"result.txt": output_file}

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(process_files, into_path=dest_dir)

        # Should work with all valid file extensions
        result_df = pipeline(df)
        assert len(result_df) == 1
