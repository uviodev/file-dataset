"""Tests for Ray integration with Pipeline class."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("ray")
import ray  # noqa: E402
import ray.data  # noqa: E402

from file_dataset import Pipeline  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def ray_context():
    """Initialize Ray for testing."""
    # Initialize Ray with minimal resources for testing
    if not ray.is_initialized():
        ray.init(num_cpus=2, object_store_memory=100_000_000)  # 100MB
    yield
    # Clean up Ray
    if ray.is_initialized():
        ray.shutdown()


def simple_processing_function(temp_dir: Path) -> dict[str, Path]:
    """Simple processing function for testing."""
    # Read input files and create output
    input_files = list(temp_dir.glob("*.txt"))
    if input_files:
        content = input_files[0].read_text()
        output_file = temp_dir / "processed.txt"
        output_file.write_text(f"processed_{content}")
        return {"processed.txt": output_file}
    return {}


def multi_file_processing_function(temp_dir: Path) -> dict[str, Path]:
    """Processing function that handles multiple input files."""
    input_files = sorted(temp_dir.glob("*.txt"))  # Sort for consistent ordering
    outputs = {}

    for i, input_file in enumerate(input_files):
        content = input_file.read_text()
        output_file = temp_dir / f"output_{i}.txt"
        output_file.write_text(f"processed_{content}")
        outputs[f"output_{i}.txt"] = output_file

    return outputs


class TestRayIntegration:
    """Test Ray integration with Pipeline class."""

    def test_pipeline_with_ray_map_batches_single_batch(self, tmp_path):
        """Test Pipeline execution with Ray map_batches using single batch."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        # Create small test files
        files_data = []
        for i in range(3):
            file_path = source_dir / f"input_{i}.txt"
            file_path.write_text(f"content_{i}")
            files_data.append({"id": f"row_{i}", "input.txt": str(file_path)})

        # Create DataFrame
        df = pd.DataFrame(files_data)

        # Create Ray dataset from DataFrame
        ray_dataset = ray.data.from_pandas([df])  # Single batch

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Execute pipeline with Ray map_batches (using default tasks compute)
        result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")

        # Collect results
        results = result_dataset.to_pandas()

        # Verify results
        assert len(results) == 3
        assert "id" in results.columns
        assert "processed.txt" in results.columns

        # Verify all rows processed correctly
        for i, row in results.iterrows():
            assert row["id"] == f"row_{i}"
            result_path = Path(row["processed.txt"])
            assert result_path.exists()
            assert result_path.read_text() == f"processed_content_{i}"

    def test_pipeline_with_ray_map_batches_multiple_batches(self, tmp_path):
        """Test Pipeline execution with Ray map_batches using multiple batches."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        # Create multiple test files (6 files for 3 batches of size 2)
        files_data = []
        for i in range(6):
            file_path = source_dir / f"input_{i}.txt"
            file_path.write_text(f"content_{i}")
            files_data.append({"id": f"row_{i}", "input.txt": str(file_path)})

        # Create DataFrame
        df = pd.DataFrame(files_data)

        # Create Ray dataset with explicit batching (3 batches of 2 rows each)
        ray_dataset = ray.data.from_pandas([df[:2], df[2:4], df[4:6]])

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Execute pipeline with Ray map_batches (using default tasks compute)
        result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")

        # Collect results
        results = result_dataset.to_pandas()

        # Verify results
        assert len(results) == 6
        assert "id" in results.columns
        assert "processed.txt" in results.columns

        # Verify all rows processed correctly
        for i, row in results.iterrows():
            assert row["id"] == f"row_{i}"
            result_path = Path(row["processed.txt"])
            assert result_path.exists()
            assert result_path.read_text() == f"processed_content_{i}"

    def test_pipeline_with_ray_concurrent_execution(self, tmp_path):
        """Test Pipeline with concurrent Ray execution (2 workers)."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        # Create test files for concurrent processing
        files_data = []
        for i in range(4):
            file_path = source_dir / f"input_{i}.txt"
            file_path.write_text(f"content_{i}")
            files_data.append({"id": f"row_{i}", "input.txt": str(file_path)})

        # Create DataFrame
        df = pd.DataFrame(files_data)

        # Create Ray dataset with 2 batches for concurrent processing
        ray_dataset = ray.data.from_pandas([df[:2], df[2:]])

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Execute pipeline with concurrent workers
        result_dataset = ray_dataset.map_batches(
            pipeline,
            batch_format="pandas",
            concurrency=2,  # Use 2 concurrent workers
        )

        # Collect results
        results = result_dataset.to_pandas()

        # Verify results
        assert len(results) == 4
        assert "id" in results.columns
        assert "processed.txt" in results.columns

        # Verify all rows processed correctly
        expected_ids = {f"row_{i}" for i in range(4)}
        actual_ids = set(results["id"])
        assert actual_ids == expected_ids

    def test_pipeline_serialization_with_ray(self, tmp_path):
        """Test that Pipeline objects serialize correctly for Ray workers."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input.txt").write_text("test_content")

        # Create DataFrame
        df = pd.DataFrame(
            [{"id": "test_row", "input.txt": str(source_dir / "input.txt")}]
        )

        # Create Ray dataset
        ray_dataset = ray.data.from_pandas([df])

        # Create pipeline with options
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Test that pipeline can be serialized by Ray
        try:
            result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")
            results = result_dataset.to_pandas()

            # Verify serialization worked correctly
            assert len(results) == 1
            assert results.iloc[0]["id"] == "test_row"

        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Pipeline serialization failed: {e}")

    def test_pipeline_error_handling_in_ray(self, tmp_path, caplog):  # noqa: ARG002
        """Test error handling when some batches fail in Ray environment."""
        # Create test files (only for successful rows)
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "good_file.txt").write_text("good_content")

        # Create DataFrame with mixed valid and invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "good_row",
                    "input.txt": str(source_dir / "good_file.txt"),
                },
                {
                    "id": "bad_row",
                    "input.txt": "/nonexistent/missing_file.txt",
                },
            ]
        )

        # Create Ray dataset
        ray_dataset = ray.data.from_pandas([df])

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Execute pipeline - should handle partial failures gracefully
        result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")

        # Collect results
        results = result_dataset.to_pandas()

        # Should only have the successful row
        assert len(results) == 1
        assert results.iloc[0]["id"] == "good_row"

    def test_pipeline_resource_cleanup_with_ray(self, tmp_path):
        """Test that temporary directories are properly cleaned up in Ray workers."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        files_data = []
        for i in range(3):
            file_path = source_dir / f"input_{i}.txt"
            file_path.write_text(f"content_{i}")
            files_data.append({"id": f"row_{i}", "input.txt": str(file_path)})

        # Create DataFrame
        df = pd.DataFrame(files_data)

        # Create Ray dataset
        ray_dataset = ray.data.from_pandas([df])

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(simple_processing_function, into_path=dest_dir)

        # Track temp directory creation before execution
        temp_dirs_before = len(list(Path(tempfile.gettempdir()).glob("tmp*")))

        # Execute pipeline
        result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")

        # Collect results
        results = result_dataset.to_pandas()

        # Verify results processed correctly
        assert len(results) == 3

        # Give some time for cleanup (Ray may have async cleanup)
        import time

        time.sleep(1)

        # Check that we don't have excessive temp directories
        # Note: We can't guarantee exact cleanup due to Ray's internal temp files
        # But we should not have significantly more temp directories
        temp_dirs_after = len(list(Path(tempfile.gettempdir()).glob("tmp*")))
        temp_dirs_created = temp_dirs_after - temp_dirs_before

        # Allow some temp directories for Ray internals, but not excessive
        msg = f"Too many temp directories created: {temp_dirs_created}"
        assert temp_dirs_created < 20, msg

    def test_pipeline_with_multiple_input_files(self, tmp_path):
        """Test Pipeline with multiple input files per row in Ray environment."""
        # Create test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        files_data = []
        for i in range(2):
            file1_path = source_dir / f"input1_{i}.txt"
            file2_path = source_dir / f"input2_{i}.txt"
            file1_path.write_text(f"content1_{i}")
            file2_path.write_text(f"content2_{i}")

            files_data.append(
                {
                    "id": f"row_{i}",
                    "file1.txt": str(file1_path),
                    "file2.txt": str(file2_path),
                }
            )

        # Create DataFrame
        df = pd.DataFrame(files_data)

        # Create Ray dataset
        ray_dataset = ray.data.from_pandas([df])

        # Create pipeline
        dest_dir = tmp_path / "output"
        pipeline = Pipeline(multi_file_processing_function, into_path=dest_dir)

        # Execute pipeline
        result_dataset = ray_dataset.map_batches(pipeline, batch_format="pandas")

        # Collect results
        results = result_dataset.to_pandas()

        # Verify results
        assert len(results) == 2
        assert "id" in results.columns
        assert "output_0.txt" in results.columns
        assert "output_1.txt" in results.columns

        # Verify all output files exist and have correct content
        for i, row in results.iterrows():
            assert row["id"] == f"row_{i}"

            # Check both output files (sorted alphabetically: file1.txt, file2.txt)
            result_path_0 = Path(row["output_0.txt"])
            result_path_1 = Path(row["output_1.txt"])

            assert result_path_0.exists()
            assert result_path_1.exists()

            # output_0.txt should contain processed content from file1.txt
            assert result_path_0.read_text() == f"processed_content1_{i}"
            # output_1.txt should contain processed content from file2.txt
            assert result_path_1.read_text() == f"processed_content2_{i}"
