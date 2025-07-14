"""Tests for Ray datasource implementation."""

import pandas as pd
import pytest

pytest.importorskip("ray")
import pyarrow as pa  # noqa: E402
import ray  # noqa: E402
import ray.data  # noqa: E402

import file_dataset  # noqa: E402


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


@pytest.fixture
def sample_file_dataframe(tmp_path):
    """Create sample file DataFrame for testing."""
    # Create test files
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    files_data = []
    for i in range(5):  # Create 5 rows of test data
        file1_path = source_dir / f"file1_{i}.txt"
        file2_path = source_dir / f"file2_{i}.txt"

        # Create small test files (< 1KB as per guidelines)
        file1_path.write_text(f"content1_{i}")
        file2_path.write_text(f"content2_{i}")

        files_data.append(
            {
                "id": f"row_{i}",
                "file1.txt": str(file1_path),
                "file2.txt": str(file2_path),
            }
        )

    return pd.DataFrame(files_data)


@pytest.fixture
def large_file_dataframe(tmp_path):
    """Create larger file DataFrame for memory testing."""
    # Create test files
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    files_data = []
    for i in range(1000):  # 1000 rows for stress testing
        file_path = source_dir / f"file_{i}.txt"

        # Create small test files (each ~100 bytes)
        content = f"content_{i}_" + "x" * 80  # About 100 bytes per file
        file_path.write_text(content)

        files_data.append(
            {
                "id": f"row_{i}",
                "data.txt": str(file_path),
            }
        )

    return pd.DataFrame(files_data)


class TestRayDatasource:
    """Test Ray datasource functionality."""

    def test_read_file_dataset_basic(self, sample_file_dataframe):
        """Test basic read_file_dataset functionality."""
        # Import the ray module
        from file_dataset.ray import read_file_dataset

        # Create Ray dataset
        dataset = read_file_dataset(
            file_dataframe=sample_file_dataframe,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Verify dataset was created
        assert isinstance(dataset, ray.data.Dataset)

        # Collect and verify results
        results = dataset.to_pandas()

        # Should have all 5 rows
        assert len(results) == 5

        # Verify schema matches blob table format
        assert "id" in results.columns
        assert "file1.txt" in results.columns
        assert "file2.txt" in results.columns

        # Verify data types (binary columns should be object/bytes)
        for col in ["file1.txt", "file2.txt"]:
            assert results[col].dtype == object

        # Sort results by id for consistent verification (Ray doesn't preserve order)
        results = results.sort_values("id").reset_index(drop=True)

        # Verify binary content is correct
        for i, row in results.iterrows():
            assert row["id"] == f"row_{i}"
            assert row["file1.txt"] == f"content1_{i}".encode()
            assert row["file2.txt"] == f"content2_{i}".encode()

    def test_read_file_dataset_validation_missing_id(self):
        """Test validation for missing id column."""
        from file_dataset.ray import read_file_dataset

        # Create DataFrame without 'id' column
        df = pd.DataFrame({"file.txt": ["/path/to/file.txt"]})

        with pytest.raises(ValueError, match="DataFrame must have an 'id' column"):
            read_file_dataset(file_dataframe=df, batch_size=1)

    def test_read_file_dataset_validation_duplicate_ids(self):
        """Test validation for duplicate id values."""
        from file_dataset.ray import read_file_dataset

        # Create DataFrame with duplicate ids
        df = pd.DataFrame(
            {
                "id": ["row1", "row1"],  # Duplicate ids
                "file.txt": ["/path/to/file1.txt", "/path/to/file2.txt"],
            }
        )

        with pytest.raises(ValueError, match="'id' column must have unique values"):
            read_file_dataset(file_dataframe=df, batch_size=1)

    def test_read_file_dataset_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        from file_dataset.ray import read_file_dataset

        # Create empty DataFrame with correct schema
        df = pd.DataFrame({"id": [], "file.txt": []})

        dataset = read_file_dataset(file_dataframe=df, batch_size=1)
        results = dataset.to_pandas()

        # Should return empty results
        # Note: Ray may not preserve schema for completely empty datasets
        assert len(results) == 0
        assert isinstance(results, pd.DataFrame)

    def test_read_file_dataset_batching(self, sample_file_dataframe):
        """Test that batching works correctly."""
        from file_dataset.ray import read_file_dataset

        # Use batch size of 2 for 5 rows (should create 3 batches)
        dataset = read_file_dataset(
            file_dataframe=sample_file_dataframe,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Collect results
        results = dataset.to_pandas()

        # Should still have all 5 rows regardless of batching
        assert len(results) == 5

        # Verify all rows are present
        expected_ids = {f"row_{i}" for i in range(5)}
        actual_ids = set(results["id"])
        assert actual_ids == expected_ids

    @pytest.mark.skip(
        reason="S3 mocking with Ray distributed workers requires complex setup"
    )
    def test_read_file_dataset_with_s3_mocking(self, sample_file_dataframe):
        """Test read_file_dataset with S3 URLs using moto.

        Note: This test is skipped because moto's S3 mocking doesn't
        propagate properly to Ray worker processes in the test environment.
        S3 functionality is tested separately in the core module tests.
        """

    def test_read_file_dataset_partial_failures(self, tmp_path):
        """Test handling of partial failures in file loading."""
        from file_dataset.ray import read_file_dataset

        # Create test files (only for successful rows)
        source_dir = tmp_path / "source"
        source_dir.mkdir()

        good_file = source_dir / "good_file.txt"
        good_file.write_text("good_content")

        # Create DataFrame with mixed valid and invalid files
        df = pd.DataFrame(
            [
                {
                    "id": "good_row",
                    "file.txt": str(good_file),
                },
                {
                    "id": "bad_row",
                    "file.txt": "/nonexistent/missing_file.txt",
                },
                {
                    "id": "another_good_row",
                    "file.txt": str(good_file),  # Reuse the good file
                },
            ]
        )

        # Create Ray dataset
        dataset = read_file_dataset(
            file_dataframe=df,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Collect results - should only have successful rows
        results = dataset.to_pandas()

        # Should only have 2 successful rows (bad_row should be dropped)
        assert len(results) == 2

        successful_ids = set(results["id"])
        assert successful_ids == {"good_row", "another_good_row"}

        # Verify content of successful rows
        for _, row in results.iterrows():
            assert row["file.txt"] == b"good_content"

    def test_large_dataset_memory_management(self, large_file_dataframe):
        """Test memory management with pseudo-realistic large dataset."""
        from file_dataset.ray import read_file_dataset

        # Use small batch size to test memory management
        batch_size = 50  # Process 50 rows at a time

        dataset = read_file_dataset(
            file_dataframe=large_file_dataframe,
            batch_size=batch_size,
            options=file_dataset.Options.default(),
        )

        # Process dataset in chunks to avoid loading everything into memory
        chunk_results = []
        for batch in dataset.iter_batches(batch_size=100, batch_format="pandas"):
            chunk_results.append(batch)
            # Verify each chunk has expected structure
            assert "id" in batch.columns
            assert "data.txt" in batch.columns

        # Combine all chunks
        all_results = pd.concat(chunk_results, ignore_index=True)

        # Should have all 1000 rows
        assert len(all_results) == 1000

        # Verify a sample of the data
        sample_row = all_results.iloc[0]
        expected_content = "content_0_" + "x" * 80
        assert sample_row["data.txt"] == expected_content.encode()

    def test_datasource_estimate_memory_size(self, sample_file_dataframe):
        """Test memory size estimation functionality."""
        from file_dataset.ray._datasource import FileDataFrameAsBlobDatasource

        datasource = FileDataFrameAsBlobDatasource(
            file_dataframe=sample_file_dataframe,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Test memory estimation
        estimated_size = datasource.estimate_inmemory_data_size()

        # Should return a reasonable estimate (> 0 for non-empty data)
        assert estimated_size is not None
        assert estimated_size > 0

        # Should be approximately the sum of file sizes for first 2 rows
        # Each file contains ~10 bytes of content
        # 2 rows * 2 files * ~10 bytes = ~40 bytes minimum
        assert estimated_size >= 20  # Allow some flexibility

    def test_datasource_get_read_tasks(self, sample_file_dataframe):
        """Test read task creation."""
        from file_dataset.ray._datasource import FileDataFrameAsBlobDatasource

        datasource = FileDataFrameAsBlobDatasource(
            file_dataframe=sample_file_dataframe,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Get read tasks (parallelism should be ignored)
        tasks = datasource.get_read_tasks(parallelism=10)  # Request 10, should ignore

        # Should create 3 tasks for 5 rows with batch_size=2
        # Tasks: [0:2], [2:4], [4:5]
        assert len(tasks) == 3

        # Verify all tasks are ReadTask objects
        for task in tasks:
            assert isinstance(task, ray.data.ReadTask)

    def test_zero_copy_operations(self, sample_file_dataframe):
        """Test that zero-copy operations work when possible."""
        from file_dataset.ray import read_file_dataset

        # Create Ray dataset
        dataset = read_file_dataset(
            file_dataframe=sample_file_dataframe,
            batch_size=5,  # Single batch
            options=file_dataset.Options.default(),
        )

        # Convert to Arrow table for zero-copy testing
        arrow_batch = next(iter(dataset.iter_batches(batch_format="pyarrow")))

        # Verify Arrow table structure
        assert isinstance(arrow_batch, pa.Table)
        assert "id" in arrow_batch.column_names
        assert "file1.txt" in arrow_batch.column_names
        assert "file2.txt" in arrow_batch.column_names

        # Verify Arrow types
        schema = arrow_batch.schema
        assert schema.field("id").type == pa.string()
        assert schema.field("file1.txt").type == pa.binary()
        assert schema.field("file2.txt").type == pa.binary()

    def test_integration_with_ray_transformations(self, sample_file_dataframe):
        """Test integration with Ray data transformations."""
        from file_dataset.ray import read_file_dataset

        # Create Ray dataset
        dataset = read_file_dataset(
            file_dataframe=sample_file_dataframe,
            batch_size=2,
            options=file_dataset.Options.default(),
        )

        # Apply Ray transformations
        def process_batch(batch: pd.DataFrame) -> pd.DataFrame:
            """Simple transformation function."""
            # Add a processed column based on file1.txt content
            batch["processed"] = batch["file1.txt"].apply(
                lambda x: len(x) if isinstance(x, bytes) else 0
            )
            return batch

        # Apply transformation
        transformed = dataset.map_batches(process_batch, batch_format="pandas")

        # Collect results
        results = transformed.to_pandas()

        # Verify transformation worked
        assert len(results) == 5
        assert "processed" in results.columns

        # Sort results by id for consistent verification (Ray doesn't preserve order)
        results = results.sort_values("id").reset_index(drop=True)

        # Verify processed column contains file lengths
        for i, row in results.iterrows():
            expected_length = len(f"content1_{i}".encode())
            assert row["processed"] == expected_length
