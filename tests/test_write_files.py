"""Tests for file dataset write_files functionality."""

import tempfile
from pathlib import Path

import pytest

from file_dataset import FileDatasetError, write_files


class TestWriteFiles:
    """Test cases for the write_files functionality."""

    # Test constants
    EXPECTED_MULTIPLE_FILES_COUNT = 3
    EXPECTED_OPTIONAL_FILES_COUNT = 3
    EXPECTED_ALL_NONE_COUNT = 2

    def test_single_file_write(self):
        """Test writing a single file with ID-based directory structure."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        with tempfile.TemporaryDirectory() as temp_dir:
            result = write_files(row=files_dict, into_path=temp_dir, id="test_001")

            # Check result format
            assert len(result) == 1
            assert "image.txt" in result
            assert isinstance(result["image.txt"], Path)

            # Check directory structure
            expected_dir = Path(temp_dir) / "test_001"
            assert expected_dir.exists()
            assert expected_dir.is_dir()

            # Check file exists and content is preserved
            written_file = expected_dir / "image.txt"
            assert written_file.exists()
            assert written_file.is_file()

            original_content = test_file.read_text()
            written_content = written_file.read_text()
            assert original_content == written_content

            # Check result points to correct file
            assert result["image.txt"] == written_file

    def test_multiple_files_write(self):
        """Test writing multiple files to ID-based directory."""
        test_data_dir = Path(__file__).parent / "data"
        files_dict = {
            "image.txt": test_data_dir / "test_image.txt",
            "mask.txt": test_data_dir / "test_mask.txt",
            "config.json": test_data_dir / "test_config.json",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            result = write_files(row=files_dict, into_path=temp_dir, id="multi_001")

            # Check result format
            assert len(result) == self.EXPECTED_MULTIPLE_FILES_COUNT
            expected_dir = Path(temp_dir) / "multi_001"

            for filename in files_dict:
                assert filename in result
                assert isinstance(result[filename], Path)

                written_file = expected_dir / filename
                assert written_file.exists()
                assert written_file.is_file()
                assert result[filename] == written_file

            # Verify content preservation for one file
            original_content = (test_data_dir / "test_image.txt").read_text()
            written_content = (expected_dir / "image.txt").read_text()
            assert original_content == written_content

    def test_missing_file_error(self):
        """Test that missing files raise FileDatasetError."""
        files_dict = {"missing.txt": "/nonexistent/path/missing.txt"}

        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(FileDatasetError) as exc_info:
                write_files(row=files_dict, into_path=temp_dir, id="error_001")

            # Check error contains information about missing file
            error = exc_info.value
            assert "missing.txt" in error.file_errors
            assert "not found" in error.file_errors["missing.txt"].lower()

            # Verify no directory was created
            expected_dir = Path(temp_dir) / "error_001"
            assert not expected_dir.exists()

    def test_none_value_handling(self):
        """Test that None values are handled correctly as optional files."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {
            "image.txt": test_file,
            "optional_mask.txt": None,
            "another_optional.txt": None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            result = write_files(row=files_dict, into_path=temp_dir, id="optional_001")

            # Check result format - None values should be preserved
            assert len(result) == self.EXPECTED_OPTIONAL_FILES_COUNT
            assert result["image.txt"] is not None
            assert result["optional_mask.txt"] is None
            assert result["another_optional.txt"] is None

            # Check only non-None file was written
            expected_dir = Path(temp_dir) / "optional_001"
            assert expected_dir.exists()

            written_files = list(expected_dir.iterdir())
            assert len(written_files) == 1
            assert written_files[0].name == "image.txt"

    def test_empty_files_dict(self):
        """Test handling of empty files dictionary."""
        files_dict = {}

        with tempfile.TemporaryDirectory() as temp_dir:
            result = write_files(row=files_dict, into_path=temp_dir, id="empty_001")

            # Should return empty dict
            assert result == {}

            # No directory should be created for empty input
            expected_dir = Path(temp_dir) / "empty_001"
            assert not expected_dir.exists()

    def test_all_none_values(self):
        """Test handling when all files are None (optional)."""
        files_dict = {
            "optional1.txt": None,
            "optional2.txt": None,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            result = write_files(row=files_dict, into_path=temp_dir, id="all_none_001")

            # Should return dict with None values
            assert len(result) == self.EXPECTED_ALL_NONE_COUNT
            assert result["optional1.txt"] is None
            assert result["optional2.txt"] is None

            # Directory should be created but empty
            expected_dir = Path(temp_dir) / "all_none_001"
            assert expected_dir.exists()
            assert list(expected_dir.iterdir()) == []

    def test_mixed_existing_and_missing_files(self):
        """Test atomic behavior - if any required file fails, no files are written."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {
            "good_file.txt": test_file,
            "missing_file.txt": "/nonexistent/path/missing.txt",
            "optional_file.txt": None,  # This should not cause failure
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(FileDatasetError) as exc_info:
                write_files(row=files_dict, into_path=temp_dir, id="atomic_001")

            # Check error mentions the missing file
            error = exc_info.value
            assert "missing_file.txt" in error.file_errors

            # Verify no files were written (atomic failure)
            expected_dir = Path(temp_dir) / "atomic_001"
            assert not expected_dir.exists()

    def test_directory_creation_nested_path(self):
        """Test that nested directory paths are created correctly."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = Path(temp_dir) / "level1" / "level2" / "level3"

            result = write_files(row=files_dict, into_path=nested_path, id="nested_001")

            # Check nested directory structure was created
            expected_dir = nested_path / "nested_001"
            assert expected_dir.exists()
            assert expected_dir.is_dir()

            # Check file was written
            written_file = expected_dir / "image.txt"
            assert written_file.exists()
            assert result["image.txt"] == written_file
