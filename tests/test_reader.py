"""Tests for file dataset reader functionality."""

from pathlib import Path

import pytest

from file_dataset import FileDatasetError, row_reader


class TestReader:
    """Test cases for the reader functionality."""

    def test_row_reader_single_file_copy(self):
        """Test row_reader copying a single file to temp directory."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        with row_reader(files_dict).into_temp_dir() as temp_dir:
            # Check temp directory exists and contains the file
            assert temp_dir.exists()
            assert temp_dir.is_dir()

            copied_file = temp_dir / "image.txt"
            assert copied_file.exists()
            assert copied_file.is_file()

            # Check file content is preserved
            original_content = test_file.read_text()
            copied_content = copied_file.read_text()
            assert original_content == copied_content

    def test_row_reader_no_table_methods(self):
        """Test that FileRowReader doesn't have table methods."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        file_row_reader = row_reader(files_dict)

        # These methods should not exist on FileRowReader anymore
        assert not hasattr(file_row_reader, "into_size_table")
        assert not hasattr(file_row_reader, "into_blob_table")
        # FileRowReader should not have into_temp_dirs (only FileDataFrameReader should)
        assert not hasattr(file_row_reader, "into_temp_dirs")

    def test_single_file_copy(self):
        """Test copying a single file to temp directory."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        with row_reader(files_dict).into_temp_dir() as temp_dir:
            # Check temp directory exists and contains the file
            assert temp_dir.exists()
            assert temp_dir.is_dir()

            copied_file = temp_dir / "image.txt"
            assert copied_file.exists()
            assert copied_file.is_file()

            # Check file content is preserved
            original_content = test_file.read_text()
            copied_content = copied_file.read_text()
            assert original_content == copied_content

    def test_multiple_files_copy(self):
        """Test copying multiple files to temp directory."""
        test_data_dir = Path(__file__).parent / "data"
        files_dict = {
            "image.txt": test_data_dir / "test_image.txt",
            "mask.txt": test_data_dir / "test_mask.txt",
            "config.json": test_data_dir / "test_config.json",
        }

        with row_reader(files_dict).into_temp_dir() as temp_dir:
            # Check all files exist in temp directory
            for filename in files_dict:
                copied_file = temp_dir / filename
                assert copied_file.exists()
                assert copied_file.is_file()

            # Verify content preservation for one file
            original_content = (test_data_dir / "test_image.txt").read_text()
            copied_content = (temp_dir / "image.txt").read_text()
            assert original_content == copied_content

    def test_missing_file_error(self):
        """Test that missing files raise FileDatasetError."""
        files_dict = {"missing.txt": "/nonexistent/path/missing.txt"}

        with (
            pytest.raises(FileDatasetError) as exc_info,
            row_reader(files_dict).into_temp_dir(),
        ):
            pass  # Should not reach this point

        # Check error contains information about missing file
        error = exc_info.value
        assert "missing.txt" in error.file_errors
        assert "not found" in error.file_errors["missing.txt"].lower()

    def test_context_manager_cleanup(self):
        """Test that temp directory is cleaned up after context exit."""
        test_file = Path(__file__).parent / "data" / "test_image.txt"
        files_dict = {"image.txt": test_file}

        temp_dir_path = None
        with row_reader(files_dict).into_temp_dir() as temp_dir:
            temp_dir_path = temp_dir
            assert temp_dir.exists()

        # After context exit, temp directory should be cleaned up
        assert not temp_dir_path.exists()

    def test_empty_files_dict(self):
        """Test handling of empty files dictionary."""
        files_dict = {}

        with row_reader(files_dict).into_temp_dir() as temp_dir:
            assert temp_dir.exists()
            assert temp_dir.is_dir()
            # Directory should be empty
            assert list(temp_dir.iterdir()) == []
