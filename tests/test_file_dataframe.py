"""Test file_dataframe validation functions."""

import pandas as pd
import pytest

from file_dataset.file_dataframe import (
    _get_potential_file_columns,
    _has_file_extension,
    get_file_columns,
    validate_file_dataframe,
    validate_id_column,
    validate_unique_ids,
)


class TestHasFileExtension:
    """Test _has_file_extension helper function."""

    def test_valid_file_extensions(self) -> None:
        """Test column names with valid file extensions."""
        assert _has_file_extension("image.jpg") is True
        assert _has_file_extension("data.csv") is True
        assert _has_file_extension("file.txt") is True
        assert _has_file_extension("config.json") is True
        assert _has_file_extension("archive.tar.gz") is True

    def test_invalid_file_extensions(self) -> None:
        """Test column names without valid file extensions."""
        assert _has_file_extension("id") is False
        assert _has_file_extension("mask") is False
        assert _has_file_extension("output") is False
        assert _has_file_extension("column_name") is False
        assert _has_file_extension("file.") is False
        assert _has_file_extension("file.123") is False


class TestGetPotentialFileColumns:
    """Test _get_potential_file_columns helper function."""

    def test_filters_out_id_column(self) -> None:
        """Test that 'id' column is filtered out."""
        df = pd.DataFrame({"id": [1, 2], "image.jpg": ["url1", "url2"]})
        result = _get_potential_file_columns(df)
        assert result == ["image.jpg"]

    def test_returns_all_non_id_columns(self) -> None:
        """Test that all non-id columns are returned."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "image.jpg": ["url1", "url2"],
                "mask.png": ["url3", "url4"],
                "config": ["url5", "url6"],
            }
        )
        result = _get_potential_file_columns(df)
        assert set(result) == {"image.jpg", "mask.png", "config"}


class TestGetFileColumns:
    """Test get_file_columns function."""

    def test_returns_columns_with_extensions(self) -> None:
        """Test that only columns with file extensions are returned."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "image.jpg": ["url1", "url2"],
                "mask.png": ["url3", "url4"],
                "config": ["url5", "url6"],
            }
        )
        result = get_file_columns(df)
        assert set(result) == {"image.jpg", "mask.png"}

    def test_raises_error_when_no_file_columns(self) -> None:
        """Test that ValueError is raised when no file columns found."""
        df = pd.DataFrame(
            {"id": [1, 2], "config": ["url1", "url2"], "output": ["url3", "url4"]}
        )
        with pytest.raises(ValueError, match="No valid file columns found"):
            get_file_columns(df)

    def test_empty_dataframe_raises_error(self) -> None:
        """Test that empty DataFrame raises error."""
        df = pd.DataFrame({"id": []})
        with pytest.raises(ValueError, match="No valid file columns found"):
            get_file_columns(df)


class TestValidateIdColumn:
    """Test validate_id_column function."""

    def test_valid_id_column(self) -> None:
        """Test that DataFrame with 'id' column passes validation."""
        df = pd.DataFrame({"id": [1, 2], "image.jpg": ["url1", "url2"]})
        validate_id_column(df)  # Should not raise

    def test_missing_id_column_raises_error(self) -> None:
        """Test that missing 'id' column raises ValueError."""
        df = pd.DataFrame({"image.jpg": ["url1", "url2"]})
        with pytest.raises(ValueError, match="DataFrame must have an 'id' column"):
            validate_id_column(df)


class TestValidateUniqueIds:
    """Test validate_unique_ids function."""

    def test_unique_ids_pass_validation(self) -> None:
        """Test that unique IDs pass validation."""
        df = pd.DataFrame({"id": [1, 2, 3], "image.jpg": ["url1", "url2", "url3"]})
        validate_unique_ids(df)  # Should not raise

    def test_duplicate_ids_raise_error(self) -> None:
        """Test that duplicate IDs raise ValueError."""
        df = pd.DataFrame({"id": [1, 2, 2], "image.jpg": ["url1", "url2", "url3"]})
        with pytest.raises(
            ValueError, match="DataFrame 'id' column must have unique values"
        ):
            validate_unique_ids(df)

    def test_empty_dataframe_passes_validation(self) -> None:
        """Test that empty DataFrame passes unique ID validation."""
        df = pd.DataFrame({"id": [], "image.jpg": []})
        validate_unique_ids(df)  # Should not raise


class TestValidateFileDataframe:
    """Test validate_file_dataframe function."""

    def test_valid_dataframe_passes(self) -> None:
        """Test that valid file DataFrame passes all validation."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "image.jpg": ["url1", "url2", "url3"],
                "mask.png": ["url4", "url5", "url6"],
            }
        )
        validate_file_dataframe(df)  # Should not raise

    def test_missing_id_column_fails(self) -> None:
        """Test that missing 'id' column fails validation."""
        df = pd.DataFrame({"image.jpg": ["url1", "url2"]})
        with pytest.raises(ValueError, match="DataFrame must have an 'id' column"):
            validate_file_dataframe(df)

    def test_no_file_columns_fails(self) -> None:
        """Test that DataFrame with no file columns fails validation."""
        df = pd.DataFrame(
            {"id": [1, 2], "config": ["url1", "url2"], "output": ["url3", "url4"]}
        )
        with pytest.raises(ValueError, match="No valid file columns found"):
            validate_file_dataframe(df)

    def test_duplicate_ids_fail(self) -> None:
        """Test that duplicate IDs fail validation."""
        df = pd.DataFrame({"id": [1, 2, 2], "image.jpg": ["url1", "url2", "url3"]})
        with pytest.raises(
            ValueError, match="DataFrame 'id' column must have unique values"
        ):
            validate_file_dataframe(df)
