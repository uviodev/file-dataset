"""Tests for DataFrame support in write_files."""

import logging

import pandas as pd
import pytest

from file_dataset import write_files
from file_dataset.exceptions import FileDatasetError


def test_write_files_dataframe_all_successful(tmp_path):
    """Test writing DataFrame where all rows succeed."""
    # Create test files
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    # Create test files
    (source_dir / "file1.txt").write_text("content1")
    (source_dir / "file2.txt").write_text("content2")
    (source_dir / "file3.txt").write_text("content3")
    (source_dir / "file4.txt").write_text("content4")

    # Create DataFrame with file paths
    df = pd.DataFrame(
        {
            "id": ["row1", "row2"],
            "file_a": [str(source_dir / "file1.txt"), str(source_dir / "file3.txt")],
            "file_b": [str(source_dir / "file2.txt"), str(source_dir / "file4.txt")],
        }
    )

    # Write files
    dest_dir = tmp_path / "dest"
    result_df = write_files(dataframe=df, into_path=dest_dir)

    # Verify result is a DataFrame
    assert isinstance(result_df, pd.DataFrame)

    # Verify result has correct structure
    assert "id" in result_df.columns
    assert "file_a" in result_df.columns
    assert "file_b" in result_df.columns
    assert len(result_df) == 2

    # Verify files were written correctly
    assert (dest_dir / "row1" / "file_a").exists()
    assert (dest_dir / "row1" / "file_b").exists()
    assert (dest_dir / "row2" / "file_a").exists()
    assert (dest_dir / "row2" / "file_b").exists()

    # Verify content
    assert (dest_dir / "row1" / "file_a").read_text() == "content1"
    assert (dest_dir / "row1" / "file_b").read_text() == "content2"
    assert (dest_dir / "row2" / "file_a").read_text() == "content3"
    assert (dest_dir / "row2" / "file_b").read_text() == "content4"


def test_write_files_dataframe_partial_failures(tmp_path, caplog):
    """Test writing DataFrame with some failing rows."""
    # Create test files
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    # Create some test files (but not all)
    (source_dir / "file1.txt").write_text("content1")
    (source_dir / "file2.txt").write_text("content2")
    # file3.txt is missing - will cause row2 to fail
    (source_dir / "file4.txt").write_text("content4")

    # Create DataFrame with file paths
    df = pd.DataFrame(
        {
            "id": ["row1", "row2", "row3"],
            "file_a": [
                str(source_dir / "file1.txt"),
                str(source_dir / "file3.txt"),  # Missing file
                str(source_dir / "file4.txt"),
            ],
            "file_b": [
                str(source_dir / "file2.txt"),
                str(source_dir / "file2.txt"),
                str(source_dir / "file1.txt"),
            ],
        }
    )

    # Write files with logging enabled
    dest_dir = tmp_path / "dest"
    with caplog.at_level(logging.WARNING):
        result_df = write_files(dataframe=df, into_path=dest_dir)

    # Verify result DataFrame
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2  # Only 2 successful rows
    assert list(result_df["id"]) == ["row1", "row3"]

    # Verify files were written for successful rows
    assert (dest_dir / "row1" / "file_a").exists()
    assert (dest_dir / "row1" / "file_b").exists()
    assert not (dest_dir / "row2").exists()  # Failed row directory not created
    assert (dest_dir / "row3" / "file_a").exists()
    assert (dest_dir / "row3" / "file_b").exists()

    # Verify error was logged
    assert "Failed to write files for row row2" in caplog.text
    assert "Source file not found" in caplog.text


def test_write_files_dataframe_all_failed(tmp_path):
    """Test writing DataFrame where all rows fail."""
    # Create DataFrame with non-existent files
    df = pd.DataFrame(
        {
            "id": ["row1", "row2"],
            "file_a": ["/nonexistent/file1.txt", "/nonexistent/file2.txt"],
            "file_b": ["/nonexistent/file3.txt", "/nonexistent/file4.txt"],
        }
    )

    # Write files - should raise error when all fail
    dest_dir = tmp_path / "dest"
    with pytest.raises(FileDatasetError) as exc_info:
        write_files(dataframe=df, into_path=dest_dir)

    assert "All rows failed" in str(exc_info.value)
    assert exc_info.value.file_errors["all_rows"] == "All rows failed to write"


def test_write_files_dataframe_no_id_column(tmp_path):
    """Test writing DataFrame without id column."""
    # Create DataFrame without id column
    df = pd.DataFrame(
        {
            "file_a": ["/some/file.txt"],
            "file_b": ["/some/other.txt"],
        }
    )

    # Should raise ValueError
    dest_dir = tmp_path / "dest"
    with pytest.raises(ValueError, match="DataFrame must have an 'id' column"):
        write_files(dataframe=df, into_path=dest_dir)


def test_write_files_dataframe_empty(tmp_path):
    """Test writing empty DataFrame."""
    # Create empty DataFrame with proper structure
    df = pd.DataFrame(columns=["id", "file_a", "file_b"])

    # Write files
    dest_dir = tmp_path / "dest"
    result_df = write_files(dataframe=df, into_path=dest_dir)

    # Verify result is empty DataFrame with id column
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 0
    assert "id" in result_df.columns


def test_write_files_dataframe_with_none_values(tmp_path):
    """Test writing DataFrame with None values (optional files)."""
    # Create test files
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    (source_dir / "file1.txt").write_text("content1")
    (source_dir / "file2.txt").write_text("content2")

    # Create DataFrame with some None values
    df = pd.DataFrame(
        {
            "id": ["row1", "row2"],
            "file_a": [str(source_dir / "file1.txt"), str(source_dir / "file2.txt")],
            "file_b": [None, str(source_dir / "file1.txt")],  # Optional file
            "file_c": [str(source_dir / "file2.txt"), None],  # Optional file
        }
    )

    # Write files
    dest_dir = tmp_path / "dest"
    result_df = write_files(dataframe=df, into_path=dest_dir)

    # Verify result DataFrame
    assert len(result_df) == 2
    assert result_df.iloc[0]["file_b"] is None
    assert result_df.iloc[1]["file_c"] is None

    # Verify files
    assert (dest_dir / "row1" / "file_a").exists()
    assert not (dest_dir / "row1" / "file_b").exists()  # None value
    assert (dest_dir / "row1" / "file_c").exists()

    assert (dest_dir / "row2" / "file_a").exists()
    assert (dest_dir / "row2" / "file_b").exists()
    assert not (dest_dir / "row2" / "file_c").exists()  # None value


def test_write_files_mutual_exclusivity(tmp_path):
    """Test that row and dataframe arguments are mutually exclusive."""
    # Both provided
    with pytest.raises(ValueError, match="Cannot specify both 'row' and 'dataframe'"):
        write_files(
            row={"file": "/some/file.txt"},
            dataframe=pd.DataFrame({"id": ["test"]}),
            into_path=tmp_path,
            id="test",
        )

    # Neither provided
    with pytest.raises(ValueError, match="Must specify either 'row' or 'dataframe'"):
        write_files(into_path=tmp_path)


def test_write_files_row_requires_id(tmp_path):
    """Test that id is required when using row argument."""
    with pytest.raises(ValueError, match="Must specify 'id' when using 'row' argument"):
        write_files(row={"file": "/some/file.txt"}, into_path=tmp_path)
