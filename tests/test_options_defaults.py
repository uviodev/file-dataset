"""Test that Options default behavior works correctly."""

import contextlib
from unittest.mock import patch

import pytest

import file_dataset
from file_dataset.exceptions import FileDatasetError


def test_write_files_s3_without_options_uses_defaults():
    """Test that write_files with S3 path uses default options when None provided."""
    # This should now use default options instead of failing
    # We expect it to fail later due to invalid file, but not due to missing options
    with pytest.raises(FileDatasetError):  # Will fail due to missing source file
        file_dataset.write_files(
            row={"test.txt": "nonexistent_file.txt"},
            into_path="s3://test-bucket/folder",
            id="test-id",
        )


def test_reader_s3_without_options_uses_defaults():
    """Test that reader with S3 URLs uses default options when None provided."""
    # This should now use default options and fail later due to network/auth issues
    # rather than failing due to missing options
    reader = file_dataset.row_reader({"test.txt": "s3://test-bucket/nonexistent.txt"})

    # The validation should now use default options but fail due to network/auth
    with pytest.raises(FileDatasetError) as exc_info:
        with reader.into_temp_dir():
            pass

    # Should not mention "Options required" anymore
    assert not any(
        "Options required" in error for error in exc_info.value.file_errors.values()
    )


def test_explicit_options_override_defaults():
    """Test that explicitly provided options override defaults."""
    from file_dataset import Options

    # Create custom options
    custom_options = Options.default()

    # Should use the provided options, not defaults
    reader = file_dataset.row_reader(
        {"test.txt": "s3://bucket/file.txt"}, options=custom_options
    )

    # The reader should use our custom options instance
    assert reader.options is custom_options


def test_dataframe_reader_uses_defaults():
    """Test that FileDataFrameReader also uses default options."""
    import pandas as pd

    # Create DataFrame with S3 URLs
    df = pd.DataFrame({"id": ["row1"], "file": ["s3://bucket/file.txt"]})

    # Should use default options for S3 operations
    reader = file_dataset.file_dataframe_reader(df)

    # When processing fails due to network/auth, not missing options
    with pytest.raises(FileDatasetError) as exc_info:
        list(reader.into_temp_dir())

    # Should not mention "Options required"
    error_messages = [str(exc_info.value)]
    assert not any("Options required" in msg for msg in error_messages)


def test_size_table_uses_defaults():
    """Test that into_size_table uses default options."""
    import pandas as pd

    df = pd.DataFrame({"id": ["row1"], "file": ["s3://bucket/file.txt"]})

    reader = file_dataset.file_dataframe_reader(df)

    # Should fail due to network/auth, not missing options
    with pytest.raises(FileDatasetError) as exc_info:
        reader.into_size_table()

    # Should not mention "Options required"
    error_messages = [str(exc_info.value)]
    assert not any("Options required" in msg for msg in error_messages)


def test_blob_table_uses_defaults():
    """Test that into_blob_table uses default options."""
    import pandas as pd

    df = pd.DataFrame({"id": ["row1"], "file": ["s3://bucket/file.txt"]})

    reader = file_dataset.file_dataframe_reader(df)

    # Should fail due to network/auth, not missing options
    with pytest.raises(FileDatasetError) as exc_info:
        reader.into_blob_table()

    # Should not mention "Options required"
    error_messages = [str(exc_info.value)]
    assert not any("Options required" in msg for msg in error_messages)


def test_options_initialized_eagerly_and_shared():
    """Test that options are initialized once in __init__ and shared."""
    from unittest.mock import MagicMock

    import pandas as pd

    # Mock Options.default() to track calls
    mock_options = MagicMock()

    with patch(
        "file_dataset._reader.Options.default", return_value=mock_options
    ) as mock_default:
        # Create DataFrame with multiple S3 files
        df = pd.DataFrame(
            {
                "id": ["row1", "row2"],
                "file1": ["s3://bucket/file1.txt", "s3://bucket/file3.txt"],
                "file2": ["s3://bucket/file2.txt", "s3://bucket/file4.txt"],
            }
        )

        # Create reader - should call Options.default() once
        reader = file_dataset.file_dataframe_reader(df)

        # Verify Options.default() was called exactly once during initialization
        assert mock_default.call_count == 1

        # Verify the reader has the mocked options
        assert reader.options is mock_options

        # Reset call count for the next test
        mock_default.reset_mock()

        # Multiple operations should not create new options
        with contextlib.suppress(Exception):
            # These would normally fail due to network, but should not call
            # Options.default() again
            reader.into_size_table()

        with contextlib.suppress(Exception):
            reader.into_blob_table()

        # Options.default() should not have been called again
        assert mock_default.call_count == 0


def test_filerowreader_options_initialized_eagerly():
    """Test that FileRowReader initializes options eagerly."""
    from unittest.mock import MagicMock

    mock_options = MagicMock()

    with patch(
        "file_dataset._reader.Options.default", return_value=mock_options
    ) as mock_default:
        # Create reader with S3 files
        reader = file_dataset.row_reader({"file1.txt": "s3://bucket/file1.txt"})

        # Verify Options.default() was called exactly once
        assert mock_default.call_count == 1
        assert reader.options is mock_options
