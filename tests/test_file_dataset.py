import pytest
from file_dataset import __version__


def test_version():
    """Test that version is defined."""
    assert __version__ is not None


def test_import():
    """Test that the package can be imported."""
    import file_dataset
    assert file_dataset is not None 