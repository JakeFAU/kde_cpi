"""Unit tests for the output utilities."""

import numpy as np

from kde_cpi.output.utils import ensure_directory, format_percent, to_numpy


def test_to_numpy():
    """Test that the to_numpy function correctly converts iterables to numpy arrays."""
    assert isinstance(to_numpy([1, 2, 3]), np.ndarray)
    assert isinstance(to_numpy((1, 2, 3)), np.ndarray)
    assert isinstance(to_numpy(np.array([1, 2, 3])), np.ndarray)


def test_ensure_directory(tmp_path):
    """Test that the ensure_directory function correctly creates a directory."""
    new_dir = tmp_path / "new_dir"
    assert not new_dir.exists()
    ensure_directory(new_dir)
    assert new_dir.exists()
    ensure_directory(new_dir)  # Should not raise an error
    assert new_dir.exists()


def test_format_percent():
    """Test that the format_percent function correctly formats a float as a percentage string."""
    assert format_percent(0.12345) == "12.35%"
    assert format_percent(1.0) == "100.00%"
    assert format_percent(0.0) == "0.00%"
