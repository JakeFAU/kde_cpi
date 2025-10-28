"""Unit tests for the math utilities."""
import numpy as np
import pytest

from kde_cpi.math.utils import (
    cumulative_weights,
    normalize_weights,
    sort_by_values,
    to_numpy,
)


def test_to_numpy():
    """Test that to_numpy converts various inputs to a numpy array."""
    assert isinstance(to_numpy([1, 2, 3]), np.ndarray)
    assert isinstance(to_numpy((1, 2, 3)), np.ndarray)
    assert isinstance(to_numpy(np.array([1, 2, 3])), np.ndarray)


def test_normalize_weights():
    """Test that normalize_weights correctly normalizes and validates weights."""
    weights = np.array([1, 2, 3])
    normalized = normalize_weights(weights)
    assert np.isclose(np.sum(normalized), 1.0)

    with pytest.raises(ValueError, match="Weights must be a 1D array."):
        normalize_weights(np.array([[1, 2], [3, 4]]))

    with pytest.raises(ValueError, match="Weights must be non-negative."):
        normalize_weights(np.array([-1, 2, 3]))

    with pytest.raises(ValueError, match="Weights must sum to a positive value."):
        normalize_weights(np.array([0, 0, 0]))


def test_sort_by_values():
    """Test that sort_by_values correctly sorts values and weights."""
    values = np.array([3, 1, 2])
    weights = np.array([10, 20, 30])
    sorted_values, sorted_weights = sort_by_values(values, weights)
    assert np.array_equal(sorted_values, np.array([1, 2, 3]))
    assert np.array_equal(sorted_weights, np.array([20, 30, 10]))


def test_cumulative_weights():
    """Test that cumulative_weights correctly computes the cumulative sum."""
    weights = np.array([1, 2, 3, 4])
    cumulative = cumulative_weights(weights)
    assert np.array_equal(cumulative, np.array([1, 3, 6, 10]))
