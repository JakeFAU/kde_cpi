"""Unit tests for the math module."""

import numpy as np
import pytest

from kde_cpi.math import compute_statistics


@pytest.fixture
def sample_data():
    """Return a sample dataset for testing."""
    return np.array([1, 2, 3, 4, 5])


@pytest.fixture
def sample_weights():
    """Return sample weights for testing."""
    return np.array([1, 1, 1, 1, 1])


def test_compute_statistics(sample_data, sample_weights):
    """Test the compute_statistics function."""
    stats = compute_statistics(sample_data, sample_weights)

    assert stats.weighted_mean == pytest.approx(3.0)
    assert stats.weighted_median == pytest.approx(3.0, abs=0.6)
    assert stats.trimmed_mean == pytest.approx(3.0)
    assert stats.weighted_std == pytest.approx(np.sqrt(2.0))
    assert stats.weighted_skewness == pytest.approx(0.0)
    assert stats.weighted_kurtosis == pytest.approx(-1.3, abs=1e-9)
