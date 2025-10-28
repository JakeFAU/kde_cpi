"""Unit tests for the plotting tools."""
from pathlib import Path

import numpy as np
import pytest
from matplotlib import pyplot as plt

from kde_cpi.output.plots import (
    DensityPlotConfig,
    HistogramPlotConfig,
    _axis_limits,
    _evaluate_kde,
    generate_density_plot,
    generate_histogram_plot,
)


@pytest.fixture
def mock_plt(mocker):
    """Fixture for a mock matplotlib.pyplot."""
    fig_mock = mocker.MagicMock()
    ax_mock = mocker.MagicMock()
    subplots_mock = mocker.patch("matplotlib.pyplot.subplots", return_value=(fig_mock, ax_mock))
    mocker.patch("matplotlib.pyplot.close")
    return subplots_mock, fig_mock, ax_mock


def test_generate_density_plot(mock_plt, tmp_path):
    """Test that the density plot is generated with the correct parameters."""
    values = np.array([1, 2, 3, 4, 5])
    weights = np.array([1, 1, 1, 1, 1])
    config = DensityPlotConfig(title="Test Density Plot")

    subplots_mock, fig_mock, ax_mock = mock_plt
    report = generate_density_plot(values, weights, output_dir=tmp_path, config=config)

    assert report.path == tmp_path / "density.png"
    assert hasattr(report.statistics, "weighted_mean")
    ax_mock.set_title.assert_called_with("Test Density Plot")
    fig_mock.savefig.assert_called_once()


def test_generate_histogram_plot(mock_plt, tmp_path):
    """Test that the histogram plot is generated with the correct parameters."""
    values = np.array([1, 2, 3, 4, 5])
    weights = np.array([1, 1, 1, 1, 1])
    config = HistogramPlotConfig(title="Test Histogram Plot")

    subplots_mock, fig_mock, ax_mock = mock_plt
    report = generate_histogram_plot(values, weights, output_dir=tmp_path, config=config)

    assert report.path == tmp_path / "histogram.png"
    assert hasattr(report.statistics, "weighted_mean")
    ax_mock.set_title.assert_called_with("Test Histogram Plot")
    fig_mock.savefig.assert_called_once()


def test_axis_limits():
    """Test that the axis limits are calculated correctly."""
    values = np.array([-10, -5, 0, 5, 10])
    lower, upper = _axis_limits(values)
    assert lower < -10
    assert upper > 10

    values_empty = np.array([])
    lower_empty, upper_empty = _axis_limits(values_empty)
    assert lower_empty == -1.0
    assert upper_empty == 1.0


def test_evaluate_kde():
    """Test that the KDE is evaluated correctly."""
    values = np.array([0, 0, 10, 10])
    weights = np.array([1, 1, 1, 1])
    grid, densities = _evaluate_kde(values, weights)
    assert grid.shape == densities.shape
    assert np.all(densities >= 0)

    # Test with zero bandwidth
    grid_zero_bw, densities_zero_bw = _evaluate_kde(values, weights, bandwidth=0)
    assert np.sum(densities_zero_bw > 0) == 1
