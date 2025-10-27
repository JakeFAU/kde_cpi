"""Plotting tools for CPI metrics."""

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter

from ..math import (
    StatSummary,
    compute_statistics,
    weighted_kde_bandwidth,
)
from ..math.utils import normalize_weights, to_numpy


def _axis_limits(
    values: np.ndarray, *, clip: float = 0.995, padding: float = 0.05
) -> tuple[float, float]:
    """Return axis limits that clip extreme tails while preserving most observations."""
    if values.size == 0:
        return (-1.0, 1.0)
    clip = min(max(clip, 0.5), 0.9999)
    tail = (1.0 - clip) / 2.0
    lower = np.quantile(values, tail)
    upper = np.quantile(values, 1.0 - tail)
    data_min = float(values.min())
    data_max = float(values.max())
    # Expand slightly so markers are not glued to the border.
    span = upper - lower
    if span <= 0:
        span = max(abs(lower), abs(upper), 1.0)
        lower -= span * 0.5
        upper += span * 0.5
    else:
        margin = span * padding
        lower -= margin
        upper += margin
    # Ensure we never clip past the actual min/max entirely.
    lower = max(lower, data_min - span)
    upper = min(upper, data_max + span)
    if lower == upper:
        lower -= 1.0
        upper += 1.0
    return float(lower), float(upper)


@dataclass(frozen=True)
class DensityPlotConfig:
    """Styling and annotation options for kernel density plots."""

    title: str = "KDE Mode Inflation"
    xlabel: str = "Percent"
    ylabel: str = "Density"
    color: str = "#d08300"
    rug_alpha: float = 0.3
    line_width: float = 2.5
    statistic_line_style: dict[str, dict[str, object]] | None = None

    def __post_init__(self) -> None:
        """Populate default styles when none are provided."""
        if self.statistic_line_style is None:
            object.__setattr__(
                self,
                "statistic_line_style",
                {
                    "mode": {"color": self.color, "linestyle": "--", "linewidth": 1.5},
                    "mean": {"color": self.color, "linestyle": ":", "linewidth": 1.5},
                    "median": {
                        "color": self.color,
                        "linestyle": "-.",
                        "linewidth": 1.5,
                    },
                },
            )


@dataclass(frozen=True)
class HistogramPlotConfig:
    """Configuration values for weighted histogram plots."""

    title: str = "Weighted Distribution"
    xlabel: str = "Percent"
    ylabel: str = "Weighted frequency"
    bins: int = 30
    color: str = "#126782"
    alpha: float = 0.6


@dataclass(frozen=True)
class PlotReport:
    """Metadata describing a saved plot and its computed statistics."""

    path: Path
    statistics: StatSummary


def _evaluate_kde(
    values: np.ndarray,
    weights: np.ndarray,
    *,
    bandwidth: float | None = None,
    grid_points: int = 2048,
) -> tuple[np.ndarray, np.ndarray]:
    """Evaluate a Gaussian KDE on an evenly spaced grid."""
    if bandwidth is None:
        bandwidth = weighted_kde_bandwidth(values, weights)
    if bandwidth <= 0:
        grid = np.linspace(values.min(), values.max(), grid_points)
        density = np.zeros_like(grid)
        idx = np.argmax(weights)
        density[np.argmin(np.abs(grid - values[idx]))] = 1.0
        return grid, density

    support_min = values.min() - 3.0 * bandwidth
    support_max = values.max() + 3.0 * bandwidth
    grid = np.linspace(support_min, support_max, grid_points)
    diffs = (grid[:, None] - values[None, :]) / bandwidth
    kernel_vals = np.exp(-0.5 * diffs**2) / np.sqrt(2 * np.pi)
    densities = (kernel_vals * weights).sum(axis=1) / bandwidth
    return grid, densities


def generate_density_plot(
    values: Iterable[float],
    weights: Iterable[float],
    *,
    output_dir: str | Path = "out",
    filename: str = "density.png",
    config: DensityPlotConfig | None = None,
) -> PlotReport:
    """Render a kernel density estimate with summary annotations."""
    config = config or DensityPlotConfig()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    vals = to_numpy(values)
    wts = normalize_weights(weights)
    stats = compute_statistics(vals, wts)
    bandwidth = stats.weighted_kde_bandwidth
    grid, densities = _evaluate_kde(vals, wts, bandwidth=bandwidth)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(
        grid,
        densities,
        color=config.color,
        linewidth=config.line_width,
        label="Weighted KDE",
    )
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.set_title(config.title)
    ax.set_xlabel(config.xlabel)
    ax.set_ylabel(config.ylabel)
    xmin, xmax = _axis_limits(vals)
    ax.set_xlim(xmin, xmax)

    # Rug plot
    ax.scatter(
        vals,
        np.zeros_like(vals),
        marker="|",
        color=config.color,
        alpha=config.rug_alpha,
        label="Components (rug)",
    )

    # Annotate statistics
    stat_styles: dict[str, dict[str, Any]] | None = config.statistic_line_style
    if not stat_styles:
        stat_styles = {
            "mode": {"color": config.color, "linestyle": "--", "linewidth": 1.5},
            "mean": {"color": config.color, "linestyle": ":", "linewidth": 1.5},
            "median": {"color": config.color, "linestyle": "-.", "linewidth": 1.5},
        }
    ax.axvline(
        stats.weighted_kde_mode,
        label=f"Mode ≈ {stats.weighted_kde_mode * 100:.2f}%",
        **stat_styles["mode"],
    )
    ax.axvline(
        stats.weighted_mean,
        label=f"Mean ≈ {stats.weighted_mean * 100:.2f}%",
        **stat_styles["mean"],
    )
    ax.axvline(
        stats.weighted_median,
        label=f"Median ≈ {stats.weighted_median * 100:.2f}%",
        **stat_styles["median"],
    )

    ax.legend(loc="upper right")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)
    fig.tight_layout()

    output_path = out_dir / filename
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return PlotReport(path=output_path, statistics=stats)


def generate_histogram_plot(
    values: Iterable[float],
    weights: Iterable[float],
    *,
    output_dir: str | Path = "out",
    filename: str = "histogram.png",
    config: HistogramPlotConfig | None = None,
) -> PlotReport:
    """Render a weighted histogram with key summary markers."""
    config = config or HistogramPlotConfig()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    vals = to_numpy(values)
    wts = normalize_weights(weights)
    stats = compute_statistics(vals, wts)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.hist(
        vals,
        weights=wts,
        bins=config.bins,
        color=config.color,
        alpha=config.alpha,
        edgecolor="white",
    )
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.set_title(config.title)
    ax.set_xlabel(config.xlabel)
    ax.set_ylabel(config.ylabel)
    xmin, xmax = _axis_limits(vals)
    ax.set_xlim(xmin, xmax)

    ax.axvline(
        stats.weighted_mean,
        color="#333333",
        linestyle="--",
        linewidth=1.5,
        label="Weighted mean",
    )
    ax.axvline(
        stats.weighted_kde_mode,
        color="#d08300",
        linestyle=":",
        linewidth=1.5,
        label="Weighted mode",
    )

    ax.legend(loc="upper right")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.6)
    fig.tight_layout()

    output_path = out_dir / filename
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    return PlotReport(path=output_path, statistics=stats)
