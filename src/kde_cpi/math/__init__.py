"""Statistical utilities for CPI analysis."""

from .stats import (  # noqa: F401
    StatSummary,
    compute_statistics,
    effective_sample_size,
    weighted_kde_bandwidth,
    weighted_kde_mode,
    weighted_kurtosis,
    weighted_mean,
    weighted_median,
    weighted_skewness,
    weighted_std,
    weighted_trimmed_mean,
)

__all__ = [
    "StatSummary",
    "compute_statistics",
    "effective_sample_size",
    "weighted_mean",
    "weighted_median",
    "weighted_trimmed_mean",
    "weighted_std",
    "weighted_skewness",
    "weighted_kurtosis",
    "weighted_kde_bandwidth",
    "weighted_kde_mode",
]
