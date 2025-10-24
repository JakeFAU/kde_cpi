"""Weighted statistics helpers for CPI data."""

import math
from dataclasses import dataclass

import numpy as np
from numpy.typing import ArrayLike

from .utils import normalize_weights, sort_by_values, to_numpy


def _validate_inputs(
    values: ArrayLike,
    weights: ArrayLike,
) -> tuple[np.ndarray, np.ndarray]:
    """Normalize inputs to aligned 1D arrays and validate shapes."""
    vals = to_numpy(values)
    wts = normalize_weights(weights)
    if vals.ndim != 1:
        raise ValueError("Values must be a 1D sequence.")
    if vals.shape != wts.shape:
        raise ValueError("Values and weights must share the same shape.")
    return vals, wts


def weighted_mean(values: ArrayLike, weights: ArrayLike) -> float:
    """Compute the weighted arithmetic mean."""
    vals, wts = _validate_inputs(values, weights)
    return float(np.dot(vals, wts))


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, quantile: float) -> float:
    """Return the weighted quantile using the cumulative weight approach."""
    sorted_vals, sorted_wts = sort_by_values(values, weights)
    cum = np.cumsum(sorted_wts)
    quantile = float(np.clip(quantile, 0.0, 1.0))
    return float(np.interp(quantile, cum, sorted_vals, left=sorted_vals[0], right=sorted_vals[-1]))


def weighted_median(values: ArrayLike, weights: ArrayLike) -> float:
    """Return the weighted 50th percentile."""
    vals, wts = _validate_inputs(values, weights)
    return _weighted_quantile(vals, wts, 0.5)


def weighted_trimmed_mean(values: ArrayLike, weights: ArrayLike, trim: float = 0.08) -> float:
    """Compute a symmetric trimmed mean with weight-aware clipping."""
    if not 0 <= trim < 0.5:
        raise ValueError("trim must lie in the interval [0, 0.5).")
    vals, wts = _validate_inputs(values, weights)
    sorted_vals, sorted_wts = sort_by_values(vals, wts)
    cum = np.concatenate(([0.0], np.cumsum(sorted_wts)))
    lower = trim
    upper = 1.0 - trim

    clipped_low = np.clip(cum[:-1], lower, upper)
    clipped_high = np.clip(cum[1:], lower, upper)
    trimmed_wts = clipped_high - clipped_low
    mask = trimmed_wts > 0
    if not np.any(mask):
        return weighted_mean(sorted_vals, sorted_wts)

    trimmed_vals = sorted_vals[mask]
    trimmed_weights = trimmed_wts[mask]
    trimmed_weights /= trimmed_weights.sum()
    return float(np.dot(trimmed_vals, trimmed_weights))


def weighted_variance(values: ArrayLike, weights: ArrayLike) -> float:
    """Return the weighted population variance."""
    vals, wts = _validate_inputs(values, weights)
    mean = weighted_mean(vals, wts)
    return float(np.dot(wts, (vals - mean) ** 2))


def weighted_std(values: ArrayLike, weights: ArrayLike) -> float:
    """Return the weighted population standard deviation."""
    variance = weighted_variance(values, weights)
    return float(math.sqrt(max(variance, 0.0)))


def weighted_skewness(values: ArrayLike, weights: ArrayLike) -> float:
    """Return the weighted third standardized moment."""
    vals, wts = _validate_inputs(values, weights)
    mean = weighted_mean(vals, wts)
    centered = vals - mean
    variance = np.dot(wts, centered**2)
    if variance <= 0:
        return 0.0
    std = math.sqrt(variance)
    skew = np.dot(wts, (centered / std) ** 3)
    return float(skew)


def weighted_kurtosis(values: ArrayLike, weights: ArrayLike, fisher: bool = True) -> float:
    """Return the weighted fourth standardized moment (Fisher by default)."""
    vals, wts = _validate_inputs(values, weights)
    mean = weighted_mean(vals, wts)
    centered = vals - mean
    variance = np.dot(wts, centered**2)
    if variance <= 0:
        return 0.0
    std = math.sqrt(variance)
    kurt = np.dot(wts, (centered / std) ** 4)
    if fisher:
        kurt -= 3.0
    return float(kurt)


def effective_sample_size(weights: ArrayLike) -> float:
    """Estimate the effective sample size implied by the weights."""
    wts = normalize_weights(weights)
    return float(1.0 / np.sum(wts**2))


def weighted_kde_bandwidth(values: ArrayLike, weights: ArrayLike) -> float:
    """Scott's rule of thumb generalized for weighted samples."""
    vals, wts = _validate_inputs(values, weights)
    std = weighted_std(vals, wts)
    q25 = _weighted_quantile(vals, wts, 0.25)
    q75 = _weighted_quantile(vals, wts, 0.75)
    iqr = q75 - q25
    scale = std if iqr <= 0 else min(std, iqr / 1.34)
    if scale <= 0:
        return 0.0
    ess = effective_sample_size(wts)
    return float(0.9 * scale * ess ** (-1.0 / 5.0))


def _gaussian_kernel(u: np.ndarray) -> np.ndarray:
    """Evaluate the standard Gaussian kernel."""
    return np.exp(-0.5 * u * u) / math.sqrt(2.0 * math.pi)


def weighted_kde_mode(
    values: ArrayLike,
    weights: ArrayLike,
    bandwidth: float | None = None,
    *,
    grid_points: int = 2048,
    extend: float = 3.0,
) -> float:
    """Locate the mode of a weighted kernel density estimate."""
    vals, wts = _validate_inputs(values, weights)
    if vals.size == 0:
        raise ValueError("values must contain at least one element.")
    if bandwidth is None:
        bandwidth = weighted_kde_bandwidth(vals, wts)
    if bandwidth <= 0:
        return float(vals[np.argmax(wts)])

    support_min = vals.min() - extend * bandwidth
    support_max = vals.max() + extend * bandwidth
    grid = np.linspace(support_min, support_max, grid_points)

    diffs = (grid[:, None] - vals[None, :]) / bandwidth
    kernel_vals = _gaussian_kernel(diffs)
    densities = (kernel_vals * wts).sum(axis=1) / bandwidth

    mode_index = int(np.argmax(densities))
    return float(grid[mode_index])


@dataclass(frozen=True)
class StatSummary:
    """Bundle of weighted descriptive statistics for CPI components."""

    weighted_mean: float
    weighted_median: float
    trimmed_mean: float
    weighted_std: float
    weighted_skewness: float
    weighted_kurtosis: float
    weighted_kde_bandwidth: float
    weighted_kde_mode: float
    effective_sample_size: float


def compute_statistics(
    values: ArrayLike,
    weights: ArrayLike,
    *,
    trim: float = 0.08,
    kde_bandwidth: float | None = None,
    grid_points: int = 2048,
) -> StatSummary:
    """Compute a consistent set of weighted statistics for CPI components."""
    vals, wts = _validate_inputs(values, weights)
    mean = weighted_mean(vals, wts)
    median = weighted_median(vals, wts)
    trimmed = weighted_trimmed_mean(vals, wts, trim=trim)
    std = weighted_std(vals, wts)
    skew = weighted_skewness(vals, wts)
    kurt = weighted_kurtosis(vals, wts)
    bandwidth = kde_bandwidth or weighted_kde_bandwidth(vals, wts)
    mode = weighted_kde_mode(vals, wts, bandwidth=bandwidth, grid_points=grid_points)
    ess = effective_sample_size(wts)
    return StatSummary(
        weighted_mean=mean,
        weighted_median=median,
        trimmed_mean=trimmed,
        weighted_std=std,
        weighted_skewness=skew,
        weighted_kurtosis=kurt,
        weighted_kde_bandwidth=bandwidth,
        weighted_kde_mode=mode,
        effective_sample_size=ess,
    )
