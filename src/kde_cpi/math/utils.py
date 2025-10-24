"""Common helper functions for statistical routines."""

import numpy as np


def to_numpy(values) -> np.ndarray:
    """Coerce the input sequence into a NumPy float array."""
    if isinstance(values, np.ndarray):
        return values
    return np.asarray(values, dtype=float)


def normalize_weights(weights, *, ensure_positive: bool = True) -> np.ndarray:
    """Normalize weights so they sum to one and validate positivity."""
    arr = to_numpy(weights)
    if arr.ndim != 1:
        raise ValueError("Weights must be a 1D array.")
    if ensure_positive and np.any(arr < 0):
        raise ValueError("Weights must be non-negative.")
    total = arr.sum()
    if total <= 0:
        raise ValueError("Weights must sum to a positive value.")
    return arr / total


def sort_by_values(values: np.ndarray, weights: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return values and weights sorted together by ascending values."""
    order = np.argsort(values)
    return values[order], weights[order]


def cumulative_weights(weights: np.ndarray) -> np.ndarray:
    """Compute the cumulative sum of weights."""
    return np.cumsum(weights)
