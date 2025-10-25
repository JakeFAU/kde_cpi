"""Common helper functions for statistical routines."""

from collections.abc import Iterable
from typing import TypeAlias, cast

import numpy as np
import numpy.typing as npt

FloatArray: TypeAlias = npt.NDArray[np.float64]
NumericInput: TypeAlias = npt.ArrayLike | Iterable[float]


def to_numpy(values: NumericInput) -> FloatArray:
    """Coerce the input sequence into a NumPy float array."""
    return cast(FloatArray, np.asarray(values, dtype=float))


def normalize_weights(weights: NumericInput, *, ensure_positive: bool = True) -> FloatArray:
    """Normalize weights so they sum to one and validate positivity."""
    arr = to_numpy(weights)
    if arr.ndim != 1:
        raise ValueError("Weights must be a 1D array.")
    if ensure_positive and np.any(arr < 0):
        raise ValueError("Weights must be non-negative.")
    total = arr.sum()
    if total <= 0:
        raise ValueError("Weights must sum to a positive value.")
    return cast(FloatArray, arr / total)


def sort_by_values(values: FloatArray, weights: FloatArray) -> tuple[FloatArray, FloatArray]:
    """Return values and weights sorted together by ascending values."""
    order = np.argsort(values)
    return values[order], weights[order]


def cumulative_weights(weights: FloatArray) -> FloatArray:
    """Compute the cumulative sum of weights."""
    return np.cumsum(weights)
