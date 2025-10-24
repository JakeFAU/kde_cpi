"""Shared helpers for CPI plotting."""

from collections.abc import Iterable
from pathlib import Path

import numpy as np


def to_numpy(values: Iterable[float]) -> np.ndarray:
    """Return the input values as a 1D NumPy float array."""
    if isinstance(values, np.ndarray):
        return values
    return np.asarray(list(values), dtype=float)


def ensure_directory(path: str | Path) -> Path:
    """Create the directory at ``path`` if needed and return its Path."""
    directory = Path(path)
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
    return directory


def format_percent(value: float) -> str:
    """Format a value expressed in fractions as a percentage string."""
    return f"{value * 100:.2f}%"
