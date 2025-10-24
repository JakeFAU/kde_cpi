"""Series-level utilities for CPI reporting."""

from .views import SeriesViewManager, build_series_view_sql

__all__ = ["SeriesViewManager", "build_series_view_sql"]
