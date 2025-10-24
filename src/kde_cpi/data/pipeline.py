"""High level orchestration helpers for CPI ingestion."""

from collections.abc import Sequence

from .ingest import CpiDatasetBuilder
from .loader import CpiDatabaseLoader
from .models import Dataset


async def load_full_history(
    dsn: str,
    *,
    schema: str = "public",
    truncate: bool = True,
    data_files: Sequence[str] | None = None,
) -> Dataset:
    """Load the full CPI history and write it into the database."""
    builder = CpiDatasetBuilder()
    try:
        dataset = builder.load_dataset(data_files=data_files)
    finally:
        builder.close()

    loader = CpiDatabaseLoader(dsn=dsn, schema=schema)
    try:
        await loader.bulk_load(dataset, truncate=truncate)
    finally:
        await loader.close()
    return dataset


async def update_current_periods(dsn: str, *, schema: str = "public") -> Dataset:
    """Refresh the current-year CPI data without truncating history."""
    builder = CpiDatasetBuilder()
    try:
        dataset = builder.load_current_observations()
    finally:
        builder.close()

    loader = CpiDatabaseLoader(dsn=dsn, schema=schema)
    try:
        await loader.merge_dataset(dataset)
    finally:
        await loader.close()
    return dataset


__all__ = ["load_full_history", "update_current_periods"]
