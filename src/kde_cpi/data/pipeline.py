"""High level orchestration helpers for CPI ingestion."""

from collections.abc import Sequence

import structlog

from .ingest import CpiDatasetBuilder
from .loader import CpiDatabaseLoader
from .models import Dataset

logger = structlog.get_logger(__name__)


async def load_full_history(
    dsn: str,
    *,
    schema: str = "public",
    truncate: bool = True,
    data_files: Sequence[str] | None = None,
) -> Dataset:
    """Load the full CPI history and write it into the database."""
    pipe_log = logger.bind(operation="load_full_history", schema=schema)
    pipe_log.info(
        "pipeline.full_history_start",
        truncate=truncate,
        data_files=list(data_files) if data_files else None,
    )
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
    pipe_log.info(
        "pipeline.full_history_complete",
        series=len(dataset.series),
        observations=len(dataset.observations),
    )
    return dataset


async def update_current_periods(dsn: str, *, schema: str = "public") -> Dataset:
    """Refresh the current-year CPI data without truncating history."""
    pipe_log = logger.bind(operation="update_current", schema=schema)
    pipe_log.info("pipeline.current_start")
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
    pipe_log.info(
        "pipeline.current_complete",
        series=len(dataset.series),
        observations=len(dataset.observations),
    )
    return dataset


__all__ = ["load_full_history", "update_current_periods"]
