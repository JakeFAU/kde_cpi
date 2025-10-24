"""Command line entry point for the kde-cpi application."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

import click

from kde_cpi.data import (
    CpiDatabaseLoader,
    CpiDatasetBuilder,
    Dataset,
    load_full_history,
    update_current_periods,
)

DSN_HELP = "PostgreSQL connection string. May also be set via the KDE_CPI_DSN env var."
SCHEMA_HELP = (
    "Target database schema for CPI tables. May also be set via the KDE_CPI_SCHEMA env var."
)
DATA_FILE_HELP = "One or more specific CPI data partitions to ingest (e.g. cu.data.0.Current)."


def _require_dsn(ctx: click.Context, override: str | None) -> str:
    """Return the resolved DSN, raising when none is provided."""
    ctx.ensure_object(dict)
    dsn = override or ctx.obj.get("dsn")
    if not dsn:
        raise click.UsageError("A PostgreSQL DSN is required; pass --dsn or set KDE_CPI_DSN.")
    return dsn


def _resolve_schema(ctx: click.Context, override: str | None) -> str:
    """Return the schema name, defaulting to the contextual configuration."""
    ctx.ensure_object(dict)
    schema = override or ctx.obj.get("schema") or "public"
    return schema


def _build_dataset(*, current_only: bool, data_files: Sequence[str] | None) -> Dataset:
    """Load CPI data using the shared dataset builder."""
    builder = CpiDatasetBuilder()
    try:
        if current_only:
            return builder.load_current_observations()
        return builder.load_dataset(data_files=data_files)
    finally:
        builder.close()


def _echo_dataset_summary(action: str, dataset: Dataset) -> None:
    """Emit a concise data volume summary for terminal feedback."""
    click.echo(
        f"{action}: {len(dataset.series)} series, "
        f"{len(dataset.observations)} observations "
        f"across {len(dataset.areas)} areas and {len(dataset.items)} items."
    )


def _write_dataset(output: Path, dataset: Dataset) -> None:
    """Serialize a dataset to disk."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dataset.to_dict(), indent=2))
    click.echo(f"Wrote dataset snapshot to {output}")


@click.group()
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default="public",
    show_default=True,
    help=SCHEMA_HELP,
)
@click.pass_context
def cli(ctx: click.Context, dsn: str | None, schema: str) -> None:
    """Manage CPI ingestion, database loading, and reporting workflows."""
    ctx.ensure_object(dict)
    ctx.obj.update({"dsn": dsn, "schema": schema})


@cli.command("fetch-dataset")
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Optional path to save the assembled dataset as JSON.",
)
@click.option(
    "--data-file",
    "data_files",
    multiple=True,
    help=DATA_FILE_HELP,
)
@click.option(
    "--current-only",
    is_flag=True,
    default=False,
    help="Limit ingestion to the current-year CPI partition.",
)
def fetch_dataset(
    *,
    output_path: Path | None,
    data_files: tuple[str, ...],
    current_only: bool,
) -> None:
    """Download CPI flat files, stitch them together, and report counts."""
    if current_only and data_files:
        raise click.UsageError("--current-only cannot be combined with --data-file.")
    dataset = _build_dataset(current_only=current_only, data_files=data_files or None)
    _echo_dataset_summary("Fetched dataset", dataset)
    if output_path:
        _write_dataset(output_path, dataset)


@cli.command("load-full")
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default=None,
    help=SCHEMA_HELP,
)
@click.option(
    "--no-truncate/--truncate",
    default=False,
    show_default=True,
    help="Skip truncating existing CPI tables before loading.",
)
@click.option(
    "--data-file",
    "data_files",
    multiple=True,
    help=DATA_FILE_HELP,
)
@click.pass_context
def load_full(
    ctx: click.Context,
    *,
    dsn: str | None,
    schema: str | None,
    no_truncate: bool,
    data_files: tuple[str, ...],
) -> None:
    """Load the entire CPI history into PostgreSQL."""
    resolved_dsn = _require_dsn(ctx, dsn)
    resolved_schema = _resolve_schema(ctx, schema)
    dataset = asyncio.run(
        load_full_history(
            resolved_dsn,
            schema=resolved_schema,
            truncate=not no_truncate,
            data_files=data_files or None,
        )
    )
    _echo_dataset_summary("Loaded dataset", dataset)


@cli.command("update-current")
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default=None,
    help=SCHEMA_HELP,
)
@click.pass_context
def update_current(ctx: click.Context, *, dsn: str | None, schema: str | None) -> None:
    """Refresh only the current-year CPI observations."""
    resolved_dsn = _require_dsn(ctx, dsn)
    resolved_schema = _resolve_schema(ctx, schema)
    dataset = asyncio.run(update_current_periods(resolved_dsn, schema=resolved_schema))
    _echo_dataset_summary("Updated current partitions", dataset)


@cli.command("ensure-schema")
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default=None,
    help=SCHEMA_HELP,
)
@click.pass_context
def ensure_schema(ctx: click.Context, *, dsn: str | None, schema: str | None) -> None:
    """Create the CPI tables if they are missing."""
    resolved_dsn = _require_dsn(ctx, dsn)
    resolved_schema = _resolve_schema(ctx, schema)

    async def _run() -> None:
        loader = CpiDatabaseLoader(dsn=resolved_dsn, schema=resolved_schema)
        try:
            await loader.ensure_schema()
        finally:
            await loader.close()

    asyncio.run(_run())
    click.echo(f"Ensured schema objects in {resolved_schema}.")


@cli.command("sync-metadata")
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default=None,
    help=SCHEMA_HELP,
)
@click.option(
    "--current-only",
    is_flag=True,
    default=False,
    help="Use the smaller current-year partition to refresh metadata.",
)
@click.option(
    "--data-file",
    "data_files",
    multiple=True,
    help=DATA_FILE_HELP,
)
@click.pass_context
def sync_metadata(
    ctx: click.Context,
    *,
    dsn: str | None,
    schema: str | None,
    current_only: bool,
    data_files: tuple[str, ...],
) -> None:
    """Upsert mapping tables and series definitions without touching observations."""
    if current_only and data_files:
        raise click.UsageError("--current-only cannot be combined with --data-file.")
    dataset = _build_dataset(current_only=current_only, data_files=data_files or None)
    resolved_dsn = _require_dsn(ctx, dsn)
    resolved_schema = _resolve_schema(ctx, schema)

    async def _run() -> None:
        loader = CpiDatabaseLoader(dsn=resolved_dsn, schema=resolved_schema)
        try:
            await loader.sync_metadata(dataset)
        finally:
            await loader.close()

    asyncio.run(_run())
    _echo_dataset_summary("Synced metadata using dataset", dataset)


if __name__ == "__main__":
    cli()
