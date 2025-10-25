"""Command line entry point for the kde-cpi application."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import DivisionByZero, InvalidOperation
from pathlib import Path

import click
import structlog

from kde_cpi.data import (
    CpiDatabaseLoader,
    CpiDatasetBuilder,
    Dataset,
    Observation,
    load_full_history,
    update_current_periods,
)
from kde_cpi.logging import configure_logging
from kde_cpi.math import StatSummary, compute_statistics
from kde_cpi.output import generate_density_plot, generate_histogram_plot
from kde_cpi.output.utils import format_percent

DSN_HELP = "PostgreSQL connection string. May also be set via the KDE_CPI_DSN env var."
SCHEMA_HELP = (
    "Target database schema for CPI tables. May also be set via the KDE_CPI_SCHEMA env var."
)
DATA_FILE_HELP = "One or more specific CPI data partitions to ingest (e.g. cu.data.0.Current)."

LOG_FORMAT_CHOICES = ("console", "json")
LOG_LEVEL_CHOICES = ("critical", "error", "warning", "info", "debug")

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class ObservationCache:
    """Precomputed lookup tables for per-series observations."""

    observations: dict[str, dict[tuple[int, str], Observation]]
    latest: dict[str, tuple[tuple[int, str], Observation]]
    periods: list[tuple[int, str]]


@dataclass(frozen=True)
class GrowthComponent:
    """Single YoY growth component derived from CPI series observations."""

    series_id: str
    item_code: str
    item_name: str
    display_level: int
    series_title: str
    value: float
    year: int
    period: str


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
    build_log = logger.bind(scope="dataset-build", current_only=current_only)
    build_log.debug("dataset.build_start", data_files=list(data_files) if data_files else [])
    builder = CpiDatasetBuilder()
    try:
        if current_only:
            dataset = builder.load_current_observations()
        else:
            dataset = builder.load_dataset(data_files=data_files)
    finally:
        builder.close()
    if not dataset.observations:
        build_log.warning("dataset.build_empty", reason="no observations parsed")
    else:
        build_log.debug("dataset.build_complete", observations=len(dataset.observations))
    return dataset


def _echo_dataset_summary(action: str, dataset: Dataset) -> None:
    """Emit a concise data volume summary for terminal feedback."""
    logger.info(
        "dataset.summary",
        action=action,
        series=len(dataset.series),
        observations=len(dataset.observations),
        areas=len(dataset.areas),
        items=len(dataset.items),
    )
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
    logger.debug("dataset.snapshot_written", output=str(output))


@click.group()
@click.option("--dsn", envvar="KDE_CPI_DSN", help=DSN_HELP, default=None)
@click.option(
    "--schema",
    envvar="KDE_CPI_SCHEMA",
    default="public",
    show_default=True,
    help=SCHEMA_HELP,
)
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVEL_CHOICES, case_sensitive=False),
    envvar="KDE_CPI_LOG_LEVEL",
    default="info",
    show_default=True,
    help="Verbosity for structured logs.",
)
@click.option(
    "--log-format",
    type=click.Choice(LOG_FORMAT_CHOICES, case_sensitive=False),
    envvar="KDE_CPI_LOG_FORMAT",
    default="console",
    show_default=True,
    help="Render logs as console-friendly text or JSON.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    dsn: str | None,
    schema: str,
    log_level: str,
    log_format: str,
) -> None:
    """Manage CPI ingestion, database loading, and reporting workflows."""
    configure_logging(level=log_level, json_output=log_format.lower() == "json")
    ctx.ensure_object(dict)
    ctx.obj.update({"dsn": dsn, "schema": schema})
    logger.bind(command_group="kde-cpi").debug(
        "cli.initialized",
        dsn=bool(dsn),
        schema=schema,
        log_level=log_level.lower(),
        log_format=log_format.lower(),
    )


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
    cmd_log = logger.bind(command="fetch-dataset", current_only=current_only)
    cmd_log.info("command.start", data_files=list(data_files))
    dataset = _build_dataset(current_only=current_only, data_files=data_files or None)
    _echo_dataset_summary("Fetched dataset", dataset)
    if output_path:
        _write_dataset(output_path, dataset)
        cmd_log.info("dataset.written", output=str(output_path), records=len(dataset.observations))


GROUP_BY_CHOICES = ("display-level", "item-code-length", "series-name-length")


@cli.command("analyze")
@click.option(
    "--group-by",
    type=click.Choice(GROUP_BY_CHOICES, case_sensitive=False),
    default="display-level",
    show_default=True,
    help="Choose how to bucket series before computing weighted statistics.",
)
@click.option(
    "--length-bin-size",
    type=int,
    default=5,
    show_default=True,
    help="Bucket size when grouping legacy series-name-length (deprecated).",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("out"),
    show_default=True,
    help="Root directory for analysis artifacts.",
)
@click.option(
    "--source",
    type=click.Choice(["database", "flatfiles"], case_sensitive=False),
    default="database",
    show_default=True,
    help="Where to read CPI data from before analysis.",
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
    help="Limit ingestion to the current-year CPI partition before analyzing (flatfiles only).",
)
@click.option(
    "--selectable-only/--include-unselectable",
    default=True,
    show_default=True,
    help="Restrict the analysis set to items flagged as selectable in CPI metadata.",
)
@click.pass_context
def analyze(
    ctx: click.Context,
    *,
    group_by: str,
    length_bin_size: int,
    output_dir: Path,
    source: str,
    data_files: tuple[str, ...],
    current_only: bool,
    selectable_only: bool,
) -> None:
    """Compute YoY growth distributions and emit charts/statistics."""
    original_group = group_by.lower()
    legacy_series_grouping = original_group == "series-name-length"
    group_by_normalized = original_group
    source = source.lower()

    if legacy_series_grouping:
        logger.warning(
            "analysis.group_by_legacy",
            original="series-name-length",
            replacement="item-code-length",
        )
        group_by_normalized = "item-code-length"

    _validate_source_args(source, current_only=current_only, data_files=data_files)
    if legacy_series_grouping and length_bin_size <= 0:
        raise click.BadParameter("length-bin-size must be a positive integer.")

    cmd_log = logger.bind(command="analyze", group_by=group_by_normalized, source=source)
    cmd_log.info(
        "command.start",
        data_files=list(data_files),
        current_only=current_only,
        selectable_only=selectable_only,
        length_bin_size=length_bin_size,
    )

    if source == "database":
        resolved_dsn = _require_dsn(ctx, None)
        resolved_schema = _resolve_schema(ctx, None)
        dataset = _load_dataset_from_database(resolved_dsn, resolved_schema)
    else:
        dataset = _build_dataset(current_only=current_only, data_files=data_files or None)
    components, _ = _compute_growth_components(
        dataset,
        selectable_only=selectable_only,
        target_period=None,
    )
    if not components:
        raise click.ClickException("No year-over-year components were available for analysis.")

    analysis_dir = _create_analysis_dir(output_dir, group_by_normalized)
    groups = _group_components(components, group_by_normalized, length_bin_size=length_bin_size)
    group_summaries = []
    for label, comps in groups.items():
        if not comps:
            continue
        group_summaries.append(_render_group_reports(analysis_dir, label, comps))

    generated_at = datetime.now(timezone.utc)
    summary_payload = {
        "generated_at": generated_at.isoformat(),
        "group_by": group_by_normalized,
        "components_total": len(components),
        "group_count": len(group_summaries),
        "output_dir": str(analysis_dir),
        "groups": group_summaries,
    }
    summary_path = analysis_dir / "summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2))
    click.echo(f"Analysis artifacts written to {analysis_dir}")
    cmd_log.info("command.completed", output=str(analysis_dir), groups=len(group_summaries))


@cli.command("compute")
@click.option(
    "--date",
    help="Target month (YYYY-MM). Defaults to the latest available observations.",
)
@click.option(
    "--group-by",
    type=click.Choice(GROUP_BY_CHOICES, case_sensitive=False),
    default="display-level",
    show_default=True,
    help="Grouping dimension for the summary.",
)
@click.option(
    "--length-bin-size",
    type=int,
    default=5,
    show_default=True,
    help="Legacy bin size for series-name-length (deprecated).",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Optional path to write the JSON summary.",
)
@click.option(
    "--source",
    type=click.Choice(["database", "flatfiles"], case_sensitive=False),
    default="database",
    show_default=True,
    help="Source for CPI components prior to analysis.",
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
    help="Limit flatfile ingestion to the current partition.",
)
@click.option(
    "--selectable-only/--include-unselectable",
    default=True,
    show_default=True,
    help="Filter to selectable CPI items only.",
)
@click.pass_context
def compute(
    ctx: click.Context,
    *,
    date: str | None,
    group_by: str,
    length_bin_size: int,
    output: Path | None,
    source: str,
    data_files: tuple[str, ...],
    current_only: bool,
    selectable_only: bool,
) -> None:
    """Compute KDE-mode inflation summary without generating plots."""
    group_by_normalized = group_by.lower()
    legacy_series_grouping = group_by_normalized == "series-name-length"
    if legacy_series_grouping:
        logger.warning(
            "compute.group_by_legacy",
            original="series-name-length",
            replacement="item-code-length",
        )
        group_by_normalized = "item-code-length"
    if legacy_series_grouping and length_bin_size <= 0:
        raise click.BadParameter("length-bin-size must be positive when using the legacy option.")

    _validate_source_args(source, current_only=current_only, data_files=data_files)

    dataset, cache = _load_analysis_dataset(
        ctx,
        source=source,
        current_only=current_only,
        data_files=data_files,
    )

    target_period: tuple[int, str] | None = None
    date_label = "latest"
    if date:
        year, period_code, dt = _parse_month(date)
        target_period = (year, period_code)
        date_label = dt.strftime("%Y-%m")
    elif cache.periods:
        latest_year, latest_period = cache.periods[-1]
        date_label = _format_period_label(latest_year, latest_period)

    components, _ = _compute_growth_components(
        dataset,
        selectable_only=selectable_only,
        target_period=target_period,
        cache=cache,
    )
    if not components:
        raise click.ClickException("No components were available for the requested configuration.")

    groups = _group_components(components, group_by_normalized, length_bin_size=length_bin_size)
    summaries = [_build_group_summary(label, comps) for label, comps in groups.items() if comps]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": date_label,
        "group_by": group_by_normalized,
        "source": source.lower(),
        "selectable_only": selectable_only,
        "component_count": len(components),
        "group_count": len(summaries),
        "groups": summaries,
    }
    document = json.dumps(payload, indent=2)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(document)
        click.echo(f"Wrote summary to {output}")
    else:
        click.echo(document)


@cli.command("panel")
@click.option("--start", required=True, help="Start month (YYYY-MM).")
@click.option("--end", required=True, help="End month (YYYY-MM).")
@click.option(
    "--group-by",
    type=click.Choice(GROUP_BY_CHOICES, case_sensitive=False),
    default="display-level",
    show_default=True,
    help="Grouping dimension for the panel.",
)
@click.option(
    "--length-bin-size",
    type=int,
    default=5,
    show_default=True,
    help="Legacy bin size for series-name-length (deprecated).",
)
@click.option(
    "--export",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Destination file (.csv or .parquet).",
)
@click.option(
    "--source",
    type=click.Choice(["database", "flatfiles"], case_sensitive=False),
    default="database",
    show_default=True,
    help="Source for CPI components prior to analysis.",
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
    help="Limit flatfile ingestion to the current partition.",
)
@click.option(
    "--selectable-only/--include-unselectable",
    default=True,
    show_default=True,
    help="Filter to selectable CPI items only.",
)
@click.pass_context
def panel(
    ctx: click.Context,
    *,
    start: str,
    end: str,
    group_by: str,
    length_bin_size: int,
    export: Path,
    source: str,
    data_files: tuple[str, ...],
    current_only: bool,
    selectable_only: bool,
) -> None:
    """Generate a tidy panel of KDE-mode metrics over a date range."""
    group_by_normalized = group_by.lower()
    legacy_series_grouping = group_by_normalized == "series-name-length"
    if legacy_series_grouping:
        logger.warning(
            "panel.group_by_legacy",
            original="series-name-length",
            replacement="item-code-length",
        )
        group_by_normalized = "item-code-length"
    if legacy_series_grouping and length_bin_size <= 0:
        raise click.BadParameter("length-bin-size must be positive when using the legacy option.")

    _validate_source_args(source, current_only=current_only, data_files=data_files)

    start_year, start_period, start_dt = _parse_month(start)
    end_year, end_period, end_dt = _parse_month(end)
    months = _month_sequence(start_dt, end_dt)

    dataset, cache = _load_analysis_dataset(
        ctx,
        source=source,
        current_only=current_only,
        data_files=data_files,
    )

    rows: list[dict[str, object]] = []
    for year, period_code, dt in months:
        components, cache = _compute_growth_components(
            dataset,
            selectable_only=selectable_only,
            target_period=(year, period_code),
            cache=cache,
        )
        if not components:
            continue
        groups = _group_components(components, group_by_normalized, length_bin_size=length_bin_size)
        date_label = dt.strftime("%Y-%m")
        for label, comps in groups.items():
            if not comps:
                continue
            summary = _build_group_summary(label, comps)
            rows.append(
                _flatten_summary_row(
                    date=date_label,
                    group_label=label,
                    summary=summary,
                    group_by=group_by_normalized,
                    selectable_only=selectable_only,
                    source=source.lower(),
                )
            )

    if not rows:
        raise click.ClickException("No rows were produced for the requested range.")

    export.parent.mkdir(parents=True, exist_ok=True)
    if export.suffix.lower() == ".csv":
        _write_csv(rows, export)
    elif export.suffix.lower() in {".parquet", ".pq"}:
        _write_parquet(rows, export)
    else:
        raise click.BadParameter("Export path must end with .csv or .parquet", param_hint="--export")
    click.echo(f"Panel written to {export}")


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
    cmd_log = logger.bind(command="load-full", schema=resolved_schema)
    cmd_log.info(
        "command.start",
        truncate=not no_truncate,
        data_files=list(data_files),
    )
    dataset = asyncio.run(
        load_full_history(
            resolved_dsn,
            schema=resolved_schema,
            truncate=not no_truncate,
            data_files=data_files or None,
        )
    )
    _echo_dataset_summary("Loaded dataset", dataset)
    cmd_log.info("command.completed", observations=len(dataset.observations))


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
    cmd_log = logger.bind(command="update-current", schema=resolved_schema)
    cmd_log.info("command.start")
    dataset = asyncio.run(update_current_periods(resolved_dsn, schema=resolved_schema))
    _echo_dataset_summary("Updated current partitions", dataset)
    cmd_log.info("command.completed", observations=len(dataset.observations))


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
    cmd_log = logger.bind(command="ensure-schema", schema=resolved_schema)
    cmd_log.info("command.start")

    async def _run() -> None:
        loader = CpiDatabaseLoader(dsn=resolved_dsn, schema=resolved_schema)
        try:
            await loader.ensure_schema()
        finally:
            await loader.close()

    asyncio.run(_run())
    click.echo(f"Ensured schema objects in {resolved_schema}.")
    cmd_log.info("command.completed")


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
    cmd_log = logger.bind(command="sync-metadata", schema=resolved_schema)
    cmd_log.info(
        "command.start",
        current_only=current_only,
        data_files=list(data_files),
    )

    async def _run() -> None:
        loader = CpiDatabaseLoader(dsn=resolved_dsn, schema=resolved_schema)
        try:
            await loader.sync_metadata(dataset)
        finally:
            await loader.close()

    asyncio.run(_run())
    _echo_dataset_summary("Synced metadata using dataset", dataset)
    cmd_log.info("command.completed", observations=len(dataset.observations))


def _create_analysis_dir(base: Path, group_by: str) -> Path:
    """Return a timestamped output directory for analysis artifacts."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = group_by.replace("-", "_")
    attempt = 0
    while True:
        suffix = f"_{attempt}" if attempt else ""
        candidate = base / f"analysis_{slug}_{timestamp}{suffix}"
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            attempt += 1


def _normalize_period(period: str) -> str:
    """Normalize CPI period codes for consistent lookups."""
    return period.strip().upper()


def _period_rank(period: str) -> int:
    """Return a sortable rank for CPI period codes (monthly-aware)."""
    period = _normalize_period(period)
    if len(period) >= 2 and period[1:].isdigit():
        return int(period[1:])
    return 0


def _period_sort_key(year: int, period: str) -> tuple[int, int, str]:
    """Provide a sorting tuple for period keys."""
    return (year, _period_rank(period), _normalize_period(period))


def _build_observation_cache(dataset: Dataset) -> ObservationCache:
    """Precompute per-series observation lookups and metadata."""
    observations: dict[str, dict[tuple[int, str], Observation]] = {}
    latest: dict[str, tuple[tuple[int, str], Observation]] = {}
    period_set: set[tuple[int, str]] = set()
    for obs in dataset.observations:
        period_code = _normalize_period(obs.period)
        key = (obs.year, period_code)
        period_set.add(key)
        series_entries = observations.setdefault(obs.series_id, {})
        series_entries[key] = obs
        order_key = _period_sort_key(obs.year, period_code)
        existing = latest.get(obs.series_id)
        if existing is None or order_key > _period_sort_key(*existing[0]):
            latest[obs.series_id] = (key, obs)
    periods = sorted(period_set, key=lambda item: _period_sort_key(item[0], item[1]))
    return ObservationCache(observations=observations, latest=latest, periods=periods)


def _load_dataset_from_database(dsn: str, schema: str) -> Dataset:
    """Load CPI data from PostgreSQL into a Dataset."""

    async def _run() -> Dataset:
        loader = CpiDatabaseLoader(dsn=dsn, schema=schema)
        try:
            return await loader.fetch_dataset()
        finally:
            await loader.close()

    logger.info("analysis.load_from_db", schema=schema)
    dataset = asyncio.run(_run())
    logger.info(
        "analysis.load_from_db_complete",
        series=len(dataset.series),
        observations=len(dataset.observations),
    )
    return dataset


def _compute_growth_components(
    dataset: Dataset,
    *,
    selectable_only: bool,
    target_period: tuple[int, str] | None = None,
    cache: ObservationCache | None = None,
) -> tuple[list[GrowthComponent], ObservationCache]:
    """Derive YoY growth components per series from the dataset."""
    cache = cache or _build_observation_cache(dataset)
    components: list[GrowthComponent] = []
    normalized_target: tuple[int, str] | None = None
    if target_period is not None:
        normalized_target = (target_period[0], _normalize_period(target_period[1]))

    for series_id, series_obs in cache.observations.items():
        if normalized_target is not None:
            current_key = normalized_target
        else:
            latest_entry = cache.latest.get(series_id)
            if latest_entry is None:
                continue
            current_key = latest_entry[0]
        current = series_obs.get(current_key)
        if current is None:
            continue
        prev_key = (current_key[0] - 1, current_key[1])
        previous = series_obs.get(prev_key)
        if previous is None:
            continue
        value = _compute_yoy(current, previous)
        if value is None:
            continue
        series = dataset.series.get(series_id)
        if series is None:
            continue
        item = dataset.items.get(series.item_code)
        if item is None:
            continue
        if selectable_only and not item.selectable:
            continue
        components.append(
            GrowthComponent(
                series_id=series_id,
                item_code=series.item_code,
                item_name=item.name,
                display_level=item.display_level,
                series_title=series.series_title,
                value=value,
                year=current.year,
                period=_normalize_period(current.period),
            )
        )
    logger.debug("analysis.components_computed", count=len(components))
    return components, cache


def _compute_yoy(current: Observation, previous: Observation) -> float | None:
    """Return the year-over-year change between two observations."""
    if current.value.is_nan() or previous.value.is_nan():
        return None
    if previous.value == 0:
        return None
    try:
        delta = (current.value - previous.value) / previous.value
    except (DivisionByZero, InvalidOperation):  # pragma: no cover - defensive
        return None
    return float(delta)


def _group_components(
    components: list[GrowthComponent],
    group_by: str,
    *,
    length_bin_size: int,
) -> dict[str, list[GrowthComponent]]:
    """Group components according to the requested strategy."""
    groups: dict[str, list[GrowthComponent]] = defaultdict(list)
    if group_by == "display-level":
        for comp in components:
            groups[str(comp.display_level)].append(comp)
    elif group_by == "item-code-length":
        for comp in components:
            code = (comp.item_code or "").strip()
            length = len(code)
            label = f"{length} chars"
            groups[label].append(comp)
    else:
        raise ValueError(f"Unsupported group_by value: {group_by}")
    def sort_key(item: tuple[str, list[GrowthComponent]]) -> tuple[int, str]:
        label = item[0]
        for token in label.split():
            if token.isdigit():
                return (int(token), label)
        return (0, label)

    return dict(sorted(groups.items(), key=sort_key))


def _render_group_reports(
    base_dir: Path,
    label: str,
    components: list[GrowthComponent],
) -> dict[str, object]:
    """Generate plots and summary payloads for a component group."""
    group_dir = base_dir / f"group_{_sanitize_label(label)}"
    group_dir.mkdir(parents=True, exist_ok=True)
    values = [comp.value for comp in components]
    weights = [1.0] * len(components)
    density_report = generate_density_plot(values, weights, output_dir=group_dir, filename="density.png")
    histogram_report = generate_histogram_plot(values, weights, output_dir=group_dir, filename="histogram.png")
    group_summary = _build_group_summary(label, components, stats=density_report.statistics)
    group_summary["density_plot"] = str(density_report.path.relative_to(base_dir))
    group_summary["histogram_plot"] = str(histogram_report.path.relative_to(base_dir))
    (group_dir / "summary.json").write_text(json.dumps(group_summary, indent=2))
    return group_summary


def _build_group_summary(
    label: str,
    components: list[GrowthComponent],
    *,
    stats: StatSummary | None = None,
) -> dict[str, object]:
    """Create a JSON-friendly summary for a group of components."""
    values = [comp.value for comp in components]
    weights = [1.0] * len(components)
    stats_obj = stats or compute_statistics(values, weights)
    stats_payload = _stats_to_dict(stats_obj)
    top_examples = sorted(components, key=lambda comp: abs(comp.value), reverse=True)[:5]
    examples = [
        {
            "series_id": comp.series_id,
            "item_code": comp.item_code,
            "item_name": comp.item_name,
            "series_title": comp.series_title,
            "yoy": comp.value,
            "yoy_percent": format_percent(comp.value),
        }
        for comp in top_examples
    ]
    return {
        "label": label,
        "count": len(components),
        "stats": stats_payload,
        "examples": examples,
    }


def _parse_month(value: str) -> tuple[int, str, datetime]:
    """Convert YYYY-MM strings into (year, period_code, datetime) tuples."""
    try:
        dt = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid date '{value}'. Expected format YYYY-MM.") from exc
    period = f"M{dt.month:02d}"
    return dt.year, period, dt


def _month_sequence(start: datetime, end: datetime) -> list[tuple[int, str, datetime]]:
    """Return inclusive list of (year, period_code, datetime) between two dates."""
    if start > end:
        raise ValueError("start date must be before end date.")
    months: list[tuple[int, str, datetime]] = []
    cursor = datetime(start.year, start.month, 1, tzinfo=start.tzinfo)
    end_marker = datetime(end.year, end.month, 1, tzinfo=end.tzinfo)
    while cursor <= end_marker:
        months.append((cursor.year, f"M{cursor.month:02d}", cursor))
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1, tzinfo=cursor.tzinfo)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1, tzinfo=cursor.tzinfo)
    return months


def _format_period_label(year: int, period: str) -> str:
    """Return YYYY-MM style labels when possible."""
    period = _normalize_period(period)
    if period.startswith("M") and period[1:].isdigit():
        return f"{year}-{int(period[1:]):02d}"
    return f"{year}-{period}"


def _load_analysis_dataset(
    ctx: click.Context,
    *,
    source: str,
    current_only: bool,
    data_files: Sequence[str],
) -> tuple[Dataset, ObservationCache]:
    """Load CPI data from the requested source and build cache metadata."""
    source = source.lower()
    if source == "database":
        resolved_dsn = _require_dsn(ctx, None)
        resolved_schema = _resolve_schema(ctx, None)
        dataset = _load_dataset_from_database(resolved_dsn, resolved_schema)
    else:
        dataset = _build_dataset(current_only=current_only, data_files=tuple(data_files) or None)
    cache = _build_observation_cache(dataset)
    return dataset, cache


def _validate_source_args(source: str, *, current_only: bool, data_files: Sequence[str]) -> None:
    """Enforce valid flag combinations for dataset sourcing."""
    source = source.lower()
    if source == "database":
        if data_files:
            raise click.UsageError("--data-file is only valid when --source flatfiles.")
        if current_only:
            raise click.UsageError("--current-only is only valid when --source flatfiles.")
    else:
        if current_only and data_files:
            raise click.UsageError("--current-only cannot be combined with --data-file.")


def _flatten_summary_row(
    *,
    date: str,
    group_label: str,
    summary: dict[str, object],
    group_by: str,
    selectable_only: bool,
    source: str,
) -> dict[str, object]:
    """Flatten a group summary into a tabular row."""
    stats = summary.get("stats", {})
    return {
        "date": date,
        "group_label": group_label,
        "group_by": group_by,
        "selectable_only": selectable_only,
        "source": source,
        "count": summary.get("count", 0),
        "mode": stats.get("weighted_kde_mode"),
        "mode_percent": stats.get("weighted_kde_mode_percent"),
        "mean": stats.get("weighted_mean"),
        "median": stats.get("weighted_median"),
        "trimmed_mean": stats.get("trimmed_mean"),
        "std": stats.get("weighted_std"),
        "skewness": stats.get("weighted_skewness"),
        "kurtosis": stats.get("weighted_kurtosis"),
        "effective_sample_size": stats.get("effective_sample_size"),
    }


def _write_csv(rows: list[dict[str, object]], path: Path) -> None:
    """Write panel rows to CSV via pandas."""
    import pandas as pd  # type: ignore

    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


def _write_parquet(rows: list[dict[str, object]], path: Path) -> None:
    """Write panel rows to parquet via pandas/pyarrow."""
    import pandas as pd  # type: ignore

    df = pd.DataFrame(rows)
    try:
        df.to_parquet(path, index=False)
    except (ImportError, ValueError) as exc:  # pragma: no cover - optional deps
        raise click.ClickException(
            "Writing parquet requires pandas with pyarrow or fastparquet installed."
        ) from exc


def _stats_to_dict(stats) -> dict[str, object]:
    """Convert a StatSummary into JSON-friendly primitives."""
    return {
        "weighted_mean": stats.weighted_mean,
        "weighted_mean_percent": format_percent(stats.weighted_mean),
        "weighted_median": stats.weighted_median,
        "weighted_median_percent": format_percent(stats.weighted_median),
        "trimmed_mean": stats.trimmed_mean,
        "trimmed_mean_percent": format_percent(stats.trimmed_mean),
        "weighted_std": stats.weighted_std,
        "weighted_skewness": stats.weighted_skewness,
        "weighted_kurtosis": stats.weighted_kurtosis,
        "weighted_kde_mode": stats.weighted_kde_mode,
        "weighted_kde_mode_percent": format_percent(stats.weighted_kde_mode),
        "effective_sample_size": stats.effective_sample_size,
    }


def _sanitize_label(label: str) -> str:
    """Return a filesystem-friendly version of the provided label."""
    safe = [ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in label]
    cleaned = "".join(safe).strip("_")
    return cleaned or "group"


if __name__ == "__main__":
    cli()
