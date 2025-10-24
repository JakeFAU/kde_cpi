"""Async PostgreSQL integration for CPI datasets."""

from collections.abc import Iterable, Sequence
from typing import Any

import asyncpg
from attrs import define, field

from .models import Dataset, Observation, Series


@define(slots=True)
class CpiDatabaseLoader:
    """Persist CPI datasets into PostgreSQL using asyncpg primitives."""

    dsn: str | None = None
    schema: str = "public"
    connection_kwargs: dict[str, Any] = field(factory=dict)
    _connection: asyncpg.Connection | None = field(default=None, init=False, repr=False)

    async def connect(self, **overrides: Any) -> asyncpg.Connection:
        """Establish (or reuse) the async connection."""
        if self._connection is not None:
            return self._connection
        kwargs: dict[str, Any] = {**self.connection_kwargs, **overrides}
        if self.dsn:
            connection = await asyncpg.connect(self.dsn, **kwargs)
        else:
            connection = await asyncpg.connect(**kwargs)
        self._connection = connection
        return connection

    async def close(self) -> None:
        """Close the open connection, if any."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def ensure_schema(self) -> None:
        """Create the CPI tables if they do not already exist."""
        conn = await self.connect()
        qualified = self._qualifier
        statements = [
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_area")} (
                area_code text PRIMARY KEY,
                area_name text NOT NULL
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_item")} (
                item_code text PRIMARY KEY,
                item_name text NOT NULL,
                display_level integer NOT NULL,
                selectable boolean NOT NULL,
                sort_sequence integer NOT NULL
            );
            """,
            f"""
            ALTER TABLE {qualified("cpi_item")}
                ADD COLUMN IF NOT EXISTS display_level integer NOT NULL DEFAULT 0;
            """,
            f"""
            ALTER TABLE {qualified("cpi_item")}
                ADD COLUMN IF NOT EXISTS selectable boolean NOT NULL DEFAULT false;
            """,
            f"""
            ALTER TABLE {qualified("cpi_item")}
                ADD COLUMN IF NOT EXISTS sort_sequence integer NOT NULL DEFAULT 0;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_period")} (
                period_code text PRIMARY KEY,
                period_abbr text NOT NULL,
                period_name text NOT NULL
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_footnote")} (
                footnote_code text PRIMARY KEY,
                footnote_text text NOT NULL
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_series")} (
                series_id text PRIMARY KEY,
                area_code text NOT NULL REFERENCES {qualified("cpi_area")} (area_code),
                item_code text NOT NULL REFERENCES {qualified("cpi_item")} (item_code),
                seasonal text NOT NULL,
                periodicity_code text NOT NULL,
                base_code text NOT NULL,
                base_period text NOT NULL,
                begin_year integer NOT NULL,
                begin_period text NOT NULL,
                end_year integer NOT NULL,
                end_period text NOT NULL
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified("cpi_observation")} (
                series_id text NOT NULL REFERENCES {qualified("cpi_series")} (series_id)
                    ON DELETE CASCADE,
                year integer NOT NULL,
                period text NOT NULL,
                value numeric,
                footnotes text[],
                PRIMARY KEY (series_id, year, period)
            );
            """,
        ]
        for statement in statements:
            await conn.execute(statement)

    async def bulk_load(self, dataset: Dataset, *, truncate: bool = True) -> None:
        """Copy the full dataset into PostgreSQL, optionally truncating first."""
        conn = await self.connect()
        await self.ensure_schema()
        async with conn.transaction():
            if truncate:
                truncate_sql = (
                    "TRUNCATE TABLE "
                    f"{self._qualified('cpi_observation')}, "
                    f"{self._qualified('cpi_series')}, "
                    f"{self._qualified('cpi_footnote')}, "
                    f"{self._qualified('cpi_period')}, "
                    f"{self._qualified('cpi_item')}, "
                    f"{self._qualified('cpi_area')} "
                    "RESTART IDENTITY"
                )
                await conn.execute(truncate_sql)
            await self._copy_mapping_tables(conn, dataset)
            await self._copy_series(conn, dataset)
            await self._copy_observations(conn, dataset)

    async def sync_metadata(self, dataset: Dataset) -> None:
        """Upsert mapping tables and series definitions without touching observations."""
        conn = await self.connect()
        await self.ensure_schema()
        await self._upsert_areas(conn, list(dataset.areas.values()))
        await self._upsert_items(conn, list(dataset.items.values()))
        await self._upsert_periods(conn, list(dataset.periods.values()))
        await self._upsert_footnotes(conn, list(dataset.footnotes.values()))
        await self._upsert_series(conn, list(dataset.series.values()))

    async def merge_dataset(self, dataset: Dataset) -> None:
        """Synchronize metadata and upsert observations in place."""
        await self.sync_metadata(dataset)
        await self.upsert_observations(dataset.observations)

    async def upsert_observations(self, observations: Iterable[Observation]) -> None:
        """Upsert one or more observation rows."""
        conn = await self.connect()
        observations = list(observations)
        if not observations:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_observation")} (series_id, year, period, value, footnotes)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (series_id, year, period)
        DO UPDATE SET value = EXCLUDED.value,
                      footnotes = EXCLUDED.footnotes;
        """
        args = [
            (
                obs.series_id,
                obs.year,
                obs.period,
                None if obs.value.is_nan() else obs.value,
                list(obs.footnotes) or None,
            )
            for obs in observations
        ]
        await conn.executemany(query, args)

    async def _copy_mapping_tables(self, conn: asyncpg.Connection, dataset: Dataset) -> None:
        """Bulk copy area, item, period, and footnote records."""
        if dataset.areas:
            await conn.copy_records_to_table(
                "cpi_area",
                records=[(area.code, area.name) for area in dataset.areas.values()],
                columns=["area_code", "area_name"],
                schema_name=self.schema,
            )
        if dataset.items:
            await conn.copy_records_to_table(
                "cpi_item",
                records=[
                    (
                        item.code,
                        item.name,
                        item.display_level,
                        item.selectable,
                        item.sort_sequence,
                    )
                    for item in dataset.items.values()
                ],
                columns=[
                    "item_code",
                    "item_name",
                    "display_level",
                    "selectable",
                    "sort_sequence",
                ],
                schema_name=self.schema,
            )
        if dataset.periods:
            await conn.copy_records_to_table(
                "cpi_period",
                records=[
                    (period.code, period.abbr, period.name) for period in dataset.periods.values()
                ],
                columns=["period_code", "period_abbr", "period_name"],
                schema_name=self.schema,
            )
        if dataset.footnotes:
            await conn.copy_records_to_table(
                "cpi_footnote",
                records=[(footnote.code, footnote.text) for footnote in dataset.footnotes.values()],
                columns=["footnote_code", "footnote_text"],
                schema_name=self.schema,
            )

    async def _copy_series(self, conn: asyncpg.Connection, dataset: Dataset) -> None:
        """Bulk copy series definitions into the database."""
        if dataset.series:
            await conn.copy_records_to_table(
                "cpi_series",
                records=[
                    (
                        series.series_id,
                        series.area_code,
                        series.item_code,
                        series.seasonal,
                        series.periodicity_code,
                        series.base_code,
                        series.base_period,
                        series.begin_year,
                        series.begin_period,
                        series.end_year,
                        series.end_period,
                    )
                    for series in dataset.series.values()
                ],
                columns=[
                    "series_id",
                    "area_code",
                    "item_code",
                    "seasonal",
                    "periodicity_code",
                    "base_code",
                    "base_period",
                    "begin_year",
                    "begin_period",
                    "end_year",
                    "end_period",
                ],
                schema_name=self.schema,
            )

    async def _copy_observations(self, conn: asyncpg.Connection, dataset: Dataset) -> None:
        """Bulk copy observation facts into the database."""
        if dataset.observations:
            await conn.copy_records_to_table(
                "cpi_observation",
                records=[
                    (
                        obs.series_id,
                        obs.year,
                        obs.period,
                        None if obs.value.is_nan() else obs.value,
                        list(obs.footnotes) or None,
                    )
                    for obs in dataset.observations
                ],
                columns=["series_id", "year", "period", "value", "footnotes"],
                schema_name=self.schema,
            )

    async def _upsert_areas(self, conn: asyncpg.Connection, areas: Sequence) -> None:
        """Upsert area dimension records."""
        if not areas:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_area")} (area_code, area_name)
        VALUES ($1, $2)
        ON CONFLICT (area_code) DO UPDATE SET area_name = EXCLUDED.area_name;
        """
        await conn.executemany(query, [(area.code, area.name) for area in areas])

    async def _upsert_items(self, conn: asyncpg.Connection, items: Sequence) -> None:
        """Upsert item dimension records."""
        if not items:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_item")} (
            item_code,
            item_name,
            display_level,
            selectable,
            sort_sequence
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (item_code) DO UPDATE SET
            item_name = EXCLUDED.item_name,
            display_level = EXCLUDED.display_level,
            selectable = EXCLUDED.selectable,
            sort_sequence = EXCLUDED.sort_sequence;
        """
        await conn.executemany(
            query,
            [
                (
                    item.code,
                    item.name,
                    item.display_level,
                    item.selectable,
                    item.sort_sequence,
                )
                for item in items
            ],
        )

    async def _upsert_periods(self, conn: asyncpg.Connection, periods: Sequence) -> None:
        """Upsert period dimension records."""
        if not periods:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_period")} (period_code, period_abbr, period_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (period_code)
        DO UPDATE SET period_abbr = EXCLUDED.period_abbr,
                      period_name = EXCLUDED.period_name;
        """
        await conn.executemany(
            query, [(period.code, period.abbr, period.name) for period in periods]
        )

    async def _upsert_footnotes(self, conn: asyncpg.Connection, footnotes: Sequence) -> None:
        """Upsert footnote dimension records."""
        if not footnotes:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_footnote")} (footnote_code, footnote_text)
        VALUES ($1, $2)
        ON CONFLICT (footnote_code) DO UPDATE SET footnote_text = EXCLUDED.footnote_text;
        """
        await conn.executemany(query, [(footnote.code, footnote.text) for footnote in footnotes])

    async def _upsert_series(self, conn: asyncpg.Connection, series_list: Sequence[Series]) -> None:
        """Upsert series dimension records."""
        if not series_list:
            return
        query = f"""  # noqa: S608
        INSERT INTO {self._qualified("cpi_series")}
            (series_id, area_code, item_code, seasonal, periodicity_code, base_code, base_period,
             begin_year, begin_period, end_year, end_period)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (series_id) DO UPDATE SET
            area_code = EXCLUDED.area_code,
            item_code = EXCLUDED.item_code,
            seasonal = EXCLUDED.seasonal,
            periodicity_code = EXCLUDED.periodicity_code,
            base_code = EXCLUDED.base_code,
            base_period = EXCLUDED.base_period,
            begin_year = EXCLUDED.begin_year,
            begin_period = EXCLUDED.begin_period,
            end_year = EXCLUDED.end_year,
            end_period = EXCLUDED.end_period;
        """
        args = [
            (
                series.series_id,
                series.area_code,
                series.item_code,
                series.seasonal,
                series.periodicity_code,
                series.base_code,
                series.base_period,
                series.begin_year,
                series.begin_period,
                series.end_year,
                series.end_period,
            )
            for series in series_list
        ]
        await conn.executemany(query, args)

    def _qualified(self, table: str) -> str:
        """Return a schema-qualified table name."""
        return f"{self.schema}.{table}"

    def _qualifier(self, table: str) -> str:
        """Alias retained for compatibility with existing SQL templates."""
        return self._qualified(table)


__all__ = ["CpiDatabaseLoader"]
