"""Utilities for building CPI series detail views in PostgreSQL."""

import re
from typing import Any

import asyncpg
from attrs import define, field

DATE_EXPRESSION = """CASE
    WHEN o.period ~ '^M(0[1-9]|1[0-2])$' THEN make_date(
        o.year,
        substring(o.period from 2 for 2)::int,
        1
    )
    WHEN o.period = 'M13' THEN make_date(o.year, 12, 31)
    WHEN o.period ~ '^Q0[1-4]$' THEN make_date(
        o.year,
        ((substring(o.period from 2 for 2)::int - 1) * 3) + 1,
        1
    )
    WHEN o.period ~ '^S0[1-3]$' THEN make_date(
        o.year,
        ((substring(o.period from 2 for 2)::int - 1) * 6) + 1,
        1
    )
    ELSE make_date(o.year, 1, 1)
END"""


def _validate_identifier(value: str) -> str:
    """Ensure the provided identifier is a valid unquoted SQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def build_series_view_sql(
    view_name: str,
    *,
    schema: str = "public",
    filter_condition: str,
    selectable_only: bool = True,
) -> str:
    """Produce CREATE OR REPLACE VIEW SQL for CPI series detail views."""
    view_ident = _validate_identifier(view_name)
    schema_ident = _validate_identifier(schema)
    filters = [filter_condition]
    if selectable_only:
        filters.append("i.selectable = TRUE")
    where_clause = " AND ".join(filters)
    return f"""  # noqa: S608
    CREATE OR REPLACE VIEW {schema_ident}.{view_ident} AS
    SELECT
        s.series_id AS id,
        i.item_name AS name,
        {DATE_EXPRESSION} AS date,
        o.value::numeric AS value
    FROM {schema_ident}.cpi_observation o
    JOIN {schema_ident}.cpi_series s ON s.series_id = o.series_id
    JOIN {schema_ident}.cpi_item i ON i.item_code = s.item_code
    WHERE {where_clause};
    """


@define(slots=True)
class SeriesViewManager:
    """Create CPI detail views grouped by item metadata."""

    dsn: str | None = None
    schema: str = "public"
    connection_kwargs: dict[str, Any] = field(factory=dict)
    selectable_only: bool = True
    _connection: asyncpg.Connection | None = field(default=None, init=False, repr=False)

    async def connect(self, **overrides: object) -> asyncpg.Connection:
        """Establish the asyncpg connection, reusing it when possible."""
        if self._connection is None:
            kwargs = {**self.connection_kwargs, **overrides}
            if self.dsn:
                self._connection = await asyncpg.connect(self.dsn, **kwargs)
            else:
                self._connection = await asyncpg.connect(**kwargs)
        if self._connection is None:
            raise RuntimeError("Failed to establish database connection.")
        return self._connection

    async def close(self) -> None:
        """Close the open connection, if any."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def create_view_for_display_level(
        self,
        level: int,
        *,
        view_name: str | None = None,
        selectable_only: bool | None = None,
    ) -> str:
        """Create (or replace) a view filtered by item display level."""
        view = view_name or f"cpi_view_display_level_{level}"
        sql = build_series_view_sql(
            view,
            schema=self.schema,
            filter_condition=f"i.display_level = {int(level)}",
            selectable_only=self._selectable_flag(selectable_only),
        )
        conn = await self.connect()
        await conn.execute(sql)  # noqa: S608
        return view

    async def create_view_for_item_code_length(
        self,
        code_length: int,
        *,
        view_name: str | None = None,
        selectable_only: bool | None = None,
    ) -> str:
        """Create (or replace) a view filtered by item code length."""
        view = view_name or f"cpi_view_item_length_{code_length}"
        sql = build_series_view_sql(
            view,
            schema=self.schema,
            filter_condition=f"char_length(s.item_code) = {int(code_length)}",
            selectable_only=self._selectable_flag(selectable_only),
        )
        conn = await self.connect()
        await conn.execute(sql)  # noqa: S608
        return view

    def _selectable_flag(self, override: bool | None) -> bool:
        """Resolve the effective selectable filter setting."""
        if override is not None:
            return override
        return self.selectable_only


__all__ = ["SeriesViewManager", "build_series_view_sql"]
