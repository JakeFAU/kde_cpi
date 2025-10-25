"""Parsers for CPI flat files."""

import csv
import io
from collections.abc import Iterable

from .models import Area, Footnote, Item, Observation, Period, Series


def _normalize_key(key: str) -> str:
    """Normalize header names for case/whitespace inconsistencies."""
    return key.strip().lower().replace(" ", "_")


def _read_tsv(text: str) -> Iterable[dict[str, str]]:
    """Read a tab-separated payload into cleaned dictionaries."""
    buffer = io.StringIO(text)
    reader = csv.DictReader(buffer, delimiter="\t")
    for row in reader:
        if not row:
            continue
        # Skip bogus blank lines that may appear at EOF.
        if all(value is None or value.strip() == "" for value in row.values()):
            continue
        yield {_normalize_key(key): (value or "").strip() for key, value in row.items()}


def parse_areas(text: str) -> list[Area]:
    """Parse the area lookup table."""
    return [Area(code=row["area_code"], name=row["area_name"]) for row in _read_tsv(text)]


def parse_items(text: str) -> list[Item]:
    """Parse the item lookup table while coercing metadata types."""
    return [
        Item(
            code=row["item_code"],
            name=row["item_name"],
            display_level=row.get("display_level", "0"),
            selectable=row.get("selectable", "F"),
            sort_sequence=row.get("sort_sequence", "0"),
        )
        for row in _read_tsv(text)
    ]


def parse_periods(text: str) -> list[Period]:
    """Parse the period lookup table with display names."""
    return [
        Period(code=row["period"], abbr=row["period_abbr"], name=row["period_name"])
        for row in _read_tsv(text)
    ]


def parse_footnotes(text: str) -> list[Footnote]:
    """Parse the footnote lookup table."""
    return [
        Footnote(code=row["footnote_code"], text=row["footnote_text"]) for row in _read_tsv(text)
    ]


def parse_series(text: str) -> list[Series]:
    """Parse CPI series metadata records from the flat file."""
    result: list[Series] = []
    for row in _read_tsv(text):
        result.append(
            Series(
                series_id=row["series_id"],
                series_title=row.get("series_title", ""),
                area_code=row["area_code"],
                item_code=row["item_code"],
                seasonal=row["seasonal"],
                periodicity_code=row["periodicity_code"],
                base_code=row["base_code"],
                base_period=row.get("base_period", ""),
                begin_year=row.get("begin_year", "0"),
                begin_period=row.get("begin_period", ""),
                end_year=row.get("end_year", "0"),
                end_period=row.get("end_period", ""),
            )
        )
    return result


def parse_observations(text: str) -> list[Observation]:
    """Parse CPI observation records from the data files."""
    observations: list[Observation] = []
    for row in _read_tsv(text):
        observations.append(
            Observation(
                series_id=row["series_id"],
                year=row["year"],
                period=row["period"],
                value=row["value"],
                footnotes=row.get("footnote_codes", ""),
            )
        )
    return observations
