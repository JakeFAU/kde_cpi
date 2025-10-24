"""Parsers for CPI flat files."""

import csv
import io
from collections.abc import Iterable

from .models import Area, Footnote, Item, Observation, Period, Series


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
        yield {key: (value or "").strip() for key, value in row.items()}


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


def _split_line(line: str) -> list[str]:
    """Split fixed-width CPI rows by whitespace while trimming extra spaces."""
    # Series and data files use whitespace-separated columns with a simple header row.
    return line.strip().split()


def parse_series(text: str) -> list[Series]:
    """Parse CPI series metadata records from the fixed-width flat file."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    header = _split_line(lines[0])
    expected_columns = [
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
    ]
    if header != expected_columns:
        raise ValueError(f"Unexpected series header: {header!r}")

    series: list[Series] = []
    for line in lines[1:]:
        parts = _split_line(line)
        if len(parts) < 11:
            continue
        # base_period can occasionally contain spaces. Split logic: the last four tokens map to
        # begin_year, begin_period, end_year, end_period respectively. Everything between
        # base_code and begin_year belongs to base_period.
        fixed = parts[:6]
        tail = parts[6:]
        if len(tail) < 5:
            continue
        base_period_tokens = tail[:-4]
        if not base_period_tokens:
            base_period_tokens = [""]
        base_period = " ".join(base_period_tokens)
        begin_year, begin_period, end_year, end_period = tail[-4:]
        series.append(
            Series(
                series_id=fixed[0],
                area_code=fixed[1],
                item_code=fixed[2],
                seasonal=fixed[3],
                periodicity_code=fixed[4],
                base_code=fixed[5],
                base_period=base_period,
                begin_year=begin_year,
                begin_period=begin_period,
                end_year=end_year,
                end_period=end_period,
            )
        )
    return series


def parse_observations(text: str) -> list[Observation]:
    """Parse CPI observation records from the data files."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    header = _split_line(lines[0])
    expected_columns = ["series_id", "year", "period", "value", "footnote_codes"]
    if header != expected_columns:
        raise ValueError(f"Unexpected data header: {header!r}")

    observations: list[Observation] = []
    for line in lines[1:]:
        parts = _split_line(line)
        if len(parts) < 4:
            continue
        series_id, year, period, value, *footnote_tokens = parts
        footnotes = " ".join(footnote_tokens)
        observations.append(
            Observation(
                series_id=series_id,
                year=year,
                period=period,
                value=value,
                footnotes=footnotes,
            )
        )
    return observations
