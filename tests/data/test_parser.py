"""Unit tests for the data parser."""

import pytest

from kde_cpi.data.parser import (
    parse_areas,
    parse_items,
    parse_periods,
    parse_footnotes,
    parse_series,
    parse_observations,
)


def test_parse_areas():
    """Test parsing of area data."""
    text = "area_code\tarea_name\n0000\tU.S. city average\n"
    areas = list(parse_areas(text))
    assert len(areas) == 1
    assert areas[0].code == "0000"
    assert areas[0].name == "U.S. city average"


def test_parse_items():
    """Test parsing of item data."""
    text = "item_code\titem_name\tdisplay_level\tselectable\tsort_sequence\n"
    text += "AA0\tAll items - old base\t0\tT\t2\n"
    items = list(parse_items(text))
    assert len(items) == 1
    assert items[0].code == "AA0"
    assert items[0].name == "All items - old base"
    assert items[0].display_level == 0
    assert items[0].selectable is True
    assert items[0].sort_sequence == 2


def test_parse_periods():
    """Test parsing of period data."""
    text = "period_code\tperiod_abbr\tperiod_name\n"
    text += "M01\tJAN\tJanuary\n"
    periods = list(parse_periods(text))
    assert len(periods) == 1
    assert periods[0].code == "M01"
    assert periods[0].abbr == "JAN"
    assert periods[0].name == "January"


def test_parse_footnotes():
    """Test parsing of footnote data."""
    text = "footnote_code\tfootnote_text\n"
    text += "D\tData are preliminary\n"
    footnotes = list(parse_footnotes(text))
    assert len(footnotes) == 1
    assert footnotes[0].code == "D"
    assert footnotes[0].text == "Data are preliminary"


def test_parse_series():
    """Test parsing of series data."""
    text = (
        "series_id\tarea_code\titem_code\tseasonal\tperiodicity_code\t"
        "base_code\tbase_period\tbegin_year\tbegin_period\tend_year\tend_period\n"
    )
    text += (
        "SUUR0000AA0\t0000\tAA0\tS\tS\tC\t1982-84=100\t1913\tM01\t2023\tM01\n"
    )
    series = list(parse_series(text))
    assert len(series) == 1
    assert series[0].series_id == "SUUR0000AA0"
    assert series[0].area_code == "0000"
    assert series[0].item_code == "AA0"
    assert series[0].seasonal == "S"
    assert series[0].periodicity_code == "S"
    assert series[0].base_code == "C"
    assert series[0].base_period == "1982-84=100"
    assert series[0].begin_year == 1913
    assert series[0].begin_period == "M01"
    assert series[0].end_year == 2023
    assert series[0].end_period == "M01"


def test_parse_observations():
    """Test parsing of observation data."""
    text = "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
    text += "SUUR0000AA0\t1913\tM01\t9.8\t\n"
    observations = list(parse_observations(text))
    assert len(observations) == 1
    assert observations[0].series_id == "SUUR0000AA0"
    assert observations[0].year == 1913
    assert observations[0].period == "M01"
    assert str(observations[0].value) == "9.8"
    assert observations[0].footnotes == ()
