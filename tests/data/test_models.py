"""Unit tests for the data models."""

from decimal import Decimal

import pytest

from kde_cpi.data.models import (
    _to_bool,
    _footnote_tuple,
    _decimal,
    Area,
    Item,
    Period,
    Footnote,
    Series,
    Observation,
    Dataset,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("T", True),
        ("t", True),
        ("True", True),
        ("true", True),
        ("1", True),
        ("Y", True),
        ("y", True),
        ("F", False),
        ("f", False),
        ("False", False),
        ("false", False),
        ("0", False),
        ("N", False),
        ("n", False),
        ("", False),
    ],
)
def test_to_bool(value, expected):
    """Test the _to_bool helper function."""
    assert _to_bool(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("", ()),
        (" ", ()),
        ("A", ("A",)),
        (" A ", ("A",)),
        ("A,B", ("A", "B")),
        ("A , B", ("A", "B")),
        ("A B", ("A", "B")),
    ],
)
def test_footnote_tuple(value, expected):
    """Test the _footnote_tuple helper function."""
    assert _footnote_tuple(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1.23", Decimal("1.23")),
        (" 1.23 ", Decimal("1.23")),
        ("", Decimal("NaN")),
        (" ", Decimal("NaN")),
    ],
)
def test_decimal(value, expected):
    """Test the _decimal helper function."""
    assert str(_decimal(value)) == str(expected)


def test_series_is_seasonally_adjusted():
    """Test the is_seasonally_adjusted method."""
    series = Series(
        series_id="test",
        area_code="0000",
        item_code="AA0",
        seasonal="S",
        periodicity_code="S",
        base_code="C",
        base_period="1982-84=100",
        begin_year=1913,
        begin_period="M01",
        end_year=2023,
        end_period="M01",
    )
    assert series.is_seasonally_adjusted() is True


def test_series_is_not_seasonally_adjusted():
    """Test the is_seasonally_adjusted method."""
    series = Series(
        series_id="test",
        area_code="0000",
        item_code="AA0",
        seasonal="U",
        periodicity_code="S",
        base_code="C",
        base_period="1982-84=100",
        begin_year=1913,
        begin_period="M01",
        end_year=2023,
        end_period="M01",
    )
    assert series.is_seasonally_adjusted() is False


@pytest.mark.parametrize(
    "period, expected",
    [
        ("M13", True),
        ("m13", True),
        ("R13", True),
        ("r13", True),
        ("M01", False),
        ("S01", False),
    ],
)
def test_observation_is_annual(period, expected):
    """Test the is_annual method."""
    observation = Observation(
        series_id="test",
        year=2023,
        period=period,
        value="1.23",
        footnotes="",
    )
    assert observation.is_annual() is expected


def test_dataset_add_and_extend():
    """Test adding and extending data in a Dataset."""
    dataset = Dataset()
    area = Area(code="0000", name="U.S. city average")
    item = Item(
        code="AA0",
        name="All items - old base",
        display_level=0,
        selectable="T",
        sort_sequence=2,
    )
    period = Period(code="M01", abbr="JAN", name="January")
    footnote = Footnote(code="D", text="Data are preliminary")
    series = Series(
        series_id="SUUR0000AA0",
        area_code="0000",
        item_code="AA0",
        seasonal="S",
        periodicity_code="S",
        base_code="C",
        base_period="1982-84=100",
        begin_year=1913,
        begin_period="M01",
        end_year=2023,
        end_period="M01",
    )
    observation1 = Observation(
        series_id="SUUR0000AA0",
        year=2023,
        period="M01",
        value="1.23",
        footnotes="",
    )
    observation2 = Observation(
        series_id="SUUR0000AA0",
        year=2023,
        period="M02",
        value="4.56",
        footnotes="",
    )

    dataset.add_area(area)
    dataset.add_item(item)
    dataset.add_period(period)
    dataset.add_footnote(footnote)
    dataset.add_series(series)
    dataset.extend_observations([observation1, observation2, observation1])

    assert len(dataset.areas) == 1
    assert len(dataset.items) == 1
    assert len(dataset.periods) == 1
    assert len(dataset.footnotes) == 1
    assert len(dataset.series) == 1
    assert len(dataset.observations) == 2
