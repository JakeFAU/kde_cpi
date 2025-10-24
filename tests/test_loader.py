"""Regression tests for the high-level CPI data loader."""

from decimal import Decimal

from kde_cpi.data import CpiDataLoader

SAMPLE_FILES = {
    "cu.area": (
        "area_code\tarea_name\tdisplay_level\tselectable\tsort_sequence\n"
        "0000\tU.S. city average\t0\tY\t10\n"
        "0100\tBoston, MA\t1\tY\t20\n"
    ),
    "cu.item": (
        "item_code\titem_name\tdisplay_level\tselectable\tsort_sequence\n"
        "SA0\tAll items\t0\tY\t01\n"
        "SAF\tFood\t1\tY\t02\n"
    ),
    "cu.period": (
        "period\tperiod_abbr\tperiod_name\tperiod_sort_order\n"
        "M01\tJAN\tJanuary\t1\n"
        "M02\tFEB\tFebruary\t2\n"
        "M13\tANN\tAnnual\t13\n"
    ),
    "cu.footnote": ("footnote_code\tfootnote_text\nP\tPreliminary\n"),
    "cu.series": (
        "series_id\tsurvey_abbreviation\tseries_title\tseasonal\tperiodicity_code\t"
        "area_code\titem_code\tbase_code\tbase_period\tbegin_year\t"
        "begin_period\tend_year\tend_period\n"
        "CUSR0000SA0\tCU\tAll items\tS\tR\t0000\tSA0\tS\t1982-84=100\t1947\tM01\t\t\n"
        "CUSR0100SAF\tCU\tBoston Food\tU\tR\t0100\tSAF\tS\t1982-84=100\t1960\tM02\t2020\tM13\n"
    ),
    "cu.data.0.Current": (
        "series_id\tyear\tperiod\tvalue\tfootnote_codes\n"
        "CUSR0000SA0\t2023\tM01\t301.5\t\n"
        "CUSR0000SA0\t2023\tM02\t302.9\tP\n"
        "CUSR0100SAF\t2020\tM02\t212.123\t\n"
    ),
}


def fake_fetch(name: str) -> str:
    """Return canned CPI file payloads keyed by filename."""
    if name not in SAMPLE_FILES:
        raise KeyError(name)
    return SAMPLE_FILES[name]


def test_loader_builds_dataset_from_flat_files():
    """The loader should stitch together CPI data and mappings."""
    loader = CpiDataLoader(source=fake_fetch)
    dataset = loader.load_dataset(data_files=("cu.data.0.Current",))

    assert dataset.mappings.area("0000").name == "U.S. city average"
    assert dataset.mappings.item("SA0").name == "All items"

    series = dataset.get("CUSR0000SA0")
    assert series.metadata.begin_year == 1947
    assert series.metadata.begin_period is not None
    assert len(series) == 2
    assert series.observations[0].period.code == "M01"
    assert series.observations[0].value == Decimal("301.5")
    assert series.observations[1].footnotes and series.observations[1].footnotes[0].code == "P"

    filtered = dataset.find(area="0100", item="SAF")
    assert len(filtered) == 1
    assert filtered[0].metadata.end_year == 2020
