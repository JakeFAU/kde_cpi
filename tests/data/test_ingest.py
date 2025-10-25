"""Unit tests for the data ingestor."""

from kde_cpi.data import parser
from kde_cpi.data.ingest import CpiDatasetBuilder
from kde_cpi.data.models import Area


def test_cpi_dataset_builder_load_dataset_by_dependencies(monkeypatch):
    """Test that the dataset builder correctly loads a dataset by mocking dependencies."""

    # Minimal fake text for each mapping key
    def fake_get_text(filename, encoding="utf-8"):
        if "cu.area" in filename:
            return "area_code\tarea_name\n0000\tU.S. city average\n"
        elif "series" in filename:
            return ""  # no series, fine
        else:
            return ""  # observation partitions empty

    fake_client = type("C", (), {"get_text": staticmethod(fake_get_text), "close": lambda: None})()
    builder = CpiDatasetBuilder(client=fake_client)

    # Make parsers return controlled, minimal objects
    monkeypatch.setattr(
        parser,
        "parse_areas",
        lambda text: [Area(code="0000", name="U.S. city average")],
    )
    monkeypatch.setattr(parser, "parse_items", lambda text: [])
    monkeypatch.setattr(parser, "parse_periods", lambda text: [])
    monkeypatch.setattr(parser, "parse_footnotes", lambda text: [])
    monkeypatch.setattr(parser, "parse_series", lambda text: [])
    monkeypatch.setattr(parser, "parse_observations", lambda text: [])

    dataset = builder.load_dataset()
    assert len(dataset.areas) == 1
