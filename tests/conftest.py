"""Global test configuration and fixtures."""

import types
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import requests

from kde_cpi.data.client import CpiHttpClient


@dataclass(slots=True)
class CannedResponse:
    """A canned HTTP response for testing."""

    text: str
    exception: requests.HTTPError | None = None


@pytest.fixture
def mock_cpi_http_client(mocker):
    """Mock the CpiHttpClient and prime it with canned responses for filenames."""
    mock_client_instance = MagicMock(spec=CpiHttpClient)

    def prime(responses: dict[str, CannedResponse]):
        def get_text_side_effect(filename: str, *, encoding: str = "utf-8"):
            if filename in responses:
                canned = responses[filename]
                if canned.exception:
                    raise canned.exception
                return canned.text
            raise FileNotFoundError(f"No canned response for filename: {filename}")

        mock_client_instance.get_text.side_effect = get_text_side_effect
        return mock_client_instance

    mocker.patch("kde_cpi.data.ingest.CpiHttpClient", return_value=mock_client_instance)
    mocker.patch("kde_cpi.data.client.CpiHttpClient", return_value=mock_client_instance)

    return prime


@pytest.fixture
def mock_db_loader(mocker):
    """Mock the CpiDatabaseLoader and its async methods."""
    mock_loader_instance = MagicMock()
    mock_loader_instance.ensure_schema = mocker.AsyncMock()
    mock_loader_instance.fetch_dataset = mocker.AsyncMock()
    mock_loader_instance.sync_metadata = mocker.AsyncMock()
    mock_loader_instance.close = mocker.AsyncMock()

    mocker.patch("kde_cpi.data.CpiDatabaseLoader", return_value=mock_loader_instance)
    return mock_loader_instance


# Minimal structures to satisfy cli expectations
@dataclass
class FakeItem:
    name: str
    display_level: int
    selectable: bool = True


@dataclass
class FakeSeries:
    item_code: str
    series_title: str
    area_code: str = "0000"
    seasonal: str = "U"
    base_code: str = "SA0"
    base_period: str = "1982-84=100"
    periodicity_code: str = "M"


@dataclass
class FakeObs:
    series_id: str
    year: int
    period: str
    value: Decimal

    @property
    def period_normalized(self):
        return self.period


class FakeDataset:
    def __init__(self):
        self.series = {"S1": FakeSeries(item_code="AA", series_title="Alpha")}
        self.items = {"AA": FakeItem(name="Alpha item", display_level=1)}
        self.observations = [
            FakeObs("S1", 2024, "M09", Decimal("100.0")),
            FakeObs("S1", 2025, "M09", Decimal("105.0")),  # +5% YoY
        ]
        self.areas = {}

    def to_dict(self):
        return {"ok": True}


@pytest.fixture
def tiny_dataset():
    return FakeDataset()


@pytest.fixture(autouse=True)
def fast_plots(monkeypatch, tmp_path):
    # Avoid matplotlib and return a simple object with .statistics/.path
    class R:
        def __init__(self, p):
            self.path = p
            self.statistics = types.SimpleNamespace(
                weighted_mean=0.01,
                weighted_median=0.009,
                trimmed_mean=0.01,
                weighted_std=0.02,
                weighted_skewness=0.0,
                weighted_kurtosis=3.0,
                weighted_kde_mode=0.012,
                effective_sample_size=123,
            )

    def _fake_density(values, weights, output_dir, filename):
        p = output_dir / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        return R(p)

    def _fake_hist(values, weights, output_dir, filename):
        p = output_dir / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"")
        return R(p)

    import cli.main as m

    monkeypatch.setattr(m, "generate_density_plot", _fake_density)
    monkeypatch.setattr(m, "generate_histogram_plot", _fake_hist)


@pytest.fixture(autouse=True)
def disable_heavy_plugins(monkeypatch):
    # pytest-sugar can slow runs a lot; turn it off during tests programmatically
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "0")  # keep coverage/xdist
