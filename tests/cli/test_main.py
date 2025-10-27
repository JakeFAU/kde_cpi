# tests/test_cli_fast.py
import json
from decimal import Decimal

from click.testing import CliRunner
from tests.conftest import FakeItem, FakeObs, FakeSeries

import cli.main as cli_mod


def test_fetch_dataset_fast(monkeypatch, tmp_path, tiny_dataset):
    monkeypatch.setattr(cli_mod, "_build_dataset", lambda **kw: tiny_dataset)
    r = CliRunner().invoke(cli_mod.cli, ["fetch-dataset", "--output", str(tmp_path / "ds.json")])
    assert r.exit_code == 0
    data = json.loads((tmp_path / "ds.json").read_text())
    assert data["ok"] is True


def test_analyze_happy_path(monkeypatch, tmp_path, tiny_dataset):
    # Make 'database' source load tiny dataset
    monkeypatch.setattr(cli_mod, "_load_dataset_from_database", lambda *a, **k: tiny_dataset)
    r = CliRunner().invoke(
        cli_mod.cli,
        ["analyze", "--source", "database", "--output-dir", str(tmp_path)],
        env={"KDE_CPI_DSN": "postgresql://u:p@h/db"},
    )
    assert r.exit_code == 0, r.output

    assert r.exit_code == 0
    # Ensure artifacts directory appears
    paths = list(tmp_path.glob("analysis_display_level_*"))
    assert paths, r.output


def test_compute_json_stdout(monkeypatch, tiny_dataset):
    monkeypatch.setattr(
        cli_mod,
        "_load_analysis_dataset",
        lambda *a, **k: (tiny_dataset, cli_mod._build_observation_cache(tiny_dataset)),
    )
    r = CliRunner().invoke(cli_mod.cli, ["compute", "--source", "flatfiles", "--current-only"])
    assert r.exit_code == 0
    payload = json.loads(r.output)
    assert payload["component_count"] >= 1
    assert payload["group_by"] == "display-level"


def test_panel_csv(monkeypatch, tmp_path, tiny_dataset):
    monkeypatch.setattr(
        cli_mod,
        "_load_analysis_dataset",
        lambda *a, **k: (tiny_dataset, cli_mod._build_observation_cache(tiny_dataset)),
    )
    r = CliRunner().invoke(
        cli_mod.cli,
        [
            "panel",
            "--source",
            "flatfiles",
            "--current-only",
            "--start",
            "2025-09",
            "--end",
            "2025-09",
            "--export",
            str(tmp_path / "panel.csv"),
        ],
    )
    assert r.exit_code == 0
    assert (tmp_path / "panel.csv").exists()


def test_metrics_timeseries_csv(monkeypatch, tmp_path, tiny_dataset):
    monkeypatch.setattr(
        cli_mod,
        "_load_analysis_dataset",
        lambda *a, **k: (tiny_dataset, cli_mod._build_observation_cache(tiny_dataset)),
    )
    output_path = tmp_path / "ts.csv"
    r = CliRunner().invoke(
        cli_mod.cli,
        [
            "metrics-timeseries",
            "--source",
            "flatfiles",
            "--current-only",
            "--start",
            "2025-09",
            "--end",
            "2025-09",
            "--export",
            str(output_path),
        ],
    )
    assert r.exit_code == 0, r.output
    contents = output_path.read_text()
    assert "weighted_kde_mode" in contents


def test_load_full_noop(monkeypatch, tiny_dataset):
    async def _fake_load(*a, **k):
        return tiny_dataset

    monkeypatch.setattr(cli_mod, "load_full_history", lambda *a, **k: _fake_load())
    r = CliRunner().invoke(cli_mod.cli, ["load-full", "--dsn", "postgresql://u:p@h/db"])
    assert r.exit_code == 0


def test_update_current_noop(monkeypatch, tiny_dataset):
    async def _fake_update(*a, **k):
        return tiny_dataset

    monkeypatch.setattr(cli_mod, "update_current_periods", lambda *a, **k: _fake_update())
    r = CliRunner().invoke(cli_mod.cli, ["update-current", "--dsn", "postgresql://u:p@h/db"])
    assert r.exit_code == 0


def test_ensure_schema_noop(monkeypatch):
    class FakeLoader:
        async def ensure_schema(self):
            return None

        async def close(self):
            return None

        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(cli_mod, "CpiDatabaseLoader", FakeLoader)
    r = CliRunner().invoke(cli_mod.cli, ["ensure-schema", "--dsn", "postgresql://u:p@h/db"])
    assert r.exit_code == 0


def _build_multi_series_dataset():
    dataset = type("Dataset", (), {})()
    dataset.series = {
        "S1": FakeSeries(
            item_code="AA",
            series_title="Alpha",
            area_code="1111",
            seasonal="U",
            base_code="BA0",
        ),
        "S2": FakeSeries(
            item_code="BB",
            series_title="Beta",
            area_code="2222",
            seasonal="S",
            base_code="BA0",
        ),
    }
    dataset.items = {
        "AA": FakeItem(name="Alpha item", display_level=1),
        "BB": FakeItem(name="Beta item", display_level=2),
    }
    dataset.observations = [
        FakeObs("S1", 2024, "M09", Decimal("100.0")),
        FakeObs("S1", 2025, "M09", Decimal("105.0")),
        FakeObs("S2", 2024, "M09", Decimal("200.0")),
        FakeObs("S2", 2025, "M09", Decimal("210.0")),
    ]
    dataset.areas = {}
    return dataset


def _json_from_output(output: str) -> dict:
    start = output.find("{")
    assert start != -1, f"Expected JSON document in output, got: {output!r}"
    return json.loads(output[start:])


def test_compute_series_lock_filters(monkeypatch):
    def _load_dataset(*args, **kwargs):
        ds = _build_multi_series_dataset()
        return ds, cli_mod._build_observation_cache(ds)

    monkeypatch.setattr(cli_mod, "_load_analysis_dataset", _load_dataset)
    result = CliRunner().invoke(
        cli_mod.cli,
        [
            "compute",
            "--source",
            "flatfiles",
            "--current-only",
            "--series-lock",
            "area_code=1111",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = _json_from_output(result.output)
    assert payload["component_count"] == 1
    assert payload["series_locks"] == {"area_code": "1111"}
    assert payload["groups"], "Expected at least one group in the filtered dataset."
    for group in payload["groups"]:
        assert group["count"] == 1
        for example in group["examples"]:
            assert example["series_id"] == "S1"


def test_compute_skip_small_samples(monkeypatch):
    def _load_dataset(*args, **kwargs):
        ds = _build_multi_series_dataset()
        return ds, cli_mod._build_observation_cache(ds)

    monkeypatch.setattr(cli_mod, "_load_analysis_dataset", _load_dataset)
    result = CliRunner().invoke(
        cli_mod.cli,
        [
            "compute",
            "--source",
            "flatfiles",
            "--current-only",
            "--series-lock",
            "area_code=1111",
            "--min-sample-size",
            "5",
            "--skip-small-samples",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = _json_from_output(result.output)
    assert payload["component_count"] == 1
    assert payload["group_count"] == 0
    assert payload["skip_small_samples"] is True
    assert "Warning: Sample size 1 below minimum 5" in result.output
