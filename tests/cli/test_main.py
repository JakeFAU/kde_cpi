# tests/test_cli_fast.py
import json

from click.testing import CliRunner

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
