"""Unit tests for the CLI."""

import pytest
from click.testing import CliRunner
from tests.conftest import CannedResponse

from cli.main import cli
from kde_cpi.data.files import DATA_FILES, MAPPING_FILES, SERIES_FILE
from kde_cpi.data.models import Dataset


@pytest.fixture
def runner():
    """Return a click CliRunner."""
    return CliRunner()


def test_fetch_dataset(runner, mock_cpi_http_client):
    """Test the fetch-dataset command."""
    canned_responses = {
        **{filename: CannedResponse("") for filename in MAPPING_FILES.values()},
        **{filename: CannedResponse("") for filename in DATA_FILES},
        SERIES_FILE: CannedResponse(""),
    }
    mock_cpi_http_client(canned_responses)
    result = runner.invoke(cli, ["fetch-dataset"])
    assert result.exit_code == 0


def test_analyze(runner, mock_cpi_http_client, mocker):
    """Test the analyze command."""
    mock_cpi_http_client({})
    mocker.patch("cli.main._load_dataset_from_database", return_value=Dataset())
    result = runner.invoke(
        cli,
        [
            "analyze",
            "--source",
            "database",
            "--dsn",
            "postgresql://user:pass@host:5432/db",
        ],
    )
    assert result.exit_code != 0  # No components to analyze


def test_compute(runner, mock_cpi_http_client, mocker):
    """Test the compute command."""
    mock_cpi_http_client({})
    mocker.patch("cli.main._load_dataset_from_database", return_value=Dataset())
    result = runner.invoke(
        cli,
        [
            "compute",
            "--source",
            "database",
            "--dsn",
            "postgresql://user:pass@host:5432/db",
        ],
    )
    assert result.exit_code != 0  # No components to compute


def test_panel(runner, mock_cpi_http_client, mocker):
    """Test the panel command."""
    mock_cpi_http_client({})
    mocker.patch("cli.main._load_dataset_from_database", return_value=Dataset())
    result = runner.invoke(
        cli,
        [
            "panel",
            "--source",
            "database",
            "--start",
            "2022-01",
            "--end",
            "2022-02",
            "--export",
            "test.csv",
            "--dsn",
            "postgresql://user:pass@host:5432/db",
        ],
    )
    assert result.exit_code != 0  # No data to create a panel from


def test_load_full(runner, mock_cpi_http_client, mocker):
    """Test the load-full command."""
    mocker.patch("cli.main.load_full_history")
    mock_cpi_http_client({})
    result = runner.invoke(cli, ["load-full", "--dsn", "postgresql://user:pass@host:5432/db"])
    assert result.exit_code == 0


def test_update_current(runner, mock_cpi_http_client, mocker):
    """Test the update-current command."""
    mocker.patch("cli.main.update_current_periods")
    mock_cpi_http_client({})
    result = runner.invoke(cli, ["update-current", "--dsn", "postgresql://user:pass@host:5432/db"])
    assert result.exit_code == 0


def test_ensure_schema(runner, mocker):
    """Test the ensure-schema command."""
    mocker.patch("kde_cpi.data.CpiDatabaseLoader.ensure_schema")
    result = runner.invoke(cli, ["ensure-schema", "--dsn", "postgresql://user:pass@host:5432/db"])
    assert result.exit_code == 0


def test_sync_metadata(runner, mock_cpi_http_client, mocker):
    """Test the sync-metadata command."""
    mocker.patch("kde_cpi.data.CpiDatabaseLoader.sync_metadata")
    mock_cpi_http_client({})
    result = runner.invoke(cli, ["sync-metadata", "--dsn", "postgresql://user:pass@host:5432/db"])
    assert result.exit_code == 0
