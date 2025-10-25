"""Global test configuration and fixtures."""

from dataclasses import dataclass
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
