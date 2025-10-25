"""Unit tests for the HTTP client."""

from unittest.mock import MagicMock

import pytest
import requests

from kde_cpi.data.client import CpiHttpClient


def test_cpi_http_client_get_text_success(mocker):
    """Test that the client returns the text of a successful response."""
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "test data"
    mock_response.content = b"test data"
    mock_response.raise_for_status = MagicMock()
    mock_session.get.return_value = mock_response

    client = CpiHttpClient()
    client.session = mock_session
    text = client.get_text("test.txt")

    assert text == "test data"
    mock_session.get.assert_called_once()
    args, kwargs = mock_session.get.call_args
    assert "test.txt" in args[0]
    assert "headers" in kwargs


def test_cpi_http_client_get_text_failure(mocker):
    """Test that the client raises an exception on a failed response."""
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
    mock_session.get.return_value = mock_response

    client = CpiHttpClient()
    client.session = mock_session
    with pytest.raises(requests.HTTPError):
        client.get_text("test.txt")

    mock_session.get.assert_called_once()
    args, kwargs = mock_session.get.call_args
    assert "test.txt" in args[0]
    assert "headers" in kwargs
