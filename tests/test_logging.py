"""Unit tests for the logging configuration."""

import logging

import pytest
import structlog

from kde_cpi.logging import configure_logging


def test_configure_logging(mocker):
    """Test that the logging is configured correctly."""
    mock_basic_config = mocker.patch("logging.basicConfig")
    configure_logging(level="debug")
    mock_basic_config.assert_called_with(
        level=logging.DEBUG, format="%(message)s", stream=mocker.ANY
    )

    configure_logging(level="info", json_output=True)
    mock_basic_config.assert_called_with(
        level=logging.INFO, format="%(message)s", stream=mocker.ANY
    )

    with pytest.raises(ValueError, match="Unsupported log level"):
        configure_logging(level="invalid")

    # Reset logging configuration
    structlog.reset_defaults()
