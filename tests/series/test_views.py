"""Unit tests for the series views."""

import pytest

from kde_cpi.series.views import build_series_view_sql, SeriesViewManager


def test_build_series_view_sql():
    """Test the build_series_view_sql function."""
    sql = build_series_view_sql(
        view_name="test_view",
        schema="test_schema",
        filter_condition="i.display_level = 0",
        selectable_only=True,
    )

    assert "CREATE OR REPLACE VIEW test_schema.test_view" in sql
    assert "i.display_level = 0" in sql
    assert "i.selectable = TRUE" in sql


@pytest.mark.asyncio
async def test_series_view_manager_create_view_for_display_level(mocker):
    """Test the create_view_for_display_level method."""
    mock_connection = mocker.AsyncMock()
    mocker.patch("asyncpg.connect", return_value=mock_connection)

    manager = SeriesViewManager(dsn="test_dsn")
    await manager.create_view_for_display_level(level=0)

    mock_connection.execute.assert_called_once()


@pytest.mark.asyncio
async def test_series_view_manager_create_view_for_item_code_length(mocker):
    """Test the create_view_for_item_code_length method."""
    mock_connection = mocker.AsyncMock()
    mocker.patch("asyncpg.connect", return_value=mock_connection)

    manager = SeriesViewManager(dsn="test_dsn")
    await manager.create_view_for_item_code_length(code_length=4)

    mock_connection.execute.assert_called_once()
