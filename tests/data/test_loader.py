"""Tests for the async database loader."""

from collections.abc import Callable, Sequence
from decimal import Decimal

import pytest

from kde_cpi.data.loader import CpiDatabaseLoader
from kde_cpi.data.models import (
    Area,
    Dataset,
    Footnote,
    Item,
    Observation,
    Period,
    Series,
)


def build_dataset() -> Dataset:
    """Create a dataset populated with one record for each dimension."""
    dataset = Dataset()
    dataset.add_area(Area(code="0000", name="All"))
    dataset.add_item(
        Item(
            code="AA0",
            name="All items",
            display_level=0,
            selectable=True,  # type: ignore[arg-type]
            sort_sequence=1,
        )
    )
    dataset.add_period(Period(code="M01", abbr="JAN", name="January"))
    dataset.add_footnote(Footnote(code="A", text="Preliminary"))
    dataset.add_series(
        Series(
            series_id="S000",
            series_title="All items",
            area_code="0000",
            item_code="AA0",
            seasonal="U",
            periodicity_code="M",
            base_code="C",
            base_period="1982-84=100",
            begin_year=2000,
            begin_period="M01",
            end_year=2020,
            end_period="M12",
        )
    )
    dataset.extend_observations(
        [
            Observation(
                series_id="S000",
                year=2020,
                period="M01",
                value="1.23",
                footnotes="A",
            ),
            Observation(
                series_id="S000",
                year=2020,
                period="M02",
                value="",
                footnotes="",
            ),
        ]
    )
    return dataset


@pytest.mark.asyncio
async def test_connect_reuses_single_connection(mocker):
    """Subsequent connect calls should reuse the same asyncpg connection."""
    mock_connection = mocker.AsyncMock()
    connect_mock = mocker.patch("asyncpg.connect", return_value=mock_connection)

    loader = CpiDatabaseLoader(dsn="postgres://example")
    first = await loader.connect(timeout=5)
    second = await loader.connect()

    assert first is second is mock_connection
    connect_mock.assert_called_once_with("postgres://example", timeout=5)


@pytest.mark.asyncio
async def test_close_closes_connection(mocker):
    """close() should release the cached connection when present."""
    mock_connection = mocker.AsyncMock()
    loader = CpiDatabaseLoader()
    loader._connection = mock_connection

    await loader.close()

    mock_connection.close.assert_awaited_once()
    assert loader._connection is None


@pytest.mark.asyncio
async def test_ensure_schema_executes_all_statements(mocker):
    """ensure_schema should run the expected DDL statements."""
    mock_connection = mocker.AsyncMock()
    loader = CpiDatabaseLoader(schema="public")
    loader._connection = mock_connection

    await loader.ensure_schema()

    assert mock_connection.execute.await_count == 10
    ddl_payload = " ".join(call.args[0] for call in mock_connection.execute.await_args_list)
    assert "CREATE TABLE IF NOT EXISTS public.cpi_area" in ddl_payload
    assert "CREATE TABLE IF NOT EXISTS public.cpi_observation" in ddl_payload


def _setup_transaction(mocker, connection):
    """Configure the async transaction context manager on the mock connection."""
    transaction_cm = mocker.MagicMock()
    transaction_cm.__aenter__ = mocker.AsyncMock(return_value=None)
    transaction_cm.__aexit__ = mocker.AsyncMock(return_value=None)
    connection.transaction = mocker.MagicMock(return_value=transaction_cm)


@pytest.mark.asyncio
async def test_bulk_load_with_truncate(mocker):
    """bulk_load should optionally truncate and then copy dataset tables."""
    dataset = build_dataset()
    connection = mocker.AsyncMock()
    _setup_transaction(mocker, connection)

    loader = CpiDatabaseLoader()
    loader._connection = connection
    ensure_schema_mock = mocker.patch.object(
        CpiDatabaseLoader, "ensure_schema", new=mocker.AsyncMock()
    )
    copy_mapping_mock = mocker.patch.object(
        CpiDatabaseLoader, "_copy_mapping_tables", new=mocker.AsyncMock()
    )
    copy_series_mock = mocker.patch.object(
        CpiDatabaseLoader, "_copy_series", new=mocker.AsyncMock()
    )
    copy_obs_mock = mocker.patch.object(
        CpiDatabaseLoader, "_copy_observations", new=mocker.AsyncMock()
    )

    await loader.bulk_load(dataset, truncate=True)

    ensure_schema_mock.assert_awaited_once()
    connection.execute.assert_awaited_once()
    copy_mapping_mock.assert_awaited_once_with(connection, dataset)
    copy_series_mock.assert_awaited_once_with(connection, dataset)
    copy_obs_mock.assert_awaited_once_with(connection, dataset)


@pytest.mark.asyncio
async def test_bulk_load_without_truncate_skips_execute(mocker):
    """bulk_load should skip TRUNCATE when truncate=False."""
    dataset = build_dataset()
    connection = mocker.AsyncMock()
    _setup_transaction(mocker, connection)

    loader = CpiDatabaseLoader()
    loader._connection = connection
    mocker.patch.object(CpiDatabaseLoader, "ensure_schema", new=mocker.AsyncMock())
    mocker.patch.object(CpiDatabaseLoader, "_copy_mapping_tables", new=mocker.AsyncMock())
    mocker.patch.object(CpiDatabaseLoader, "_copy_series", new=mocker.AsyncMock())
    mocker.patch.object(CpiDatabaseLoader, "_copy_observations", new=mocker.AsyncMock())

    await loader.bulk_load(dataset, truncate=False)

    connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_sync_metadata_invokes_upserts(mocker):
    """sync_metadata should upsert each dimension table."""
    dataset = build_dataset()
    connection = mocker.AsyncMock()
    loader = CpiDatabaseLoader()
    loader._connection = connection
    mocker.patch.object(CpiDatabaseLoader, "ensure_schema", new=mocker.AsyncMock())
    upsert_areas = mocker.patch.object(CpiDatabaseLoader, "_upsert_areas", new=mocker.AsyncMock())
    upsert_items = mocker.patch.object(CpiDatabaseLoader, "_upsert_items", new=mocker.AsyncMock())
    upsert_periods = mocker.patch.object(
        CpiDatabaseLoader, "_upsert_periods", new=mocker.AsyncMock()
    )
    upsert_footnotes = mocker.patch.object(
        CpiDatabaseLoader, "_upsert_footnotes", new=mocker.AsyncMock()
    )
    upsert_series = mocker.patch.object(CpiDatabaseLoader, "_upsert_series", new=mocker.AsyncMock())

    await loader.sync_metadata(dataset)

    upsert_areas.assert_awaited_once_with(connection, list(dataset.areas.values()))
    upsert_items.assert_awaited_once_with(connection, list(dataset.items.values()))
    upsert_periods.assert_awaited_once_with(connection, list(dataset.periods.values()))
    upsert_footnotes.assert_awaited_once_with(connection, list(dataset.footnotes.values()))
    upsert_series.assert_awaited_once_with(connection, list(dataset.series.values()))


@pytest.mark.asyncio
async def test_merge_dataset_delegates_to_helpers(mocker):
    """merge_dataset should sync metadata then upsert observations."""
    dataset = build_dataset()
    loader = CpiDatabaseLoader()
    sync_mock = mocker.patch.object(CpiDatabaseLoader, "sync_metadata", new=mocker.AsyncMock())
    upsert_mock = mocker.patch.object(
        CpiDatabaseLoader, "upsert_observations", new=mocker.AsyncMock()
    )

    await loader.merge_dataset(dataset)

    sync_mock.assert_awaited_once_with(dataset)
    upsert_mock.assert_awaited_once_with(dataset.observations)


@pytest.mark.asyncio
async def test_fetch_dataset_rehydrates_domain_models(mocker):
    """fetch_dataset should load each table and construct a Dataset."""
    connection = mocker.AsyncMock()
    connection.fetch = mocker.AsyncMock(
        side_effect=[
            [{"area_code": "0000", "area_name": "All"}],
            [
                {
                    "item_code": "AA0",
                    "item_name": "All items",
                    "display_level": 0,
                    "selectable": True,
                    "sort_sequence": 1,
                }
            ],
            [{"period_code": "M01", "period_abbr": "JAN", "period_name": "January"}],
            [{"footnote_code": "A", "footnote_text": "Prelim"}],
            [
                {
                    "series_id": "S000",
                    "series_title": None,
                    "area_code": "0000",
                    "item_code": "AA0",
                    "seasonal": "U",
                    "periodicity_code": "M",
                    "base_code": "C",
                    "base_period": "1982-84=100",
                    "begin_year": 2000,
                    "begin_period": "M01",
                    "end_year": 2020,
                    "end_period": "M12",
                }
            ],
            [
                {
                    "series_id": "S000",
                    "year": 2020,
                    "period": "M01",
                    "value": Decimal("1.23"),
                    "footnotes": ["A", "B"],
                }
            ],
        ]
    )

    loader = CpiDatabaseLoader()
    loader._connection = connection

    dataset = await loader.fetch_dataset()

    assert dataset.areas["0000"].name == "All"
    assert dataset.items["AA0"].display_level == 0
    assert dataset.periods["M01"].abbr == "JAN"
    assert dataset.footnotes["A"].text == "Prelim"
    assert dataset.series["S000"].series_title == ""
    assert len(dataset.observations) == 1
    observation = dataset.observations[0]
    assert str(observation.value) == "1.23"
    assert observation.footnotes == ("A", "B")


@pytest.mark.asyncio
async def test_upsert_observations_transforms_payload(mocker):
    """upsert_observations should normalize decimals and footnotes."""
    loader = CpiDatabaseLoader()
    connection = mocker.AsyncMock()
    loader._connection = connection

    observations = build_dataset().observations
    await loader.upsert_observations(observations)

    connection.executemany.assert_awaited_once()
    sql, args = connection.executemany.await_args_list[0].args
    assert "INSERT INTO" in sql
    assert args[0][3] == observations[0].value
    assert args[0][4] == list(observations[0].footnotes)
    assert args[1][3] is None
    assert args[1][4] is None


@pytest.mark.asyncio
async def test_upsert_observations_noop_for_empty_input(mocker):
    """upsert_observations should return early for empty iterables."""
    loader = CpiDatabaseLoader()
    connection = mocker.AsyncMock()
    loader._connection = connection

    await loader.upsert_observations([])

    connection.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_copy_mapping_tables_emits_each_dimension(mocker):
    """_copy_mapping_tables should emit each populated mapping table."""
    dataset = build_dataset()
    loader = CpiDatabaseLoader(schema="custom")
    connection = mocker.AsyncMock()

    await loader._copy_mapping_tables(connection, dataset)

    tables = [call.args[0] for call in connection.copy_records_to_table.await_args_list]
    assert tables == ["cpi_area", "cpi_item", "cpi_period", "cpi_footnote"]


@pytest.mark.asyncio
async def test_copy_series_and_observations_payloads(mocker):
    """_copy_series and _copy_observations should send properly shaped tuples."""
    dataset = build_dataset()
    loader = CpiDatabaseLoader(schema="custom")
    connection = mocker.AsyncMock()

    await loader._copy_series(connection, dataset)
    series_call = connection.copy_records_to_table.await_args_list[0]
    assert series_call.args[0] == "cpi_series"
    kwargs = series_call.kwargs
    assert kwargs["schema_name"] == "custom"
    assert kwargs["columns"][0] == "series_id"
    assert kwargs["records"][0][0] == "S000"

    connection.copy_records_to_table.reset_mock()
    await loader._copy_observations(connection, dataset)
    obs_call = connection.copy_records_to_table.await_args_list[0]
    assert obs_call.args[0] == "cpi_observation"
    obs_records: Sequence = obs_call.kwargs["records"]
    assert obs_records[0][4] == ["A"]
    assert obs_records[1][3] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "data_selector"),
    [
        ("_upsert_areas", lambda d: list(d.areas.values())),
        ("_upsert_items", lambda d: list(d.items.values())),
        ("_upsert_periods", lambda d: list(d.periods.values())),
        ("_upsert_footnotes", lambda d: list(d.footnotes.values())),
        ("_upsert_series", lambda d: list(d.series.values())),
    ],
)
async def test_upsert_helpers_call_executemany(
    mocker,
    method_name: str,
    data_selector: Callable[[Dataset], Sequence],
):
    """Each upsert helper should pass rows to executemany when data exists."""
    dataset = build_dataset()
    loader = CpiDatabaseLoader()
    connection = mocker.AsyncMock()
    method = getattr(loader, method_name)

    await method(connection, data_selector(dataset))

    connection.executemany.assert_awaited_once()


@pytest.mark.asyncio
async def test_upsert_series_skips_empty_sequences(mocker):
    """Upsert helpers should bail early on empty inputs."""
    loader = CpiDatabaseLoader()
    connection = mocker.AsyncMock()

    await loader._upsert_series(connection, [])

    connection.executemany.assert_not_called()
