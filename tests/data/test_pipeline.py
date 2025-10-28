"""Unit tests for the data pipeline orchestrator."""

import pytest

from kde_cpi.data.models import Dataset
from kde_cpi.data.pipeline import load_full_history, update_current_periods


@pytest.fixture
def mock_dataset_builder(mocker):
    """Fixture for a mock CpiDatasetBuilder."""
    dataset = Dataset()
    builder_instance = mocker.MagicMock()
    builder_instance.load_dataset.return_value = dataset
    builder_class = mocker.patch("kde_cpi.data.pipeline.CpiDatasetBuilder", return_value=builder_instance)
    return builder_class, builder_instance, dataset


@pytest.fixture
def mock_database_loader(mocker):
    """Fixture for a mock CpiDatabaseLoader."""
    loader_instance = mocker.AsyncMock()
    loader_class = mocker.patch("kde_cpi.data.pipeline.CpiDatabaseLoader", return_value=loader_instance)
    return loader_class, loader_instance


@pytest.mark.asyncio
async def test_load_full_history_orchestrates_load(mock_dataset_builder, mock_database_loader):
    """Test that load_full_history correctly orchestrates a full load."""
    builder_class, builder_instance, dataset = mock_dataset_builder
    loader_class, loader_instance = mock_database_loader

    result = await load_full_history("test_dsn", schema="test_schema")

    assert result is dataset
    builder_class.assert_called_once_with()
    builder_instance.load_dataset.assert_called_once_with(data_files=None)
    builder_instance.close.assert_called_once()
    loader_class.assert_called_once_with(dsn="test_dsn", schema="test_schema")
    loader_instance.bulk_load.assert_awaited_once_with(dataset, truncate=True)
    loader_instance.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_full_history_with_data_files_and_no_truncate(mock_dataset_builder, mock_database_loader):
    """Test that load_full_history handles data_files and truncate=False."""
    _, builder_instance, dataset = mock_dataset_builder
    _, loader_instance = mock_database_loader
    data_files = ["file1.txt", "file2.txt"]

    await load_full_history("test_dsn", truncate=False, data_files=data_files)

    builder_instance.load_dataset.assert_called_once_with(data_files=data_files)
    loader_instance.bulk_load.assert_awaited_once_with(dataset, truncate=False)


@pytest.mark.asyncio
async def test_update_current_periods_orchestrates_update(mock_dataset_builder, mock_database_loader):
    """Test that update_current_periods correctly orchestrates an update."""
    builder_class, builder_instance, dataset = mock_dataset_builder
    loader_class, loader_instance = mock_database_loader
    builder_instance.load_current_observations.return_value = dataset

    result = await update_current_periods("test_dsn", schema="test_schema")

    assert result is dataset
    builder_class.assert_called_once_with()
    builder_instance.load_current_observations.assert_called_once_with()
    builder_instance.close.assert_called_once()
    loader_class.assert_called_once_with(dsn="test_dsn", schema="test_schema")
    loader_instance.merge_dataset.assert_awaited_once_with(dataset)
    loader_instance.close.assert_awaited_once()
