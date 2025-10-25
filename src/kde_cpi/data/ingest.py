"""Orchestration utilities for assembling CPI datasets from flat files."""

from collections.abc import Iterable, Sequence

import structlog
from attrs import define, field

from . import parser
from .client import CpiHttpClient
from .files import CURRENT_DATA_FILES, DATA_FILES, MAPPING_FILES, SERIES_FILE
from .models import Dataset

logger = structlog.get_logger(__name__)


@define(slots=True)
class CpiDatasetBuilder:
    """Coordinate retrieval and parsing of CPI datasets from BLS flat files."""

    client: CpiHttpClient = field(factory=CpiHttpClient)

    def load_dataset(self, *, data_files: Sequence[str] | None = None) -> Dataset:
        """Fetch mapping tables, series definitions, and observations into a dataset."""
        dataset = Dataset()
        files_to_fetch = data_files or DATA_FILES
        log = logger.bind(data_files=list(files_to_fetch), builder="dataset")
        log.info("builder.load_start")

        # Load mapping tables first so downstream consumers can resolve codes.
        dataset = self._populate_mappings(dataset)
        dataset = self._populate_series(dataset)
        dataset = self._populate_observations(dataset, files_to_fetch)
        log.info(
            "builder.load_complete",
            series=len(dataset.series),
            observations=len(dataset.observations),
        )
        return dataset

    def load_current_observations(self) -> Dataset:
        """Load only the current-year data partition."""
        return self.load_dataset(data_files=CURRENT_DATA_FILES)

    def _populate_mappings(self, dataset: Dataset) -> Dataset:
        """Fetch and attach mapping tables (areas, items, periods, footnotes)."""
        for key, filename in MAPPING_FILES.items():
            log = logger.bind(mapping=key, filename=filename)
            log.debug("builder.mappings_fetch")
            text = self.client.get_text(filename)
            added = 0
            if key == "areas":
                for area in parser.parse_areas(text):
                    dataset.add_area(area)
                    added += 1
            elif key == "items":
                for item in parser.parse_items(text):
                    dataset.add_item(item)
                    added += 1
            elif key == "periods":
                for period in parser.parse_periods(text):
                    dataset.add_period(period)
                    added += 1
            elif key == "footnotes":
                for footnote in parser.parse_footnotes(text):
                    dataset.add_footnote(footnote)
                    added += 1
            log.debug("builder.mappings_loaded", count=added)
        return dataset

    def _populate_series(self, dataset: Dataset) -> Dataset:
        """Fetch and attach the CPI series metadata table."""
        series_text = self.client.get_text(SERIES_FILE)
        for series in parser.parse_series(series_text):
            dataset.add_series(series)
        logger.debug("builder.series_loaded", count=len(dataset.series))
        return dataset

    def _populate_observations(self, dataset: Dataset, files_to_fetch: Iterable[str]) -> Dataset:
        """Fetch observation partitions and append them to the dataset."""
        for filename in files_to_fetch:
            file_log = logger.bind(filename=filename)
            file_log.debug("builder.observations_fetch")
            text = self.client.get_text(filename)
            observations = parser.parse_observations(text)
            dataset.extend_observations(observations)
            file_log.debug("builder.observations_loaded", count=len(observations))
        return dataset

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self.client.close()
        logger.debug("builder.client_closed")


__all__ = ["CpiDatasetBuilder"]
