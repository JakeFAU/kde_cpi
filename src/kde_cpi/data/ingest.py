"""Orchestration utilities for assembling CPI datasets from flat files."""

from collections.abc import Iterable, Sequence

from attrs import define, field

from . import parser
from .client import CpiHttpClient
from .files import CURRENT_DATA_FILES, DATA_FILES, MAPPING_FILES, SERIES_FILE
from .models import Dataset


@define(slots=True)
class CpiDatasetBuilder:
    """Coordinate retrieval and parsing of CPI datasets from BLS flat files."""

    client: CpiHttpClient = field(factory=CpiHttpClient)

    def load_dataset(self, *, data_files: Sequence[str] | None = None) -> Dataset:
        """Fetch mapping tables, series definitions, and observations into a dataset."""
        dataset = Dataset()
        files_to_fetch = data_files or DATA_FILES

        # Load mapping tables first so downstream consumers can resolve codes.
        dataset = self._populate_mappings(dataset)
        dataset = self._populate_series(dataset)
        dataset = self._populate_observations(dataset, files_to_fetch)
        return dataset

    def load_current_observations(self) -> Dataset:
        """Load only the current-year data partition."""
        return self.load_dataset(data_files=CURRENT_DATA_FILES)

    def _populate_mappings(self, dataset: Dataset) -> Dataset:
        """Fetch and attach mapping tables (areas, items, periods, footnotes)."""
        for key, filename in MAPPING_FILES.items():
            text = self.client.get_text(filename)
            if key == "areas":
                for area in parser.parse_areas(text):
                    dataset.add_area(area)
            elif key == "items":
                for item in parser.parse_items(text):
                    dataset.add_item(item)
            elif key == "periods":
                for period in parser.parse_periods(text):
                    dataset.add_period(period)
            elif key == "footnotes":
                for footnote in parser.parse_footnotes(text):
                    dataset.add_footnote(footnote)
        return dataset

    def _populate_series(self, dataset: Dataset) -> Dataset:
        """Fetch and attach the CPI series metadata table."""
        series_text = self.client.get_text(SERIES_FILE)
        for series in parser.parse_series(series_text):
            dataset.add_series(series)
        return dataset

    def _populate_observations(self, dataset: Dataset, files_to_fetch: Iterable[str]) -> Dataset:
        """Fetch observation partitions and append them to the dataset."""
        for filename in files_to_fetch:
            text = self.client.get_text(filename)
            observations = parser.parse_observations(text)
            dataset.extend_observations(observations)
        return dataset

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self.client.close()


__all__ = ["CpiDatasetBuilder"]
