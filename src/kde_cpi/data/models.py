"""Domain models for BLS Consumer Price Index (CU) survey flat files."""

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from typing import Any

import marshmallow as ma
from attrs import asdict as attrs_asdict, define, field


def _strip(value: str) -> str:
    """Trim surrounding whitespace from a field."""
    return value.strip()


@define(slots=True, frozen=True)
class Area:
    """Geographic area metadata from the CPI area lookup table."""

    code: str = field(converter=_strip)
    name: str = field(converter=_strip)


class AreaSchema(ma.Schema):
    """Marshmallow schema for serializing :class:`Area`."""

    code = ma.fields.Str(required=True)
    name = ma.fields.Str(required=True)

    @ma.post_load
    def make_area(self, data: dict[str, str], **kwargs: object) -> Area:
        """Convert validated payloads into :class:`Area` objects."""
        return Area(**data)


def _to_bool(value: str) -> bool:
    """Normalize BLS truthy strings (T, Y, 1) into booleans."""
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() in {"T", "TRUE", "1", "Y"}


@define(slots=True, frozen=True)
class Item:
    """Item metadata describing CPI product groupings."""

    code: str = field(converter=_strip)
    name: str = field(converter=_strip)
    display_level: int = field(converter=int)
    selectable: bool = field(converter=_to_bool)
    sort_sequence: int = field(converter=int)


class ItemSchema(ma.Schema):
    """Marshmallow schema for :class:`Item` records."""

    code = ma.fields.Str(required=True)
    name = ma.fields.Str(required=True)

    @ma.post_load
    def make_item(self, data: dict[str, str], **kwargs: object) -> Item:
        """Instantiate :class:`Item` from validated row data."""
        return Item(**data)


@define(slots=True, frozen=True)
class Period:
    """Period metadata describing CPI reporting intervals."""

    code: str = field(converter=_strip)
    abbr: str = field(converter=_strip)
    name: str = field(converter=_strip)


class PeriodSchema(ma.Schema):
    """Marshmallow schema for :class:`Period`."""

    code = ma.fields.Str(required=True)
    abbr = ma.fields.Str(required=True)
    name = ma.fields.Str(required=True)

    @ma.post_load
    def make_period(self, data: dict[str, str], **kwargs: object) -> Period:
        """Instantiate :class:`Period` objects from parsed data."""
        return Period(**data)


@define(slots=True, frozen=True)
class Footnote:
    """Footnote reference associated with CPI observations."""

    code: str = field(converter=_strip)
    text: str = field(converter=_strip)


class FootnoteSchema(ma.Schema):
    """Marshmallow schema for :class:`Footnote`."""

    code = ma.fields.Str(required=True)
    text = ma.fields.Str(required=True)

    @ma.post_load
    def make_footnote(self, data: dict[str, str], **kwargs: object) -> Footnote:
        """Instantiate :class:`Footnote` records."""
        return Footnote(**data)


@define(slots=True, frozen=True, kw_only=True)
class Series:
    """Metadata describing a CPI series and its structural attributes."""

    series_id: str = field(converter=_strip, kw_only=False)
    area_code: str = field(converter=_strip, kw_only=False)
    item_code: str = field(converter=_strip, kw_only=False)
    seasonal: str = field(converter=_strip, kw_only=False)
    periodicity_code: str = field(converter=_strip, kw_only=False)
    base_code: str = field(converter=_strip, kw_only=False)
    base_period: str = field(converter=_strip, kw_only=False)
    begin_year: int = field(converter=int, kw_only=False)
    begin_period: str = field(converter=_strip, kw_only=False)
    end_year: int = field(converter=int, kw_only=False)
    end_period: str = field(converter=_strip, kw_only=False)
    series_title: str = field(converter=_strip, default="")

    def is_seasonally_adjusted(self) -> bool:
        """Return True when the series is flagged as seasonally adjusted."""
        return self.seasonal.upper() == "S"


class SeriesSchema(ma.Schema):
    """Marshmallow schema for :class:`Series`."""

    series_id = ma.fields.Str(required=True)
    series_title = ma.fields.Str(required=False, allow_none=True, load_default="", dump_default="")
    area_code = ma.fields.Str(required=True)
    item_code = ma.fields.Str(required=True)
    seasonal = ma.fields.Str(required=True)
    periodicity_code = ma.fields.Str(required=True)
    base_code = ma.fields.Str(required=True)
    base_period = ma.fields.Str(required=True)
    begin_year = ma.fields.Int(required=True)
    begin_period = ma.fields.Str(required=True)
    end_year = ma.fields.Int(required=True)
    end_period = ma.fields.Str(required=True)

    @ma.post_load
    def make_series(self, data: dict[str, str], **kwargs: object) -> Series:
        """Instantiate :class:`Series` from validated payloads."""
        return Series(**data)


def _footnote_tuple(value: str) -> tuple[str, ...]:
    """Parse footnote codes into a normalized tuple."""
    value = value.strip()
    if not value:
        return ()
    # Footnote codes may be a comma-separated list or contain whitespace.
    tokens = [token.strip() for token in value.replace(",", " ").split()]
    return tuple(token for token in tokens if token)


def _decimal(value: str) -> Decimal:
    """Convert raw observation strings into :class:`Decimal` values."""
    value = value.strip()
    if not value:
        return Decimal("NaN")
    try:
        return Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise ValueError(f"Cannot parse decimal value from {value!r}") from exc


@define(slots=True, frozen=True)
class Observation:
    """Single CPI observation value tied to a series and period."""

    series_id: str = field(converter=_strip)
    year: int = field(converter=int)
    period: str = field(converter=_strip)
    value: Decimal = field(converter=_decimal)
    footnotes: tuple[str, ...] = field(converter=_footnote_tuple, factory=tuple)

    def is_annual(self) -> bool:
        """Return True when the observation corresponds to annual data."""
        return self.period.upper().startswith("M13") or self.period.upper().startswith("R13")


class ObservationSchema(ma.Schema):
    """Marshmallow schema for :class:`Observation`."""

    series_id = ma.fields.Str(required=True)
    year = ma.fields.Int(required=True)
    period = ma.fields.Str(required=True)
    value = ma.fields.Str(required=True)
    footnotes = ma.fields.Str(required=False, allow_none=True)

    @ma.post_load
    def make_observation(self, data: dict[str, str], **kwargs: object) -> Observation:
        """Instantiate :class:`Observation` with normalized payloads."""
        return Observation(
            series_id=data["series_id"],
            year=data["year"],
            period=data["period"],
            value=data["value"],
            footnotes=data.get("footnotes", ""),
        )


@define(slots=True)
class Dataset:
    """Aggregate CPI dataset containing mapping tables, series, and observations."""

    areas: dict[str, Area] = field(factory=dict)
    items: dict[str, Item] = field(factory=dict)
    periods: dict[str, Period] = field(factory=dict)
    footnotes: dict[str, Footnote] = field(factory=dict)
    series: dict[str, Series] = field(factory=dict)
    observations: list[Observation] = field(factory=list)
    _observation_keys: set[tuple[str, int, str]] = field(factory=set, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        """Populate the observation de-duplication index."""
        if not self.observations:
            return
        unique: list[Observation] = []
        for obs in self.observations:
            key = (obs.series_id, obs.year, obs.period)
            if key in self._observation_keys:
                continue
            self._observation_keys.add(key)
            unique.append(obs)
        if len(unique) != len(self.observations):
            self.observations = unique

    def add_area(self, area: Area) -> None:
        """Insert or update an area in the dataset."""
        self.areas[area.code] = area

    def add_item(self, item: Item) -> None:
        """Insert or update an item in the dataset."""
        self.items[item.code] = item

    def add_period(self, period: Period) -> None:
        """Insert or update a period definition."""
        self.periods[period.code] = period

    def add_footnote(self, footnote: Footnote) -> None:
        """Insert or update a footnote definition."""
        self.footnotes[footnote.code] = footnote

    def add_series(self, series: Series) -> None:
        """Insert or update a series metadata record."""
        self.series[series.series_id] = series

    def extend_observations(self, observations: Iterable[Observation]) -> None:
        """Append observation records to the dataset, dropping duplicates."""
        for obs in observations:
            key = (obs.series_id, obs.year, obs.period)
            if key in self._observation_keys:
                continue
            self._observation_keys.add(key)
            self.observations.append(obs)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly representation of the dataset."""
        return {
            "areas": [attrs_asdict(area) for area in self.areas.values()],
            "items": [attrs_asdict(item) for item in self.items.values()],
            "periods": [attrs_asdict(period) for period in self.periods.values()],
            "footnotes": [attrs_asdict(footnote) for footnote in self.footnotes.values()],
            "series": [attrs_asdict(series) for series in self.series.values()],
            "observations": [
                {
                    "series_id": obs.series_id,
                    "year": obs.year,
                    "period": obs.period,
                    "value": str(obs.value),
                    "footnotes": list(obs.footnotes),
                }
                for obs in self.observations
            ],
        }


class DatasetSchema(ma.Schema):
    """Marshmallow schema for serializing :class:`Dataset` collections."""

    areas = ma.fields.List(ma.fields.Nested(AreaSchema), required=True)
    items = ma.fields.List(ma.fields.Nested(ItemSchema), required=True)
    periods = ma.fields.List(ma.fields.Nested(PeriodSchema), required=True)
    footnotes = ma.fields.List(ma.fields.Nested(FootnoteSchema), required=True)
    series = ma.fields.List(ma.fields.Nested(SeriesSchema), required=True)
    observations = ma.fields.List(ma.fields.Nested(ObservationSchema), required=True)

    @ma.post_load
    def make_dataset(self, data: dict[str, Any], **kwargs: object) -> Dataset:
        """Instantiate :class:`Dataset` objects from validated payloads."""
        return Dataset(
            areas={area.code: area for area in data["areas"]},
            items={item.code: item for item in data["items"]},
            periods={period.code: period for period in data["periods"]},
            footnotes={footnote.code: footnote for footnote in data["footnotes"]},
            series={series.series_id: series for series in data["series"]},
            observations=data["observations"],
        )
