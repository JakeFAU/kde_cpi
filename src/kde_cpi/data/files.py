"""Constants describing CPI flat file naming."""

from attrs import define

BASE_URL = "https://download.bls.gov/pub/time.series/cu/"
SERIES_FILE = "cu.series"

MAPPING_FILES = {
    "areas": "cu.area",
    "items": "cu.item",
    "periods": "cu.period",
    "footnotes": "cu.footnote",
}

# Order matters: specific subsets include the general data partitions.
DATA_FILES = [
    "cu.data.0.Current",
    "cu.data.1.AllItems",
    "cu.data.2.Summaries",
    "cu.data.3.AsizeNorthEast",
    "cu.data.4.AsizeNorthCentral",
    "cu.data.5.AsizeSouth",
    "cu.data.6.AsizeWest",
    "cu.data.7.OtherNorthEast",
    "cu.data.8.OtherNorthCentral",
    "cu.data.9.OtherSouth",
    "cu.data.10.OtherWest",
    "cu.data.11.USFoodBeverage",
    "cu.data.12.USHousing",
    "cu.data.13.USApparel",
    "cu.data.14.USTransportation",
    "cu.data.15.USMedical",
    "cu.data.16.USRecreation",
    "cu.data.17.USEducationAndCommunication",
    "cu.data.18.USOtherGoodsAndServices",
    "cu.data.19.PopulationSize",
    "cu.data.20.USCommoditiesServicesSpecial",
]

CURRENT_DATA_FILES = ["cu.data.0.Current"]


@define(frozen=True)
class FileRequest:
    """Bundle describing a remote CPI resource."""

    name: str
    description: str
