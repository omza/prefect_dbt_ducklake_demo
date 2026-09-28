"""Central settings for the demo pipeline.

Every path can be overridden with an environment variable, so the same code
works on your laptop, in CI, or inside a Prefect worker.
"""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Where the lakehouse lives on disk:
#   data/catalog.ducklake  -> DuckLake metadata catalog (a DuckDB file)
#   data/lake/             -> the actual table data, stored as Parquet files
DATA_DIR = Path(os.environ.get("DUCKLAKE_DATA_DIR", PROJECT_ROOT / "data"))
CATALOG_PATH = DATA_DIR / "catalog.ducklake"
LAKE_DATA_PATH = DATA_DIR / "lake"

DBT_PROJECT_DIR = PROJECT_ROOT / "transform"

LAKE_ALIAS = "lake"
RAW_SCHEMA = "raw"


@dataclass(frozen=True)
class City:
    name: str
    country: str
    latitude: float
    longitude: float


CITIES = [
    City("Berlin", "DE", 52.52, 13.41),
    City("Hamburg", "DE", 53.55, 9.99),
    City("Munich", "DE", 48.14, 11.58),
    City("Vienna", "AT", 48.21, 16.37),
    City("Zurich", "CH", 47.37, 8.54),
]
