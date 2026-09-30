"""Central settings for the demo pipeline.

Settings and secrets come from environment variables, which are loaded from
the project's .env file (see pipeline/__init__.py and .env.example). This is
the only module that reads them - everything else imports from here.
"""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _env_path(name: str, default: str) -> Path:
    """Read a path setting; relative paths are relative to the project root."""
    return (PROJECT_ROOT / _env(name, default)).resolve()


# Where the lakehouse lives on disk:
#   data/catalog.ducklake  -> DuckLake metadata catalog (a DuckDB file)
#   data/lake/             -> the actual table data, stored as Parquet files
DATA_DIR = _env_path("DUCKLAKE_DATA_DIR", "./data")
CATALOG_PATH = DATA_DIR / "catalog.ducklake"
LAKE_DATA_PATH = DATA_DIR / "lake"

DBT_PROJECT_DIR = PROJECT_ROOT / "transform"

OPEN_METEO_ARCHIVE_URL = _env("OPEN_METEO_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1/archive")

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
