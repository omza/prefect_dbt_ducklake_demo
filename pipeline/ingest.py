"""Extract + Load: pull daily weather from the Open-Meteo API into DuckLake.

The raw layer stores data exactly as delivered (plus a load timestamp).
All cleaning and modelling happens later in dbt.
"""

from datetime import date, datetime, timezone

import httpx
from prefect import task
from prefect.logging import get_run_logger

from pipeline.config import RAW_SCHEMA, City
from pipeline import lake

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "weather_code",
]


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def extract_city_weather(city: City, start_date: date, end_date: date) -> list[dict]:
    """Fetch one city's daily weather and return it as a list of rows."""
    response = httpx.get(
        ARCHIVE_URL,
        params={
            "latitude": city.latitude,
            "longitude": city.longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": "UTC",
        },
        timeout=30,
    )
    response.raise_for_status()
    daily = response.json()["daily"]

    rows = [
        {
            "city": city.name,
            "country": city.country,
            "latitude": city.latitude,
            "longitude": city.longitude,
            "date": day,
            **{var: daily[var][i] for var in DAILY_VARIABLES},
        }
        for i, day in enumerate(daily["time"])
    ]
    print(f"{city.name}: fetched {len(rows)} days")
    return rows


@task
def load_raw_weather(rows: list[dict], start_date: date, end_date: date) -> int:
    """Write rows to lake.raw.weather_daily.

    The load is idempotent: rows in the requested date window are deleted
    first, so re-running the flow for the same dates never creates duplicates.
    Every write becomes a new DuckLake snapshot you can time-travel to.
    """
    logger = get_run_logger()
    loaded_at = datetime.now(timezone.utc)

    with lake.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.weather_daily (
                city VARCHAR,
                country VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                date DATE,
                temperature_2m_max DOUBLE,
                temperature_2m_min DOUBLE,
                temperature_2m_mean DOUBLE,
                precipitation_sum DOUBLE,
                wind_speed_10m_max DOUBLE,
                weather_code INTEGER,
                _loaded_at TIMESTAMPTZ
            )
        """)

        con.execute("BEGIN")
        con.execute(
            f"DELETE FROM {RAW_SCHEMA}.weather_daily WHERE date BETWEEN ? AND ?",
            [start_date, end_date],
        )
        con.executemany(
            f"""
            INSERT INTO {RAW_SCHEMA}.weather_daily VALUES
                ($city, $country, $latitude, $longitude, $date,
                 $temperature_2m_max, $temperature_2m_min, $temperature_2m_mean,
                 $precipitation_sum, $wind_speed_10m_max, $weather_code, $loaded_at)
            """,
            [{**row, "loaded_at": loaded_at} for row in rows],
        )
        con.execute("COMMIT")

    logger.info("Loaded %d rows into %s.weather_daily", len(rows), RAW_SCHEMA)
    return len(rows)
