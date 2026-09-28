"""The end-to-end pipeline: ingest -> transform, orchestrated by Prefect.

Run it once:            uv run python -m pipeline.flow
Backfill a date range:  uv run python -m pipeline.flow --start 2026-01-01 --end 2026-06-30
Run on a schedule:      uv run python -m pipeline.flow --serve
"""

import argparse
from datetime import date, timedelta

from prefect import flow

from pipeline.config import CITIES
from pipeline.ingest import extract_city_weather, load_raw_weather
from pipeline.transform import dbt_build

# The Open-Meteo archive lags a few days behind today.
ARCHIVE_LAG_DAYS = 5


@flow(name="weather-lakehouse", log_prints=True)
def weather_pipeline(start_date: date | None = None, end_date: date | None = None) -> None:
    end_date = end_date or date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    start_date = start_date or end_date - timedelta(days=30)
    print(f"Processing {start_date} -> {end_date} for {len(CITIES)} cities")

    # 1. Extract: one task per city, run concurrently.
    futures = extract_city_weather.map(CITIES, start_date=start_date, end_date=end_date)
    rows = [row for city_rows in futures.result() for row in city_rows]

    # 2. Load into the DuckLake raw layer.
    load_raw_weather(rows, start_date, end_date)

    # 3. Transform with dbt (staging -> marts, including tests).
    dbt_build()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--serve", action="store_true", help="serve the flow with a daily schedule")
    args = parser.parse_args()

    if args.serve:
        weather_pipeline.serve(name="daily", cron="0 6 * * *")
    else:
        weather_pipeline(start_date=args.start, end_date=args.end)


if __name__ == "__main__":
    main()
