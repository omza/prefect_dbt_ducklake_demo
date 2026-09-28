"""Peek inside the lakehouse: tables, a sample query, snapshots, time travel.

    uv run python -m pipeline.explore
"""

from pipeline import lake
from pipeline.config import LAKE_ALIAS


def show(con, title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    con.sql(sql).show(max_rows=20)


def main() -> None:
    with lake.connect() as con:
        show(con, "Tables in the lake", f"""
            select schema_name, table_name, estimated_size as rows
            from duckdb_tables() where database_name = '{LAKE_ALIAS}'
            order by all
        """)

        show(con, "Monthly summary (marts.agg_city_weather_monthly)", """
            select * from marts.agg_city_weather_monthly order by city, month
        """)

        # Every write (insert, delete, dbt run) creates a new snapshot.
        show(con, "DuckLake snapshots", f"""
            select snapshot_id, snapshot_time, changes
            from {LAKE_ALIAS}.snapshots() order by snapshot_id desc limit 10
        """)

        # Time travel: compare the raw table now vs. right after the first load.
        # (Small writes are "inlined" into the catalog instead of written as
        # Parquet files, so both kinds of insert count as a load.)
        first_load = con.sql(f"""
            select min(snapshot_id) from {LAKE_ALIAS}.snapshots()
            where list_has_any(map_keys(changes), ['inlined_insert', 'tables_inserted_into'])
        """).fetchone()[0]
        show(con, f"raw.weather_daily now vs. AT (VERSION => {first_load})", f"""
            select 'now' as version, count(*) as rows, min(date), max(date), max(_loaded_at)
            from raw.weather_daily
            union all
            select 'snapshot {first_load}', count(*), min(date), max(date), max(_loaded_at)
            from raw.weather_daily AT (VERSION => {first_load})
        """)


if __name__ == "__main__":
    main()
