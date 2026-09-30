"""Open the lakehouse in the DuckDB web UI (a SQL notebook in your browser).

    uv run python -m pipeline.ui              # opens http://localhost:4213
    uv run python -m pipeline.ui --no-browser

The lake is attached as the `lake` database, e.g.:

    from lake.marts.agg_city_weather_monthly;
    from lake.snapshots();

While the UI runs, this process holds the DuckLake catalog, so stop it
(Ctrl+C) before running the pipeline flow.
"""

import argparse
import time

from pipeline import lake


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-browser", action="store_true", help="start the server without opening a browser")
    args = parser.parse_args()

    with lake.connect() as con:
        # The ui extension for DuckDB 1.5.6 is only published on the nightly
        # extension repository so far. Your data stays local, but the UI's
        # web page itself is loaded from ui.duckdb.org.
        con.execute("INSTALL ui FROM core_nightly")
        con.load_extension("ui")
        message = con.sql("CALL start_ui_server()" if args.no_browser else "CALL start_ui()").fetchone()[0]
        print(f"{message}\nPress Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            con.execute("CALL stop_ui_server()")
            print("\nUI stopped.")


if __name__ == "__main__":
    main()
