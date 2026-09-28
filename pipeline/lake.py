"""Helpers for connecting to the DuckLake lakehouse from Python."""

from contextlib import contextmanager
from collections.abc import Iterator

import duckdb

from pipeline.config import CATALOG_PATH, LAKE_ALIAS, LAKE_DATA_PATH


@contextmanager
def connect() -> Iterator[duckdb.DuckDBPyConnection]:
    """Open an in-memory DuckDB session with the DuckLake catalog attached.

    DuckLake splits a table into two parts:
      * metadata (schemas, snapshots, file lists) in a catalog database
      * data as Parquet files under DATA_PATH

    With a DuckDB-file catalog only one process can write at a time, so we
    always close the connection before handing over to dbt.
    """
    LAKE_DATA_PATH.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.install_extension("ducklake")
        con.load_extension("ducklake")
        con.execute(
            f"ATTACH 'ducklake:{CATALOG_PATH}' AS {LAKE_ALIAS} (DATA_PATH '{LAKE_DATA_PATH}/')"
        )
        con.execute(f"USE {LAKE_ALIAS}")
        yield con
    finally:
        con.close()
