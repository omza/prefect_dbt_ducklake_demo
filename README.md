# prefect_dbt_ducklake_demo

A small, fully local lakehouse for learning how to build a data pipeline:

| Layer         | Tool                                  | What it does here                                  |
|---------------|---------------------------------------|----------------------------------------------------|
| Orchestration | [Prefect 3](https://docs.prefect.io)  | Runs the steps in order, retries, logs, schedules  |
| Ingest (EL)   | Python + `httpx` + `duckdb`           | Pulls daily weather from the Open-Meteo API        |
| Transform (T) | [dbt v2](https://docs.getdbt.com)     | SQL models + tests, staging → marts                |
| Storage       | [DuckLake](https://ducklake.select)   | Lakehouse format: Parquet files + a SQL catalog    |

```
 Open-Meteo API
       │  extract_city_weather  (Prefect task, one per city, runs concurrently)
       ▼
 lake.raw.weather_daily          ◄── load_raw_weather (idempotent delete + insert)
       │  dbt build
       ▼
 lake.staging.stg_weather_daily  (view: rename, type, clean)
       │        + lake.reference.weather_codes (dbt seed)
       ▼
 lake.marts.fct_weather_daily        (table: one row per city/day)
 lake.marts.agg_city_weather_monthly (table: one row per city/month)
```

Everything is stored under `data/` (gitignored):

- `data/catalog.ducklake` is the DuckLake **catalog**: schemas, tables, snapshots, and which files belong to which table.
- `data/lake/` holds the **data** as Parquet files.

## Setup

You need [uv](https://docs.astral.sh/uv/).

```bash
uv sync                      # creates .venv with prefect, dbt v2, duckdb
uv run dbt --version         # dbt 2.0.x. The DuckDB adapter is built in.
cp .env.example .env         # local settings and secrets
```

### Configuration & secrets

All settings and secrets live in `.env`, which is gitignored. `.env.example` is the committed template, so add new variables there too.

| Variable                 | Used by           | Purpose                                              |
|--------------------------|-------------------|------------------------------------------------------|
| `DUCKLAKE_DATA_DIR`      | ingest, dbt       | Folder for the DuckLake catalog + Parquet files      |
| `OPEN_METEO_ARCHIVE_URL` | ingest            | Source API endpoint                                  |
| `PREFECT_API_URL`        | Prefect           | Optional: send runs to a persistent Prefect server   |

How it's wired:

- `pipeline/__init__.py` loads `.env` with `python-dotenv` before anything else, including Prefect, is imported.
- `pipeline/config.py` is the only module that reads the environment. Everything else imports from it.
- The dbt subprocess inherits the environment, and `catalogs.yml` reads `DUCKLAKE_DATA_DIR` via `env_var()`.
- Variables already set in your shell win over `.env`. For example, `DUCKLAKE_DATA_DIR=/tmp/lake uv run python -m pipeline.flow` writes to `/tmp/lake`.

For deployed flows, the next step is [Prefect Secret blocks](https://docs.prefect.io/v3/develop/blocks) instead of a file on disk.

## Run the pipeline

```bash
# last ~30 days (the archive API lags ~5 days behind today)
uv run python -m pipeline.flow

# backfill a date range; re-running the same range never creates duplicates
uv run python -m pipeline.flow --start 2026-01-01 --end 2026-09-20

# look inside the lake: tables, results, snapshots, time travel
uv run python -m pipeline.explore
```

### See runs in the Prefect UI

With no server configured, Prefect starts a temporary one for each run, so the run history disappears afterwards. To keep it:

```bash
uv run prefect server start                # terminal 1, UI at http://127.0.0.1:4200
# uncomment PREFECT_API_URL in .env
uv run python -m pipeline.flow             # terminal 2
uv run python -m pipeline.flow --serve     # or: run daily at 06:00
```

### Work on the dbt project on its own

```bash
# a thin wrapper around the dbt CLI that applies the settings from .env
uv run python -m pipeline.transform build                  # seeds + models + tests
uv run python -m pipeline.transform show --select agg_city_weather_monthly
uv run python -m pipeline.transform source freshness
```

dbt doesn't read `.env` itself. The wrapper also turns `DUCKLAKE_DATA_DIR` into an absolute path, because DuckLake saves the data path in its catalog and rejects a differently spelled one (such as `./data`) later on.

## Project layout

```
pipeline/
  __init__.py    loads .env (python-dotenv)
  config.py      settings from the environment + list of cities
  lake.py        connect() → DuckDB session with DuckLake attached as "lake"
  ingest.py      Prefect tasks: extract from API, load into raw
  transform.py   Prefect task: run `dbt build`
  flow.py        the Prefect flow wiring it all together
  explore.py     query the lake, list snapshots, time travel
transform/       dbt v2 project
  profiles.yml   compute: an in-memory DuckDB session
  catalogs.yml   storage: the DuckLake catalog "lake" (metadata + data path)
  dbt_project.yml  +catalog_name: lake → all models/seeds are DuckLake tables
  models/staging/  sources + staging models
  models/marts/    fact + aggregate tables
  seeds/           weather_codes.csv (WMO code → description)
  tests/           singular data tests
  macros/          generate_schema_name (schemas named raw/staging/marts)
```

## Things worth knowing

- **Only one process writes at a time.** A DuckDB-file catalog allows a single writer, so the ingest task closes its connection before dbt starts. If you need concurrent writers, move the catalog to Postgres (see exercises below).
- **Every write is a snapshot.** Each insert, delete, and dbt model build creates a DuckLake snapshot. You can query old versions with `SELECT ... FROM tbl AT (VERSION => 3)`.
- **Data inlining.** Small writes are stored inside the catalog instead of as tiny Parquet files (`inlined_insert` in the snapshot list). Run `CALL ducklake_flush_inlined_data('lake')` to write them out as Parquet.
- **dbt keeps compute and storage apart.** `profiles.yml` says *how* to run SQL (DuckDB in memory), and `catalogs.yml` says *where* tables live (DuckLake). To add a second lake, add another catalog entry and point some models at it with `catalog_name`.
- **dbt v2 is stricter than v1.** For example, source `freshness` / `loaded_at_field` must go under `config:`. Unknown keys are errors.

## Exercises to keep learning

1. **Add a city.** Edit `CITIES` in `pipeline/config.py`, backfill, and watch the marts update.
2. **Add a column.** Ingest `sunshine_duration` from the API and carry it through staging and marts. What does DuckLake do when a table's schema changes?
3. **Incremental model.** Make `fct_weather_daily` `materialized: incremental` so it only processes new days.
4. **Break a test.** Add a test that fails and watch the Prefect flow run fail.
5. **Time travel.** Use `AT (VERSION => n)` to compare a mart before and after a backfill.
6. **Maintenance.** Look up `ducklake_expire_snapshots` and `ducklake_merge_adjacent_files` and add a weekly Prefect flow that runs them.
7. **Postgres catalog.** Run Postgres in Docker and attach `ducklake:postgres:...` so multiple processes can write at once.
8. **Object storage.** Point `DATA_PATH` at S3 or MinIO instead of a local folder.
