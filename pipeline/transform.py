"""Transform: run the dbt project against DuckLake.

Also usable as a dbt wrapper outside Prefect, with settings from .env:

    uv run python -m pipeline.transform build
    uv run python -m pipeline.transform show --select agg_city_weather_monthly
"""

import os
import shutil
import subprocess
import sys

from prefect import task
from prefect.logging import get_run_logger

from pipeline.config import DATA_DIR, DBT_PROJECT_DIR


def dbt_command(args: list[str]) -> tuple[list[str], dict[str, str]]:
    """Build the dbt command line and environment for this project.

    dbt inherits the settings loaded from .env. DUCKLAKE_DATA_DIR is replaced
    with the resolved absolute path, because DuckLake stores the data path in
    its catalog and rejects a different spelling (e.g. "./data") later on.
    """
    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError("dbt executable not found - run `uv sync` first")

    cmd = [dbt, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    env = {**os.environ, "DUCKLAKE_DATA_DIR": str(DATA_DIR)}
    return cmd, env


@task
def dbt_build(select: str | None = None) -> None:
    """Run `dbt build` (seeds + models + tests, in dependency order).

    We call the dbt CLI as a subprocess and stream its output into the
    Prefect logs. A non-zero exit code (e.g. a failing test) fails the task.
    """
    logger = get_run_logger()
    cmd, env = dbt_command(["build", *(["--select", select] if select else [])])

    logger.info("Running: %s", " ".join(cmd))
    process = subprocess.Popen(
        cmd, cwd=DBT_PROJECT_DIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    for line in process.stdout:
        logger.info(line.rstrip())
    if process.wait() != 0:
        raise RuntimeError(f"dbt build failed with exit code {process.returncode}")


if __name__ == "__main__":
    cmd, env = dbt_command(sys.argv[1:])
    sys.exit(subprocess.run(cmd, cwd=DBT_PROJECT_DIR, env=env).returncode)
