"""Transform: run the dbt project against DuckLake."""

import os
import shutil
import subprocess

from prefect import task
from prefect.logging import get_run_logger

from pipeline.config import DATA_DIR, DBT_PROJECT_DIR


@task
def dbt_build(select: str | None = None) -> None:
    """Run `dbt build` (seeds + models + tests, in dependency order).

    We call the dbt CLI as a subprocess and stream its output into the
    Prefect logs. A non-zero exit code (e.g. a failing test) fails the task.
    """
    logger = get_run_logger()
    dbt = shutil.which("dbt")
    if dbt is None:
        raise RuntimeError("dbt executable not found - run `uv sync` first")

    cmd = [dbt, "build", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    if select:
        cmd += ["--select", select]

    env = {**os.environ, "DUCKLAKE_DATA_DIR": str(DATA_DIR)}
    logger.info("Running: %s", " ".join(cmd))
    process = subprocess.Popen(
        cmd, cwd=DBT_PROJECT_DIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    for line in process.stdout:
        logger.info(line.rstrip())
    if process.wait() != 0:
        raise RuntimeError(f"dbt build failed with exit code {process.returncode}")
