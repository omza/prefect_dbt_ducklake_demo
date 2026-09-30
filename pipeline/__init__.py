"""Load settings and secrets from the project's .env file.

This runs before any pipeline module is imported, so values from .env are in
os.environ before Prefect reads its own settings (e.g. PREFECT_API_URL) and
before dbt is started. Variables already set in the shell are not overridden.
"""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
