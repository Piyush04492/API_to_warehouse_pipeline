"""
dags/weather_pipeline_dag.py
=============================
PURPOSE: Define the Airflow DAG that orchestrates our weather ETL pipeline.

WHAT THIS FILE DOES (in plain English):
  This file tells Airflow HOW and WHEN to run our pipeline.

  Airflow reads all Python files in the /dags folder and looks for DAG objects.
  When it finds one, it shows it in the Airflow web UI.

  Our DAG has 4 tasks that run in order:
    extract_task  →  transform_task  →  load_task  →  validate_task

  Each task runs one of our src/ functions.
  Tasks pass data to each other using XCom (Airflow's key-value store).

  SCHEDULE: Runs once per day at midnight UTC.
  RETRIES:  If a task fails, Airflow retries it 2 more times,
            waiting 5 minutes between each attempt.

HOW TO VIEW THIS IN AIRFLOW:
  1. docker compose up
  2. Open http://localhost:8080
  3. Login: admin / admin
  4. Find "weather_daily_pipeline" DAG and enable it.
  5. Click "Trigger DAG" to run it now.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------
# Path Setup
# ----------------------------------------------------------------
# In Airflow's Docker container, we need to tell Python where to
# find our src/ modules. We add the project root to the Python path.
# The project root is mounted at /opt/airflow in the container.
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Now we can import from our src/ package
from src.extract import run_extract
from src.transform import run_transform
from src.load import run_load, get_engine
from src.validate import run_validate

# ----------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------
# In Airflow, each task has its own log that you can view in the UI.
# We use Python's standard logging here.
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------
# Database Connection Settings
# ----------------------------------------------------------------
# When running inside Docker Compose, the MySQL hostname is "mysql"
# (the service name in docker-compose.yml), NOT "localhost".
# "localhost" would refer to the Airflow container itself.

DB_HOST     = os.environ.get("WEATHER_DB_HOST", "mysql")   # Docker service name
DB_PORT     = int(os.environ.get("WEATHER_DB_PORT", "3306"))  # Internal Docker port
DB_NAME     = os.environ.get("WEATHER_DB_NAME", "weather_db")
DB_USER     = os.environ.get("WEATHER_DB_USER", "weather_user")
DB_PASSWORD = os.environ.get("WEATHER_DB_PASSWORD", "weather_pass")


# ----------------------------------------------------------------
# Task Functions
# ----------------------------------------------------------------
# Each task is a plain Python function.
# Airflow calls these functions when it's time to run the task.
#
# "ti" is the Task Instance — Airflow passes it automatically.
# We use ti.xcom_push() to store data for the next task.
# We use ti.xcom_pull() to retrieve data from the previous task.
#
# IMPORTANT: XCom can only store small amounts of data (a few MB).
# For large datasets, you'd use a file or object storage instead.
# For our 5-city daily pipeline, it's perfectly fine.

def extract_task_fn(ti) -> None:
    """
    Task 1: Extract weather data from Open-Meteo API.

    Calls run_extract(), which fetches data for all 5 cities.
    Pushes the raw data list to XCom so transform_task can use it.

    XCom key: "raw_weather_data"
    XCom value: A JSON-serializable list of dicts
    """
    logger.info("Starting EXTRACT task...")

    # Create a database engine to read watermarks
    engine = get_engine(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    # Run the extract step
    raw_data = run_extract(engine)

    # Store the result in XCom for the next task
    # XCom requires JSON-serializable data, so we convert dates to strings
    # (The raw API data is already a list of dicts with strings — no conversion needed)
    ti.xcom_push(key="raw_weather_data", value=raw_data)

    logger.info(f"EXTRACT task complete. Pushed {len(raw_data)} city records to XCom.")


def transform_task_fn(ti) -> None:
    """
    Task 2: Transform raw data into a clean DataFrame.

    Pulls raw data from XCom (from extract_task).
    Calls run_transform() to clean and reshape the data.
    Pushes the result as a JSON string to XCom for load_task.

    XCom key: "clean_weather_json"
    XCom value: A JSON string (DataFrame serialized to JSON)
    """
    logger.info("Starting TRANSFORM task...")

    # Pull raw data from the previous task (extract_task)
    # task_ids tells XCom which task pushed the data we want
    raw_data = ti.xcom_pull(task_ids="extract_task", key="raw_weather_data")

    if not raw_data:
        logger.warning("No raw data received from extract task. Skipping transform.")
        ti.xcom_push(key="clean_weather_json", value=None)
        return

    # Run the transform step
    clean_df = run_transform(raw_data)

    # Serialize the DataFrame to JSON for XCom storage
    # orient="records" gives us a list of dicts: [{"city": "Delhi", ...}, ...]
    # date_format="iso" ensures dates are stored as ISO strings
    if clean_df is not None and not clean_df.empty:
        clean_json = clean_df.to_json(orient="records", date_format="iso")
        ti.xcom_push(key="clean_weather_json", value=clean_json)
        logger.info(f"TRANSFORM task complete. Pushed {len(clean_df)} rows to XCom.")
    else:
        ti.xcom_push(key="clean_weather_json", value=None)
        logger.warning("Transform produced empty DataFrame. Nothing to push.")


def load_task_fn(ti) -> None:
    """
    Task 3: Load the clean DataFrame into MySQL.

    Pulls the clean JSON from XCom (from transform_task).
    Deserializes it back into a DataFrame.
    Calls run_load() to insert/update rows in MySQL.

    XCom key: "rows_loaded"
    XCom value: Number of rows loaded (int)
    """
    import pandas as pd
    from io import StringIO

    logger.info("Starting LOAD task...")

    # Pull cleaned data from the transform task
    clean_json = ti.xcom_pull(task_ids="transform_task", key="clean_weather_json")

    if not clean_json:
        logger.warning("No clean data received from transform task. Skipping load.")
        ti.xcom_push(key="rows_loaded", value=0)
        return

    # Deserialize JSON back into a pandas DataFrame
    clean_df = pd.read_json(StringIO(clean_json), orient="records")

    # Fix the date column back to Python date objects
    # (JSON serialization converts dates to strings or timestamps)
    clean_df["date"] = pd.to_datetime(clean_df["date"]).dt.date

    # Create the DB engine for MySQL (use Docker service name "mysql")
    engine = get_engine(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    # Run the load step
    rows_loaded = run_load(clean_df, engine)

    # Store result for the validate task (or just for logging)
    ti.xcom_push(key="rows_loaded", value=rows_loaded)

    logger.info(f"LOAD task complete. Loaded {rows_loaded} rows.")


def validate_task_fn(ti) -> None:
    """
    Task 4: Run data quality checks on the loaded data.

    Runs three checks on the weather_daily table:
    1. Row count >= 1
    2. No NULLs in critical columns
    3. No duplicate (city, date) pairs

    If any check fails, raises ValueError — Airflow marks the task as FAILED.
    """
    logger.info("Starting VALIDATE task...")

    # Quick sanity check: how many rows were loaded in this run?
    rows_loaded = ti.xcom_pull(task_ids="load_task", key="rows_loaded")
    logger.info(f"Rows loaded in this run: {rows_loaded}")

    # Create DB engine
    engine = get_engine(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    # Run validation — will raise ValueError if any check fails
    run_validate(engine)

    logger.info("VALIDATE task complete. All checks passed.")


# ----------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------
# A DAG (Directed Acyclic Graph) defines the pipeline structure.
# "Directed" = tasks have a defined order (extract before transform).
# "Acyclic"  = no loops (you can't go backwards).

# Default arguments applied to ALL tasks in this DAG
default_args = {
    # Who owns this DAG (shown in Airflow UI)
    "owner": "weather_team",

    # If the pipeline misses a scheduled run (e.g., Airflow was down),
    # don't try to catch up and run all missed runs at once.
    "depends_on_past": False,

    # Email alerts on failure (set to False for now — requires email config)
    "email_on_failure": False,
    "email_on_retry": False,

    # Retry configuration:
    # If a task fails, retry 2 more times (3 total attempts)
    "retries": 2,

    # Wait 5 minutes between retry attempts
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG object
# This is what Airflow discovers when it scans the dags/ folder
with DAG(
    # Unique name shown in the Airflow UI
    dag_id="weather_daily_pipeline",

    # Description shown in Airflow UI
    description="Daily ETL pipeline: fetch weather for 5 Indian cities and load into MySQL",

    # Default arguments (defined above)
    default_args=default_args,

    # When to start scheduling this DAG
    # We set it to 1 day ago so it's immediately schedulable
    start_date=datetime(2024, 1, 1),

    # Run once per day at midnight UTC
    # @daily is shorthand for the cron expression "0 0 * * *"
    schedule_interval="@daily",

    # Don't run catch-up jobs for all past dates since start_date
    # This prevents a flood of runs when you first enable the DAG
    catchup=False,

    # Tags help organize DAGs in the Airflow UI
    tags=["weather", "etl", "beginner"],

) as dag:

    # ----------------------------------------------------------------
    # Task 1: Extract
    # ----------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extract_task",    # Unique name within this DAG
        python_callable=extract_task_fn,  # The function to call
    )

    # ----------------------------------------------------------------
    # Task 2: Transform
    # ----------------------------------------------------------------
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task_fn,
    )

    # ----------------------------------------------------------------
    # Task 3: Load
    # ----------------------------------------------------------------
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_task_fn,
    )

    # ----------------------------------------------------------------
    # Task 4: Validate
    # ----------------------------------------------------------------
    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_task_fn,
    )

    # ----------------------------------------------------------------
    # Task Dependencies (the pipeline order)
    # ----------------------------------------------------------------
    # The >> operator means "run this task AFTER the one on the left".
    # This creates the pipeline: extract → transform → load → validate
    extract_task >> transform_task >> load_task >> validate_task
