"""
src/load.py
===========
PURPOSE: Load the clean DataFrame into the MySQL weather_daily table.

WHAT THIS FILE DOES (in plain English):
  1. Takes the clean DataFrame from transform.py.
  2. For each row, runs an INSERT ... ON DUPLICATE KEY UPDATE query.
     - If the (city, date) pair doesn't exist: INSERT a new row.
     - If the (city, date) pair already exists: UPDATE the values.
     This makes the pipeline IDEMPOTENT — safe to re-run any time.
  3. After loading, updates the pipeline_watermark table so the next run
     knows where to pick up from (INCREMENTAL loading).
  4. Logs the number of rows processed.

WHY NOT use df.to_sql()?
  pandas' df.to_sql() is simple but doesn't support "ON DUPLICATE KEY UPDATE".
  We write raw SQL to get this idempotency feature.

HOW TO TEST THIS FILE STANDALONE:
  Requires MySQL to be running. Then:
  python -m src.load
"""

import logging
from datetime import datetime, timezone

import pandas as pd
import sqlalchemy
from sqlalchemy import text

# ----------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------
logger = logging.getLogger("weather_pipeline.load")

# ----------------------------------------------------------------
# Database Connection Helper
# ----------------------------------------------------------------

def get_engine(
    host: str = "localhost",
    port: int = 3307,          # We use 3307 on the host to avoid conflicts with local MySQL
    database: str = "weather_db",
    user: str = "weather_user",
    password: str = "weather_pass",
) -> sqlalchemy.Engine:
    """
    Create and return a SQLAlchemy engine for MySQL.

    SQLAlchemy is an ORM and SQL toolkit. An "engine" is the connection
    factory — it manages the connection pool to the database.

    We use the PyMySQL driver: mysql+pymysql://user:pass@host:port/db

    Args:
        host     : MySQL server hostname (use "localhost" when testing locally)
        port     : MySQL server port (3307 maps to Docker container's 3306)
        database : The database to connect to
        user     : MySQL username
        password : MySQL password

    Returns:
        A SQLAlchemy Engine object ready to execute queries.
    """
    # Build the connection URL in the format SQLAlchemy expects
    connection_url = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    )

    logger.info(f"Creating DB engine: mysql+pymysql://{user}:***@{host}:{port}/{database}")

    # create_engine() does NOT open a connection immediately.
    # It only opens connections when you actually run a query.
    engine = sqlalchemy.create_engine(connection_url)

    return engine


# ----------------------------------------------------------------
# Main Load Function
# ----------------------------------------------------------------

def run_load(df: pd.DataFrame, engine: sqlalchemy.Engine) -> int:
    """
    Load the clean DataFrame into the weather_daily MySQL table.

    Uses INSERT ... ON DUPLICATE KEY UPDATE so that:
    - New (city, date) pairs are INSERTED as fresh rows.
    - Existing (city, date) pairs are UPDATED with new values.
    This ensures the pipeline is idempotent — re-running it
    won't create duplicates.

    Args:
        df     : The clean DataFrame from run_transform()
        engine : A SQLAlchemy engine connected to MySQL

    Returns:
        The number of rows that were processed (inserted or updated).
    """
    logger.info("=" * 60)
    logger.info("LOAD STEP STARTED")
    logger.info("=" * 60)

    # Guard: nothing to load
    if df is None or df.empty:
        logger.warning("Received empty DataFrame. Nothing to load.")
        return 0

    logger.info(f"Loading {len(df)} rows into weather_daily...")

    # ----------------------------------------------------------
    # The INSERT ... ON DUPLICATE KEY UPDATE SQL statement
    # ----------------------------------------------------------
    # This is the key to idempotency.
    #
    # HOW IT WORKS:
    #   - MySQL tries to INSERT the row.
    #   - If the UNIQUE KEY (city, date) already exists, MySQL
    #     executes the ON DUPLICATE KEY UPDATE part instead of
    #     throwing an error.
    #   - We update all non-key columns to the new values.
    #
    # :city, :date, etc. are named parameters — SQLAlchemy
    # will safely substitute the actual values, preventing SQL injection.
    insert_sql = text("""
        INSERT INTO weather_daily
            (city, date, max_temp_c, min_temp_c, precipitation_mm, wind_speed_kmh, ingested_at)
        VALUES
            (:city, :date, :max_temp_c, :min_temp_c, :precipitation_mm, :wind_speed_kmh, :ingested_at)
        ON DUPLICATE KEY UPDATE
            max_temp_c       = VALUES(max_temp_c),
            min_temp_c       = VALUES(min_temp_c),
            precipitation_mm = VALUES(precipitation_mm),
            wind_speed_kmh   = VALUES(wind_speed_kmh),
            ingested_at      = VALUES(ingested_at)
    """)

    rows_processed = 0

    try:
        # Open a database connection (automatically returned to pool when done)
        with engine.begin() as conn:
            # engine.begin() opens a TRANSACTION.
            # If any error occurs, the whole batch is ROLLED BACK automatically.

            # Convert the DataFrame to a list of dicts for SQLAlchemy
            # Each dict = one row = one set of named parameters for the SQL
            records = df.to_dict(orient="records")

            # Execute the INSERT for every row
            for record in records:
                # Convert Python date to string (MySQL DATE format)
                record["date"] = str(record["date"])

                # Convert pandas Timestamp to Python datetime for compatibility
                if hasattr(record.get("ingested_at"), "to_pydatetime"):
                    record["ingested_at"] = record["ingested_at"].to_pydatetime()

                conn.execute(insert_sql, record)
                rows_processed += 1

        logger.info(f"Successfully loaded {rows_processed} rows into weather_daily.")

    except Exception as e:
        # Log the full error and re-raise so the caller (Airflow) knows it failed
        logger.error(f"Failed to load data into MySQL: {e}")
        raise

    # ----------------------------------------------------------
    # Update the watermark table
    # ----------------------------------------------------------
    # After a successful load, record the latest date per city.
    # Next run will start from (this date + 1 day).
    _update_watermarks(df, engine)

    return rows_processed


# ----------------------------------------------------------------
# Helper: Update the watermark table
# ----------------------------------------------------------------

def _update_watermarks(df: pd.DataFrame, engine: sqlalchemy.Engine) -> None:
    """
    Update the pipeline_watermark table with the latest loaded date per city.

    For each city in the loaded DataFrame:
    - Find the maximum date that was loaded.
    - INSERT or UPDATE the watermark for that city.

    We use INSERT ... ON DUPLICATE KEY UPDATE here too —
    the pipeline_watermark table has UNIQUE on (city).

    Args:
        df     : The DataFrame that was just loaded
        engine : SQLAlchemy engine
    """
    # SQL to upsert the watermark
    watermark_sql = text("""
        INSERT INTO pipeline_watermark (city, last_loaded_date, updated_at)
        VALUES (:city, :last_loaded_date, :updated_at)
        ON DUPLICATE KEY UPDATE
            last_loaded_date = VALUES(last_loaded_date),
            updated_at       = VALUES(updated_at)
    """)

    # Find the latest date loaded for each city
    # e.g., {"Delhi": date(2024, 1, 15), "Mumbai": date(2024, 1, 15)}
    city_max_dates = df.groupby("city")["date"].max()

    try:
        with engine.begin() as conn:
            for city, max_date in city_max_dates.items():
                conn.execute(watermark_sql, {
                    "city":             city,
                    "last_loaded_date": str(max_date),
                    "updated_at":       datetime.now(timezone.utc).replace(tzinfo=None),
                })
                logger.info(f"[{city}] Watermark updated to {max_date}.")

    except Exception as e:
        # Log but don't re-raise: loading succeeded, only watermark update failed.
        # Next run might re-load some data, but it's idempotent so that's OK.
        logger.error(f"Failed to update watermark table: {e}")


# ----------------------------------------------------------------
# Standalone Test
# ----------------------------------------------------------------
if __name__ == "__main__":
    """
    Quick test: load some fake data into MySQL.
    Requires MySQL to be running (via docker compose).
    Usage: python -m src.load
    """
    import logging
    from datetime import date
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s - %(message)s")

    print("Testing load with mock DataFrame...")
    print("Make sure MySQL is running: docker compose up mysql -d")

    # Create a small test DataFrame
    mock_df = pd.DataFrame({
        "city":             ["Delhi", "Mumbai"],
        "date":             [date(2024, 1, 10), date(2024, 1, 10)],
        "max_temp_c":       [22.5, 30.5],
        "min_temp_c":       [10.0, 22.0],
        "precipitation_mm": [0.0, 10.0],
        "wind_speed_kmh":   [15.0, 20.0],
        "ingested_at":      [datetime.now(timezone.utc).replace(tzinfo=None),
                             datetime.now(timezone.utc).replace(tzinfo=None)],
    })

    try:
        engine = get_engine()
        rows = run_load(mock_df, engine)
        print(f"\n[OK] Loaded {rows} rows successfully!")
        print("Run this twice -- the second run should UPDATE, not INSERT duplicates.")
    except Exception as e:
        print(f"\n[FAIL] Load failed: {e}")
        print("Is MySQL running? Try: docker compose up mysql -d")
