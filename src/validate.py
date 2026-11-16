"""
src/validate.py
===============
PURPOSE: Run data quality checks AFTER the data has been loaded into MySQL.

WHAT THIS FILE DOES (in plain English):
  After loading data, we ask three questions:
  1. ROW COUNT CHECK: Did we actually load any rows today?
     (Catches API issues, empty responses, failed loads)
  2. NULL CHECK: Are there any NULL values in critical columns?
     (Catches data quality issues from the source API)
  3. DUPLICATE CHECK: Are there any (city, date) duplicates in the DB?
     (Verifies our ON DUPLICATE KEY UPDATE is working correctly)

  For each check, we log PASS or FAIL.
  If any check FAILS, we raise an exception so Airflow marks the task as failed
  and can alert you.

HOW TO TEST THIS FILE STANDALONE:
  Requires MySQL with data loaded. Then:
  python -m src.validate
"""

import logging

import sqlalchemy
from sqlalchemy import text

# ----------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------
logger = logging.getLogger("weather_pipeline.validate")


# ----------------------------------------------------------------
# Individual Quality Checks
# ----------------------------------------------------------------

def check_row_count(engine: sqlalchemy.Engine, min_expected_rows: int = 1) -> bool:
    """
    Check that the weather_daily table has at least `min_expected_rows` rows.

    Why this matters: If the table is empty or has very few rows, something
    likely went wrong in the extract or load step.

    Args:
        engine           : SQLAlchemy engine
        min_expected_rows: The minimum number of rows we expect (default: 1)

    Returns:
        True if the check passes, False otherwise.
    """
    logger.info("--- CHECK 1: Row Count ---")

    try:
        with engine.connect() as conn:
            # COUNT(*) returns the total number of rows in the table
            result = conn.execute(text("SELECT COUNT(*) FROM weather_daily"))
            total_rows = result.scalar()  # .scalar() gets the single value from the result

        if total_rows >= min_expected_rows:
            logger.info(f"  [PASS] weather_daily has {total_rows} rows "
                        f"(minimum expected: {min_expected_rows}).")
            return True
        else:
            logger.error(f"  [FAIL] weather_daily has only {total_rows} rows "
                         f"(minimum expected: {min_expected_rows}).")
            return False

    except Exception as e:
        logger.error(f"  [FAIL] Row count check threw an exception: {e}")
        return False


def check_no_nulls(engine: sqlalchemy.Engine) -> bool:
    """
    Check that critical columns (city, date, max_temp_c) have no NULL values.

    Why this matters: NULLs in key columns can break downstream queries and
    reports. We catch them here before they cause issues later.

    Args:
        engine : SQLAlchemy engine

    Returns:
        True if no nulls found, False otherwise.
    """
    logger.info("--- CHECK 2: Null Values ---")

    # Columns that must NEVER be null
    critical_columns = ["city", "date", "max_temp_c"]

    all_passed = True

    try:
        with engine.connect() as conn:
            for col in critical_columns:
                # Count how many rows have NULL in this column
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM weather_daily WHERE {col} IS NULL")
                )
                null_count = result.scalar()

                if null_count == 0:
                    logger.info(f"  [PASS] Column '{col}' has no NULLs.")
                else:
                    logger.error(f"  [FAIL] Column '{col}' has {null_count} NULL rows!")
                    all_passed = False

    except Exception as e:
        logger.error(f"  [FAIL] Null check threw an exception: {e}")
        return False

    return all_passed


def check_no_duplicates(engine: sqlalchemy.Engine) -> bool:
    """
    Check that there are no duplicate (city, date) pairs in weather_daily.

    Why this matters: Our ON DUPLICATE KEY UPDATE should prevent this,
    but this check verifies it's actually working as expected.

    How it works:
      We GROUP BY (city, date) and COUNT. If any group has COUNT > 1,
      those are duplicates.

    Args:
        engine : SQLAlchemy engine

    Returns:
        True if no duplicates found, False otherwise.
    """
    logger.info("--- CHECK 3: Duplicate Rows ---")

    # This query finds any (city, date) combinations that appear more than once
    duplicate_check_sql = text("""
        SELECT city, date, COUNT(*) AS count
        FROM weather_daily
        GROUP BY city, date
        HAVING COUNT(*) > 1
        LIMIT 10
    """)
    # LIMIT 10 so we don't overwhelm the logs if there are many duplicates

    try:
        with engine.connect() as conn:
            result = conn.execute(duplicate_check_sql)
            duplicates = result.fetchall()  # Returns a list of rows

        if not duplicates:
            logger.info("  [PASS] No duplicate (city, date) pairs found.")
            return True
        else:
            logger.error(f"  [FAIL] Found {len(duplicates)} duplicate (city, date) combinations!")
            for row in duplicates:
                logger.error(f"    - {row[0]} on {row[1]}: {row[2]} duplicates")
            return False

    except Exception as e:
        logger.error(f"  [FAIL] Duplicate check threw an exception: {e}")
        return False


# ----------------------------------------------------------------
# Main Validate Function
# ----------------------------------------------------------------

def run_validate(engine: sqlalchemy.Engine) -> None:
    """
    Run all data quality checks and raise an exception if any fail.

    We run ALL checks even if one fails (so we can see all issues at once),
    then raise at the end if there were any failures.

    Args:
        engine : SQLAlchemy engine connected to MySQL

    Raises:
        ValueError: If any quality check fails. This causes Airflow
                    to mark the validate task as FAILED.
    """
    logger.info("=" * 60)
    logger.info("VALIDATE STEP STARTED")
    logger.info("=" * 60)

    # Run all three checks and collect results
    checks = {
        "Row Count":  check_row_count(engine),
        "No Nulls":   check_no_nulls(engine),
        "No Duplicates": check_no_duplicates(engine),
    }

    # Count how many passed and failed
    passed = sum(1 for v in checks.values() if v)
    failed = sum(1 for v in checks.values() if not v)

    logger.info("=" * 60)
    logger.info(f"VALIDATE STEP COMPLETE: {passed} passed, {failed} failed.")
    logger.info("=" * 60)

    # If any check failed, raise an exception
    # This signals to Airflow that the pipeline has a data quality issue
    if failed > 0:
        failed_checks = [name for name, result in checks.items() if not result]
        raise ValueError(
            f"Data quality checks FAILED: {', '.join(failed_checks)}. "
            f"Check the logs for details."
        )

    logger.info("All data quality checks PASSED. Pipeline run successful!")


# ----------------------------------------------------------------
# Standalone Test
# ----------------------------------------------------------------
if __name__ == "__main__":
    """
    Quick test of validate checks.
    Requires MySQL with data loaded. Then:
    python -m src.validate
    """
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s - %(message)s")

    # Import get_engine from load.py
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.load import get_engine

    print("Running data quality checks on weather_daily table...")
    print("Make sure MySQL is running with some data loaded first.")

    try:
        engine = get_engine()
        run_validate(engine)
        print("\n[OK] All checks passed!")
    except ValueError as e:
        print(f"\n[FAIL] Quality check failed: {e}")
    except Exception as e:
        print(f"\n[ERROR] Could not connect to MySQL: {e}")
        print("Is MySQL running? Try: docker compose up mysql -d")
