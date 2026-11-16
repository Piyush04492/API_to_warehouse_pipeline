"""
src/transform.py
================
PURPOSE: Clean and reshape the raw API data into a tidy pandas DataFrame.

WHAT THIS FILE DOES (in plain English):
  1. Takes the raw list of dicts returned by extract.py.
  2. For each city, "unpacks" the nested JSON into flat rows
     (one row = one day for one city).
  3. Renames columns to snake_case (e.g., temperature_2m_max → max_temp_c).
  4. Casts data types (dates as date, temperatures as float, etc.).
  5. Handles nulls: fills missing precipitation with 0 (no rain = 0 mm),
     drops rows where temperature is missing (we can't impute that).
  6. Removes any duplicate (city, date) pairs.
  7. Adds an ingested_at timestamp column.
  8. Returns a clean, ready-to-load DataFrame.

HOW TO TEST THIS FILE STANDALONE:
  python -m src.transform
"""

import logging
from datetime import datetime, timezone

import pandas as pd

# ----------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------
# We reuse the same logger name so messages go to the same log file
# that extract.py set up. If running standalone, basicConfig applies.
logger = logging.getLogger("weather_pipeline.transform")

# ----------------------------------------------------------------
# Column Renaming Map
# ----------------------------------------------------------------
# Maps Open-Meteo's API column names → our database column names.
# This makes the purpose of each column crystal clear.

COLUMN_RENAME_MAP = {
    "time":                  "date",              # Date string
    "temperature_2m_max":    "max_temp_c",        # Max temperature in Celsius
    "temperature_2m_min":    "min_temp_c",        # Min temperature in Celsius
    "precipitation_sum":     "precipitation_mm",  # Total precipitation in mm
    "windspeed_10m_max":     "wind_speed_kmh",    # Max wind speed in km/h
}


# ----------------------------------------------------------------
# Helper: Convert one city's raw API dict into a DataFrame
# ----------------------------------------------------------------

def parse_city_data(raw_data: dict) -> pd.DataFrame:
    """
    Convert a single city's raw API response into a flat DataFrame.

    The API returns data in this nested format:
      {
        "city": "Delhi",
        "daily": {
          "time":               ["2024-01-01", "2024-01-02", ...],
          "temperature_2m_max": [25.5, 24.1, ...],
          "temperature_2m_min": [10.2, 11.0, ...],
          ...
        }
      }

    We want to turn this into a flat table like:
      date        max_temp_c  min_temp_c  precipitation_mm  wind_speed_kmh  city
      2024-01-01  25.5        10.2        0.0               12.5            Delhi
      2024-01-02  24.1        11.0        2.3               8.2             Delhi

    Args:
        raw_data : One city's raw API response dict (from extract.py)

    Returns:
        A pandas DataFrame with one row per day for this city.
    """
    city_name = raw_data["city"]
    daily_data = raw_data["daily"]  # The nested dict with all the arrays

    # pd.DataFrame(daily_data) converts the dict of arrays into a table.
    # Each key becomes a column, each value (a list) becomes the column's values.
    df = pd.DataFrame(daily_data)

    # Add a column for the city name so we know which city each row belongs to
    df["city"] = city_name

    logger.debug(f"[{city_name}] Parsed {len(df)} rows from API response.")

    return df


# ----------------------------------------------------------------
# Main Transform Function
# ----------------------------------------------------------------

def run_transform(all_city_data: list[dict]) -> pd.DataFrame:
    """
    Transform raw data for ALL cities into a single clean DataFrame.

    Steps:
      1. Parse each city's data into a DataFrame
      2. Stack all cities into one big DataFrame
      3. Rename columns
      4. Fix data types
      5. Handle nulls
      6. Remove duplicates
      7. Add ingested_at timestamp

    Args:
        all_city_data : List of raw API dicts from run_extract()

    Returns:
        A clean pandas DataFrame ready to be loaded into MySQL.
        Returns an empty DataFrame if input is empty.
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM STEP STARTED")
    logger.info("=" * 60)
    logger.info(f"Received data for {len(all_city_data)} cities.")

    # Guard: if extract returned nothing, return an empty DataFrame
    if not all_city_data:
        logger.warning("No data received from extract step. Returning empty DataFrame.")
        return pd.DataFrame()

    # ----------------------------------------------------------
    # Step 1: Parse each city and combine into one DataFrame
    # ----------------------------------------------------------
    city_dataframes = []
    for raw_data in all_city_data:
        city_df = parse_city_data(raw_data)
        city_dataframes.append(city_df)

    # pd.concat stacks all city DataFrames vertically (row-wise)
    # ignore_index=True resets the index from 0, 1, 2, ... for the combined df
    df = pd.concat(city_dataframes, ignore_index=True)
    logger.info(f"Combined DataFrame shape: {df.shape} (rows × columns)")
    logger.info(f"Columns before rename: {list(df.columns)}")

    # ----------------------------------------------------------
    # Step 2: Rename columns to our snake_case naming convention
    # ----------------------------------------------------------
    # We only rename columns that exist in both the DataFrame AND our map.
    # This avoids errors if the API ever changes a column name.
    rename_map = {k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    logger.info(f"Columns after rename: {list(df.columns)}")

    # ----------------------------------------------------------
    # Step 3: Fix data types
    # ----------------------------------------------------------
    # "date" column: convert string "2024-01-15" → Python date object
    # errors="coerce" means: if a value can't be parsed, make it NaT (null)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Temperature and weather columns: convert to float
    # errors="coerce" means: if a value can't be cast to float, make it NaN (null)
    numeric_columns = ["max_temp_c", "min_temp_c", "precipitation_mm", "wind_speed_kmh"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Data types fixed.")

    # ----------------------------------------------------------
    # Step 4: Handle nulls (missing values)
    # ----------------------------------------------------------
    rows_before = len(df)

    # Rule 1: Fill missing precipitation with 0.
    # Reasoning: If the API has no precipitation data, it most likely means
    # there was no rain. This is a safe assumption for a data pipeline.
    if "precipitation_mm" in df.columns:
        df["precipitation_mm"] = df["precipitation_mm"].fillna(0.0)

    # Rule 2: Drop rows where the date is missing.
    # We can't load a row into the DB without a valid date.
    df = df.dropna(subset=["date"])

    # Rule 3: Drop rows where BOTH temperature columns are null.
    # A weather row without any temperature is not useful.
    if "max_temp_c" in df.columns and "min_temp_c" in df.columns:
        df = df.dropna(subset=["max_temp_c", "min_temp_c"], how="all")

    rows_after = len(df)
    logger.info(f"Null handling: dropped {rows_before - rows_after} rows. "
                f"Remaining: {rows_after} rows.")

    # ----------------------------------------------------------
    # Step 5: Remove duplicate (city, date) pairs
    # ----------------------------------------------------------
    # This can happen if the API returns overlapping date ranges.
    rows_before = len(df)
    df = df.drop_duplicates(subset=["city", "date"], keep="first")
    rows_after = len(df)
    logger.info(f"Deduplication: removed {rows_before - rows_after} duplicate rows. "
                f"Remaining: {rows_after} rows.")

    # ----------------------------------------------------------
    # Step 6: Add ingested_at timestamp
    # ----------------------------------------------------------
    # This records exactly when this row was processed by our pipeline.
    # We use UTC time for consistency.
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    # ----------------------------------------------------------
    # Step 7: Keep only the columns we need (in the right order)
    # ----------------------------------------------------------
    final_columns = ["city", "date", "max_temp_c", "min_temp_c",
                     "precipitation_mm", "wind_speed_kmh", "ingested_at"]

    # Only keep columns that actually exist (defensive programming)
    final_columns = [c for c in final_columns if c in df.columns]
    df = df[final_columns]

    logger.info(f"TRANSFORM STEP COMPLETE. Final DataFrame: {len(df)} rows x {len(df.columns)} columns.")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Cities: {df['city'].unique().tolist()}")

    return df


# ----------------------------------------------------------------
# Standalone Test
# ----------------------------------------------------------------
if __name__ == "__main__":
    """
    Quick test of the transform step using fake/mock data.
    Usage: python -m src.transform
    """
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname)s - %(message)s")

    print("Testing transform with mock data...")

    # Fake data that mimics what the Open-Meteo API returns
    mock_raw_data = [
        {
            "city": "Delhi",
            "daily": {
                "time":                  ["2024-01-10", "2024-01-11", "2024-01-12"],
                "temperature_2m_max":    [22.5, 23.1, None],  # None = missing value
                "temperature_2m_min":    [10.0, 11.5, 9.8],
                "precipitation_sum":     [0.0, None, 5.2],    # None = should become 0.0
                "windspeed_10m_max":     [15.0, 12.3, 18.7],
            }
        },
        {
            "city": "Mumbai",
            "daily": {
                "time":                  ["2024-01-10", "2024-01-11"],
                "temperature_2m_max":    [30.5, 31.0],
                "temperature_2m_min":    [22.0, 22.5],
                "precipitation_sum":     [10.0, 0.5],
                "windspeed_10m_max":     [20.0, 18.0],
            }
        }
    ]

    result_df = run_transform(mock_raw_data)

    print("\n[OK] Transform result:")
    print(result_df.to_string(index=False))
    print(f"\nData types:\n{result_df.dtypes}")
