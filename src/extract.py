"""
src/extract.py
==============
PURPOSE: Fetch raw daily weather data from the Open-Meteo API.

WHAT THIS FILE DOES (in plain English):
  1. Defines which 5 Indian cities to fetch and their lat/lon coordinates.
  2. Looks up the pipeline_watermark table to find the last loaded date
     for each city. If no watermark exists, it defaults to 30 days ago
     (so the first run always backfills the last 30 days).
  3. Calls the Open-Meteo API for each city with a date range.
  4. If the API call fails, it retries up to 3 times with a delay.
  5. If a city still fails after retries, it logs the error and moves on
     to the next city — one bad city won't kill the whole pipeline.

HOW TO TEST THIS FILE STANDALONE (no Airflow needed):
  python -m src.extract
"""

import logging
import os
import time
from datetime import date, timedelta

import requests
import sqlalchemy

# ----------------------------------------------------------------
# Logging Setup
# ----------------------------------------------------------------
# We use Python's built-in logging module.
# This creates a logger named "weather_pipeline.extract".
# Log messages will appear in the console AND in a log file.

# Create the logs/ directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("weather_pipeline.extract")
logger.setLevel(logging.DEBUG)  # Capture all log levels (DEBUG and above)

# Handler 1: Print logs to the console (so we can see them while running)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Handler 2: Write logs to a file for later review
file_handler = logging.FileHandler("logs/pipeline.log")
file_handler.setLevel(logging.DEBUG)

# Format: [2024-01-15 10:30:00] INFO extract - Starting extraction...
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Attach handlers to our logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ----------------------------------------------------------------
# City Configuration
# ----------------------------------------------------------------
# Each city needs its latitude and longitude for the Open-Meteo API.
# Open-Meteo does not use city names — it uses coordinates.
# Source: https://open-meteo.com/en/docs

CITIES = {
    "Delhi":     {"latitude": 28.6139, "longitude": 77.2090},
    "Mumbai":    {"latitude": 19.0760, "longitude": 72.8777},
    "Bengaluru": {"latitude": 12.9716, "longitude": 77.5946},
    "Chennai":   {"latitude": 13.0827, "longitude": 80.2707},
    "Kolkata":   {"latitude": 22.5726, "longitude": 88.3639},
}

# The Open-Meteo API base URL for daily weather data
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Which weather variables to request from the API
# These map to our database columns
DAILY_VARIABLES = [
    "temperature_2m_max",    # → max_temp_c
    "temperature_2m_min",    # → min_temp_c
    "precipitation_sum",     # → precipitation_mm
    "windspeed_10m_max",     # → wind_speed_kmh
]

# Retry configuration for API calls
MAX_RETRIES = 3          # How many times to retry a failed API call
RETRY_DELAY_SECONDS = 5  # How many seconds to wait between retries


# ----------------------------------------------------------------
# Helper: Get the watermark date for a city
# ----------------------------------------------------------------

def get_watermark_date(city: str, engine: sqlalchemy.Engine) -> date:
    """
    Look up the last_loaded_date for a city in the pipeline_watermark table.

    If no watermark exists (first run), return 30 days ago.
    This ensures the first run backfills the last 30 days of data.

    Args:
        city   : The city name (e.g., "Delhi")
        engine : A SQLAlchemy engine connected to our MySQL database

    Returns:
        A Python date object — the day AFTER which we should fetch data.
    """
    # Default: 30 days ago (used only on the very first run)
    default_start = date.today() - timedelta(days=30)

    try:
        with engine.connect() as conn:
            # Query the watermark table for this specific city
            result = conn.execute(
                sqlalchemy.text(
                    "SELECT last_loaded_date FROM pipeline_watermark WHERE city = :city"
                ),
                {"city": city}
            )
            row = result.fetchone()

        if row:
            # Watermark found — start fetching from the day AFTER the last loaded date
            watermark = row[0]  # This is a datetime.date object
            start_date = watermark + timedelta(days=1)
            logger.info(f"[{city}] Watermark found: {watermark}. Fetching from {start_date}.")
            return start_date
        else:
            # No watermark — this is the first run for this city
            logger.info(f"[{city}] No watermark found. Backfilling last 30 days from {default_start}.")
            return default_start

    except Exception as e:
        # If we can't read the watermark (e.g., table missing), log and use default
        logger.warning(f"[{city}] Could not read watermark: {e}. Using default start date.")
        return default_start


# ----------------------------------------------------------------
# Helper: Call the Open-Meteo API (with retries)
# ----------------------------------------------------------------

def fetch_weather_for_city(city: str, latitude: float, longitude: float,
                           start_date: date, end_date: date) -> dict | None:
    """
    Call the Open-Meteo API for a single city and date range.
    Retries up to MAX_RETRIES times if the call fails.

    Args:
        city       : City name (used only for logging)
        latitude   : City's latitude coordinate
        longitude  : City's longitude coordinate
        start_date : Start of the date range to fetch
        end_date   : End of the date range to fetch (usually today)

    Returns:
        A dictionary with the raw API response, or None if all retries failed.
    """
    # Build the query parameters for the API request
    params = {
        "latitude":    latitude,
        "longitude":   longitude,
        "daily":       ",".join(DAILY_VARIABLES),  # Comma-separated list of variables
        "start_date":  start_date.strftime("%Y-%m-%d"),  # API expects "YYYY-MM-DD"
        "end_date":    end_date.strftime("%Y-%m-%d"),
        "timezone":    "Asia/Kolkata",  # Indian Standard Time
    }

    # Try the API call up to MAX_RETRIES times
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[{city}] API call attempt {attempt}/{MAX_RETRIES} "
                        f"for dates {start_date} to {end_date}")

            # Make the HTTP GET request
            # timeout=10 means: give up if the server doesn't respond in 10 seconds
            response = requests.get(OPEN_METEO_URL, params=params, timeout=10)

            # Raise an exception if the HTTP status code is 4xx or 5xx
            response.raise_for_status()

            # Parse the JSON response into a Python dictionary
            data = response.json()

            logger.info(f"[{city}] API call successful. "
                        f"Got {len(data.get('daily', {}).get('time', []))} days of data.")

            # Attach the city name to the data — we'll need it in transform.py
            data["city"] = city

            return data

        except requests.exceptions.Timeout:
            logger.warning(f"[{city}] Attempt {attempt} timed out.")

        except requests.exceptions.ConnectionError:
            logger.warning(f"[{city}] Attempt {attempt} failed: connection error.")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"[{city}] Attempt {attempt} failed: HTTP error {e}.")

        except Exception as e:
            logger.warning(f"[{city}] Attempt {attempt} failed: {e}.")

        # If not the last attempt, wait before retrying
        if attempt < MAX_RETRIES:
            logger.info(f"[{city}] Waiting {RETRY_DELAY_SECONDS}s before retry...")
            time.sleep(RETRY_DELAY_SECONDS)

    # All retries failed
    logger.error(f"[{city}] All {MAX_RETRIES} attempts failed. Skipping this city.")
    return None


# ----------------------------------------------------------------
# Main Extract Function
# ----------------------------------------------------------------

def run_extract(engine: sqlalchemy.Engine) -> list[dict]:
    """
    Run the extraction step for ALL cities.

    For each city:
      1. Get the watermark date (or default to 30 days ago)
      2. Call the Open-Meteo API
      3. If successful, add to results list
      4. If failed, log and skip

    Args:
        engine : A SQLAlchemy engine connected to MySQL

    Returns:
        A list of raw API response dicts, one per successful city.
        Example: [{"city": "Delhi", "daily": {...}}, ...]
    """
    logger.info("=" * 60)
    logger.info("EXTRACT STEP STARTED")
    logger.info("=" * 60)

    today = date.today()
    all_city_data = []

    for city, coords in CITIES.items():
        logger.info(f"--- Processing city: {city} ---")

        # Step 1: Find out which dates to fetch for this city
        start_date = get_watermark_date(city, engine)

        # If the start date is today or in the future, there's nothing new to fetch
        if start_date > today:
            logger.info(f"[{city}] Already up to date. No new data to fetch.")
            continue

        # Step 2: Call the API
        raw_data = fetch_weather_for_city(
            city=city,
            latitude=coords["latitude"],
            longitude=coords["longitude"],
            start_date=start_date,
            end_date=today,
        )

        # Step 3: Collect successful results
        if raw_data is not None:
            all_city_data.append(raw_data)
        # If raw_data is None, fetch_weather_for_city already logged the error
        # We simply move on to the next city (graceful degradation)

    logger.info(f"EXTRACT STEP COMPLETE. Successfully fetched data for "
                f"{len(all_city_data)}/{len(CITIES)} cities.")

    return all_city_data


# ----------------------------------------------------------------
# Standalone Test (run this file directly to test it)
# ----------------------------------------------------------------
if __name__ == "__main__":
    """
    Quick test: run this file directly to see if the API works.
    Usage: python -m src.extract
    (run from the project root directory)

    NOTE: This test does NOT need Airflow or MySQL to be running.
    It just prints the raw API response to show you the data shape.
    """
    print("Testing Open-Meteo API connection (no DB needed)...")

    # Test just one city without connecting to MySQL
    test_city = "Delhi"
    test_coords = CITIES[test_city]
    test_start = date.today() - timedelta(days=3)  # Last 3 days only (quick test)
    test_end = date.today()

    result = fetch_weather_for_city(
        city=test_city,
        latitude=test_coords["latitude"],
        longitude=test_coords["longitude"],
        start_date=test_start,
        end_date=test_end,
    )

    if result:
        print("\n[OK] API call succeeded! Raw data preview:")
        print(f"  City   : {result['city']}")
        print(f"  Dates  : {result['daily']['time']}")
        print(f"  Max temps: {result['daily']['temperature_2m_max']}")
        print(f"  Min temps: {result['daily']['temperature_2m_min']}")
    else:
        print("[FAIL] API call failed. Check your internet connection.")
