-- =============================================================
-- create_tables.sql
-- Purpose : Create the two tables our pipeline uses.
--
-- Table 1: weather_daily
--   Stores cleaned daily weather records for each city.
--   The UNIQUE KEY on (city, date) is what makes our pipeline
--   idempotent — MySQL will UPDATE instead of INSERT if the
--   same (city, date) pair is loaded again.
--
-- Table 2: pipeline_watermark
--   Stores the last date we successfully loaded for each city.
--   On the next run, we only fetch data AFTER this date,
--   making the pipeline incremental (no re-processing old data).
-- =============================================================

-- Create the database if it doesn't exist yet
CREATE DATABASE IF NOT EXISTS weather_db;

-- Always work inside weather_db
USE weather_db;

-- -------------------------------------------------------------
-- Table 1: weather_daily
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_daily (
    -- Auto-incrementing primary key (MySQL best practice)
    id            INT AUTO_INCREMENT PRIMARY KEY,

    -- The city name (e.g., "Delhi")
    city          VARCHAR(100) NOT NULL,

    -- The date this weather record is for (not the ingestion date)
    date          DATE         NOT NULL,

    -- Maximum temperature for the day, in degrees Celsius
    max_temp_c    FLOAT,

    -- Minimum temperature for the day, in degrees Celsius
    min_temp_c    FLOAT,

    -- Total precipitation (rain + snow) for the day, in mm
    precipitation_mm FLOAT,

    -- Maximum wind speed for the day, in km/h
    wind_speed_kmh   FLOAT,

    -- Timestamp of when this row was inserted/updated by our pipeline
    -- DEFAULT CURRENT_TIMESTAMP fills this automatically on INSERT
    ingested_at   DATETIME     DEFAULT CURRENT_TIMESTAMP,

    -- UNIQUE KEY is the heart of idempotency.
    -- MySQL will REJECT a second INSERT for the same city+date,
    -- and we use "ON DUPLICATE KEY UPDATE" to UPDATE it instead.
    UNIQUE KEY uq_city_date (city, date)
);

-- -------------------------------------------------------------
-- Table 2: pipeline_watermark
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    -- Auto-incrementing primary key
    id              INT AUTO_INCREMENT PRIMARY KEY,

    -- One row per city
    city            VARCHAR(100) NOT NULL UNIQUE,

    -- The most recent date that was successfully loaded for this city.
    -- Next run: fetch everything AFTER this date.
    last_loaded_date DATE        NOT NULL,

    -- When we last updated this watermark
    updated_at      DATETIME    DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP
);
