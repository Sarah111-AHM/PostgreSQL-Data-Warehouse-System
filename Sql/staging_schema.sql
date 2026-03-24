-- =============================================================================
-- staging_schema.sql
-- Staging schema DDL for the Data Warehousing Project
-- Run AFTER ingestion.py to verify or manually recreate staging tables.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- staging.weather_raw
-- One row per city per day as fetched from the Open-Meteo Archive API.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.weather_raw CASCADE;
CREATE TABLE staging.weather_raw (
    id            SERIAL       PRIMARY KEY,
    city_name     TEXT         NOT NULL,
    country_code  CHAR(2)      NOT NULL,
    latitude      NUMERIC(9,4),
    longitude     NUMERIC(9,4),
    date          DATE         NOT NULL,
    temp_max      NUMERIC(6,2),        -- Celsius
    temp_min      NUMERIC(6,2),        -- Celsius
    temp_mean     NUMERIC(6,2),        -- Celsius
    precipitation NUMERIC(8,2),        -- mm
    windspeed_max NUMERIC(6,2),        -- km/h
    weathercode   SMALLINT,            -- WMO code
    ingested_at   TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE staging.weather_raw IS
    'Raw daily weather data from Open-Meteo API. '
    'Populated by ingestion.py.';

-- ---------------------------------------------------------------------------
-- staging.countries_raw
-- One row per country as fetched from REST Countries v3.1 API.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS staging.countries_raw CASCADE;
CREATE TABLE staging.countries_raw (
    id            SERIAL       PRIMARY KEY,
    cca2          CHAR(2)      NOT NULL,   -- ISO 3166-1 alpha-2
    cca3          CHAR(3),                 -- ISO 3166-1 alpha-3
    common_name   TEXT,
    official_name TEXT,
    region        TEXT,
    subregion     TEXT,
    population    BIGINT,
    area_km2      NUMERIC(15,2),
    capital       TEXT,
    languages     JSONB,
    currencies    JSONB,
    timezones     JSONB,
    latlng        JSONB,
    raw_json      JSONB,                   -- full API response preserved
    ingested_at   TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE staging.countries_raw IS
    'Raw country metadata from REST Countries API. '
    'Populated by ingestion.py.';

-- ---------------------------------------------------------------------------
-- staging.weather_clean  (created by transformation.py)
-- ---------------------------------------------------------------------------
-- This table is created programmatically; definition shown here for reference.
-- See transformation.py -> persist_clean_tables().

-- ---------------------------------------------------------------------------
-- staging.countries_clean  (created by transformation.py)
-- ---------------------------------------------------------------------------
-- This table is created programmatically; definition shown here for reference.
-- See transformation.py -> persist_clean_tables().
