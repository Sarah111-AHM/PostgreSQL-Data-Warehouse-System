-- =============================================================================
-- dw_schema.sql
-- Data Warehouse Star Schema DDL
-- PostgreSQL schema: dw
-- =============================================================================
--
-- SCHEMA OVERVIEW (Star Schema)
-- ─────────────────────────────────────────────────────────────────────────────
--
--                          ┌─────────────┐
--                          │  dim_date   │
--                          └──────┬──────┘
--                                 │ sk_date
--   ┌──────────┐   sk_city  ┌─────┴──────────────────────┐  sk_condition  ┌──────────────────────┐
--   │ dim_city │ ───────────► fact_weather_observations   ◄─────────────── │ dim_weather_condition│
--   └──────────┘            └─────┬──────────────────────┘                └──────────────────────┘
--         │ sk_country            │ sk_country
--         ▼                       ▼
--   ┌─────────────┐         ┌─────────────┐
--   │ dim_country │◄────────│ dim_country │   (same table, two FK paths)
--   └─────────────┘         └─────────────┘
--
-- Grain: one row per CITY per CALENDAR DAY (365 days x 10 cities = 3,650 fact rows)
-- =============================================================================

DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

-- ---------------------------------------------------------------------------
-- DIMENSION: dim_date
-- Calendar dimension covering the full year 2023.
-- ---------------------------------------------------------------------------
CREATE TABLE dw.dim_date (
    sk_date         SERIAL       PRIMARY KEY,
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL    CHECK (quarter BETWEEN 1 AND 4),
    quarter_label   CHAR(6)      NOT NULL,   -- e.g. '2023Q1'
    month           SMALLINT     NOT NULL    CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(10)  NOT NULL,   -- e.g. 'January'
    month_abbr      CHAR(3)      NOT NULL,   -- e.g. 'Jan'
    week_of_year    SMALLINT     NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL    CHECK (day_of_week BETWEEN 0 AND 6), -- 0=Mon
    day_name        VARCHAR(10)  NOT NULL,
    day_abbr        CHAR(3)      NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_leap_year    BOOLEAN      NOT NULL,
    day_of_year     SMALLINT     NOT NULL
);

COMMENT ON TABLE dw.dim_date IS
    'Conformed date dimension. One row per calendar day in 2023.';

-- ---------------------------------------------------------------------------
-- DIMENSION: dim_country
-- Country metadata sourced from REST Countries API.
-- ---------------------------------------------------------------------------
CREATE TABLE dw.dim_country (
    sk_country      SERIAL       PRIMARY KEY,
    cca2            CHAR(2)      NOT NULL UNIQUE,
    cca3            CHAR(3),
    common_name     TEXT         NOT NULL,
    official_name   TEXT,
    region          TEXT,        -- e.g. 'Americas', 'Europe', 'Asia'
    subregion       TEXT,        -- e.g. 'Northern America', 'Western Europe'
    capital         TEXT,
    population      BIGINT,
    area_km2        NUMERIC(15,2),
    pop_density     NUMERIC(12,2),   -- people / km2
    population_tier TEXT,            -- Small / Medium / Large / Very Large
    area_tier       TEXT,            -- Tiny / Small / Medium / Large
    languages       TEXT,            -- comma-separated language names
    language_count  SMALLINT,
    currency_codes  TEXT,            -- e.g. 'USD'
    currency_names  TEXT,            -- e.g. 'United States dollar'
    timezone_count  SMALLINT
);

COMMENT ON TABLE dw.dim_country IS
    'Country reference dimension. Populated from REST Countries v3.1 API.';

-- ---------------------------------------------------------------------------
-- DIMENSION: dim_city
-- City reference with snowflake link to dim_country.
-- ---------------------------------------------------------------------------
CREATE TABLE dw.dim_city (
    sk_city         SERIAL       PRIMARY KEY,
    city_name       TEXT         NOT NULL,
    sk_country      INT          NOT NULL REFERENCES dw.dim_country(sk_country),
    country_code    CHAR(2)      NOT NULL,
    latitude        NUMERIC(9,4),
    longitude       NUMERIC(9,4),
    hemisphere      CHAR(1)      NOT NULL CHECK (hemisphere IN ('N','S'))
);

COMMENT ON TABLE dw.dim_city IS
    'City dimension. Snowflake link to dim_country via sk_country.';

-- ---------------------------------------------------------------------------
-- DIMENSION: dim_weather_condition
-- WMO weather code lookup table.
-- ---------------------------------------------------------------------------
CREATE TABLE dw.dim_weather_condition (
    sk_condition    SERIAL       PRIMARY KEY,
    wmo_code        SMALLINT     NOT NULL UNIQUE,
    description     TEXT         NOT NULL,  -- e.g. 'Moderate rain'
    category        TEXT         NOT NULL   -- Clear / Cloudy / Fog / Drizzle / Rain / Snow / Thunderstorm
);

COMMENT ON TABLE dw.dim_weather_condition IS
    'WMO weather code reference. Maps numeric codes to human-readable descriptions.';

-- ---------------------------------------------------------------------------
-- FACT TABLE: fact_weather_observations
-- Grain: one row per city per calendar day.
-- ---------------------------------------------------------------------------
CREATE TABLE dw.fact_weather_observations (
    sk_observation  BIGSERIAL    PRIMARY KEY,

    -- Foreign keys (dimension references)
    sk_date         INT          NOT NULL REFERENCES dw.dim_date(sk_date),
    sk_city         INT          NOT NULL REFERENCES dw.dim_city(sk_city),
    sk_country      INT          NOT NULL REFERENCES dw.dim_country(sk_country),
    sk_condition    INT                   REFERENCES dw.dim_weather_condition(sk_condition),

    -- Additive measures (temperature in Celsius, precip in mm, wind in km/h)
    temp_max        NUMERIC(6,2),
    temp_min        NUMERIC(6,2),
    temp_mean       NUMERIC(6,2),
    temp_range      NUMERIC(6,2),       -- temp_max - temp_min
    precipitation   NUMERIC(8,2),
    windspeed_max   NUMERIC(6,2),

    -- Degenerate dimensions / flags
    season          TEXT,               -- Winter / Spring / Summer / Autumn
    precip_category TEXT,               -- None / Light / Moderate / Heavy
    is_rainy        BOOLEAN,
    is_hot_day      BOOLEAN,            -- temp_max > 35 C
    is_freezing_day BOOLEAN,            -- temp_min < 0 C

    -- Audit
    loaded_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX idx_fact_date     ON dw.fact_weather_observations(sk_date);
CREATE INDEX idx_fact_city     ON dw.fact_weather_observations(sk_city);
CREATE INDEX idx_fact_country  ON dw.fact_weather_observations(sk_country);
CREATE INDEX idx_fact_season   ON dw.fact_weather_observations(season);
CREATE INDEX idx_fact_rainy    ON dw.fact_weather_observations(is_rainy);

COMMENT ON TABLE dw.fact_weather_observations IS
    'Central fact table. Grain: one row per city per calendar day (2023). '
    'Conformed dimensions: dim_date, dim_city, dim_country, dim_weather_condition.';
