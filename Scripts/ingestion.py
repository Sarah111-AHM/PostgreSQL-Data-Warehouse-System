"""
ingestion.py
------------
Data Ingestion Script for the Data Warehousing Project.

Sources:
  1. Open-Meteo Historical Weather API  (free, no key required)
     -> Daily weather observations for 10 major world cities (2023-01-01 to 2023-12-31)
  2. REST Countries API  (free, no key required)
     -> Country metadata: population, region, area, languages, currencies

Outputs (raw JSON saved to data/raw/):
  - weather_raw.json
  - countries_raw.json

Staging tables created in PostgreSQL (schema: staging):
  - staging.weather_raw
  - staging.countries_raw
"""

import json
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent.parent
RAW_DIR    = BASE_DIR / "data" / "raw"
CONFIG_DIR = BASE_DIR / "config"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def load_db_config() -> dict:
    with open(CONFIG_DIR / "db_config.json") as f:
        return json.load(f)

def get_connection(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"],
    )

# ---------------------------------------------------------------------------
# Cities & date range
# ---------------------------------------------------------------------------
CITIES = [
    {"name": "New York",  "country": "US", "latitude":  40.7128, "longitude": -74.0060},
    {"name": "London",    "country": "GB", "latitude":  51.5074, "longitude":  -0.1278},
    {"name": "Tokyo",     "country": "JP", "latitude":  35.6762, "longitude": 139.6503},
    {"name": "Paris",     "country": "FR", "latitude":  48.8566, "longitude":   2.3522},
    {"name": "Sydney",    "country": "AU", "latitude": -33.8688, "longitude": 151.2093},
    {"name": "Dubai",     "country": "AE", "latitude":  25.2048, "longitude":  55.2708},
    {"name": "Sao Paulo", "country": "BR", "latitude": -23.5505, "longitude": -46.6333},
    {"name": "Mumbai",    "country": "IN", "latitude":  19.0760, "longitude":  72.8777},
    {"name": "Cairo",     "country": "EG", "latitude":  30.0444, "longitude":  31.2357},
    {"name": "Toronto",   "country": "CA", "latitude":  43.6532, "longitude": -79.3832},
]
COUNTRY_CODES = list({c["country"] for c in CITIES})
DATE_START, DATE_END = "2023-01-01", "2023-12-31"

# ---------------------------------------------------------------------------
# 1. Weather ingestion (Open-Meteo)
# ---------------------------------------------------------------------------
def fetch_weather(city: dict) -> dict:
    """Fetch daily weather data from Open-Meteo Archive API."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city["latitude"], "longitude": city["longitude"],
        "start_date": DATE_START, "end_date": DATE_END,
        "daily": ",".join([
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "windspeed_10m_max", "weathercode",
        ]),
        "timezone": "UTC",
    }
    log.info(f"  Fetching weather -> {city['name']}")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    data["city_name"]    = city["name"]
    data["country_code"] = city["country"]
    return data

def ingest_weather() -> list:
    log.info("--- Weather Ingestion ---")
    all_weather = []
    for city in CITIES:
        try:
            all_weather.append(fetch_weather(city))
            time.sleep(0.5)
        except Exception as exc:
            log.error(f"  FAILED {city['name']}: {exc}")
    with open(RAW_DIR / "weather_raw.json", "w") as f:
        json.dump(all_weather, f, indent=2)
    log.info(f"  Saved weather_raw.json  ({len(all_weather)} cities, ~{len(all_weather)*365} rows)")
    return all_weather

# ---------------------------------------------------------------------------
# 2. Countries ingestion (REST Countries v3.1)
# ---------------------------------------------------------------------------
def ingest_countries() -> list:
    log.info("--- Countries Ingestion ---")
    results = []
    for code in COUNTRY_CODES:
        try:
            log.info(f"  Fetching country -> {code}")
            resp = requests.get(f"https://restcountries.com/v3.1/alpha/{code}", timeout=15)
            resp.raise_for_status()
            results.extend(resp.json())
            time.sleep(0.3)
        except Exception as exc:
            log.error(f"  FAILED {code}: {exc}")
    with open(RAW_DIR / "countries_raw.json", "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"  Saved countries_raw.json  ({len(results)} records)")
    return results

# ---------------------------------------------------------------------------
# 3. Staging schema DDL
# ---------------------------------------------------------------------------
DDL_STAGING = """
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.weather_raw CASCADE;
CREATE TABLE staging.weather_raw (
    id            SERIAL PRIMARY KEY,
    city_name     TEXT        NOT NULL,
    country_code  CHAR(2)     NOT NULL,
    latitude      NUMERIC(9,4),
    longitude     NUMERIC(9,4),
    date          DATE        NOT NULL,
    temp_max      NUMERIC(6,2),
    temp_min      NUMERIC(6,2),
    temp_mean     NUMERIC(6,2),
    precipitation NUMERIC(8,2),
    windspeed_max NUMERIC(6,2),
    weathercode   SMALLINT,
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

DROP TABLE IF EXISTS staging.countries_raw CASCADE;
CREATE TABLE staging.countries_raw (
    id            SERIAL PRIMARY KEY,
    cca2          CHAR(2)  NOT NULL,
    cca3          CHAR(3),
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
    raw_json      JSONB,
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# 4. Load to staging
# ---------------------------------------------------------------------------
def load_weather_to_staging(conn, records: list):
    rows = []
    for city_data in records:
        daily  = city_data.get("daily", {})
        dates  = daily.get("time", [])
        t_max  = daily.get("temperature_2m_max",  [None]*len(dates))
        t_min  = daily.get("temperature_2m_min",  [None]*len(dates))
        t_mean = daily.get("temperature_2m_mean", [None]*len(dates))
        precip = daily.get("precipitation_sum",   [None]*len(dates))
        wind   = daily.get("windspeed_10m_max",   [None]*len(dates))
        wcode  = daily.get("weathercode",         [None]*len(dates))
        lat, lon = city_data.get("latitude"), city_data.get("longitude")
        for i, ds in enumerate(dates):
            rows.append((
                city_data["city_name"], city_data["country_code"], lat, lon,
                ds, t_max[i], t_min[i], t_mean[i], precip[i], wind[i], wcode[i],
            ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO staging.weather_raw
               (city_name,country_code,latitude,longitude,date,
                temp_max,temp_min,temp_mean,precipitation,windspeed_max,weathercode)
               VALUES %s""",
            rows, page_size=500,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows):,} rows -> staging.weather_raw")

def load_countries_to_staging(conn, countries: list):
    rows = []
    for c in countries:
        names    = c.get("name", {})
        capitals = c.get("capital", [])
        rows.append((
            c.get("cca2"), c.get("cca3"),
            names.get("common"), names.get("official"),
            c.get("region"), c.get("subregion"),
            c.get("population"), c.get("area"),
            capitals[0] if capitals else None,
            json.dumps(c.get("languages", {})),
            json.dumps(c.get("currencies", {})),
            json.dumps(c.get("timezones", [])),
            json.dumps(c.get("latlng", [])),
            json.dumps(c),
        ))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO staging.countries_raw
               (cca2,cca3,common_name,official_name,region,subregion,
                population,area_km2,capital,languages,currencies,timezones,latlng,raw_json)
               VALUES %s""",
            rows,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows):,} rows -> staging.countries_raw")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("DATA INGESTION PIPELINE  --  START")
    log.info("=" * 60)

    weather_data   = ingest_weather()
    countries_data = ingest_countries()

    log.info("Connecting to PostgreSQL ...")
    cfg  = load_db_config()
    conn = get_connection(cfg)
    try:
        log.info("Creating staging schema ...")
        with conn.cursor() as cur:
            cur.execute(DDL_STAGING)
        conn.commit()

        log.info("Loading data into staging ...")
        load_weather_to_staging(conn, weather_data)
        load_countries_to_staging(conn, countries_data)
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("DATA INGESTION PIPELINE  --  COMPLETE")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
