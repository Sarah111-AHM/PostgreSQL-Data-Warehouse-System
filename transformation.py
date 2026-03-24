"""
transformation.py
-----------------
Data Transformation Script for the Data Warehousing Project.

Reads raw data from:
  - staging.weather_raw
  - staging.countries_raw

Applies cleaning & enrichment:
  - Null handling / outlier removal
  - Weather code -> human-readable description
  - Temperature range & daily temperature variation
  - Season derivation (by hemisphere)
  - Country currency & language normalization

Writes processed CSVs to data/processed/ and loads into:
  - staging.weather_clean
  - staging.countries_clean
"""

import json
import logging
import pandas as pd
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
BASE_DIR      = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
CONFIG_DIR    = BASE_DIR / "config"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# DB helpers
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
# WMO Weather Code lookup
# ---------------------------------------------------------------------------
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    56: "Light freezing drizzle", 57: "Dense freezing drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    66: "Light freezing rain", 67: "Heavy freezing rain",
    71: "Slight snowfall", 73: "Moderate snowfall", 75: "Heavy snowfall",
    77: "Snow grains",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}

def wmo_description(code) -> str:
    if pd.isna(code):
        return "Unknown"
    return WMO_CODES.get(int(code), f"Code {int(code)}")

# ---------------------------------------------------------------------------
# Season derivation
# ---------------------------------------------------------------------------
SOUTHERN_HEMISPHERE = {"AU", "BR", "ZA", "AR", "NZ"}

def get_season(month: int, country_code: str) -> str:
    if country_code in SOUTHERN_HEMISPHERE:
        # Flip seasons for southern hemisphere
        month = (month + 6 - 1) % 12 + 1
    if month in (12, 1, 2):
        return "Winter"
    elif month in (3, 4, 5):
        return "Spring"
    elif month in (6, 7, 8):
        return "Summer"
    else:
        return "Autumn"

# ---------------------------------------------------------------------------
# Precipitation category
# ---------------------------------------------------------------------------
def precip_category(mm) -> str:
    if pd.isna(mm):
        return "Unknown"
    if mm == 0:
        return "None"
    elif mm < 5:
        return "Light"
    elif mm < 20:
        return "Moderate"
    else:
        return "Heavy"

# ---------------------------------------------------------------------------
# Transform weather
# ---------------------------------------------------------------------------
def transform_weather(conn) -> pd.DataFrame:
    log.info("--- Transforming weather data ---")
    df = pd.read_sql("SELECT * FROM staging.weather_raw", conn)
    log.info(f"  Loaded {len(df):,} rows from staging.weather_raw")

    original_count = len(df)

    # 1. Drop rows missing the date (key field)
    df = df.dropna(subset=["date"])

    # 2. Parse date
    df["date"] = pd.to_datetime(df["date"])

    # 3. Cap extreme temperatures (physical plausibility check)
    df = df[df["temp_max"].between(-80, 60) | df["temp_max"].isna()]
    df = df[df["temp_min"].between(-80, 60) | df["temp_min"].isna()]

    # 4. Fill missing temp_mean with average of max/min
    mask = df["temp_mean"].isna() & df["temp_max"].notna() & df["temp_min"].notna()
    df.loc[mask, "temp_mean"] = (df.loc[mask, "temp_max"] + df.loc[mask, "temp_min"]) / 2

    # 5. Fill missing precipitation with 0 (no rain reported = 0)
    df["precipitation"] = df["precipitation"].fillna(0)

    # 6. Derived columns
    df["temp_range"]        = (df["temp_max"] - df["temp_min"]).round(2)
    df["month"]             = df["date"].dt.month
    df["month_name"]        = df["date"].dt.strftime("%B")
    df["quarter"]           = df["date"].dt.quarter
    df["year"]              = df["date"].dt.year
    df["day_of_week"]       = df["date"].dt.day_name()
    df["is_weekend"]        = df["date"].dt.dayofweek >= 5
    df["weather_desc"]      = df["weathercode"].apply(wmo_description)
    df["season"]            = df.apply(lambda r: get_season(r["month"], r["country_code"]), axis=1)
    df["precip_category"]   = df["precipitation"].apply(precip_category)
    df["is_rainy"]          = df["precipitation"] > 0
    df["is_hot_day"]        = df["temp_max"] > 35
    df["is_freezing_day"]   = df["temp_min"] < 0

    removed = original_count - len(df)
    log.info(f"  Removed {removed} invalid rows; {len(df):,} clean rows remain")

    out_path = PROCESSED_DIR / "weather_clean.csv"
    df.to_csv(out_path, index=False)
    log.info(f"  Saved -> {out_path}")
    return df

# ---------------------------------------------------------------------------
# Transform countries
# ---------------------------------------------------------------------------
def transform_countries(conn) -> pd.DataFrame:
    log.info("--- Transforming countries data ---")
    df = pd.read_sql("SELECT * FROM staging.countries_raw", conn)
    log.info(f"  Loaded {len(df):,} rows from staging.countries_raw")

    # 1. Parse JSONB columns (stored as strings when read via pandas)
    def safe_json(val):
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return {}
        return val if val else {}

    df["languages"]  = df["languages"].apply(safe_json)
    df["currencies"] = df["currencies"].apply(safe_json)
    df["timezones"]  = df["timezones"].apply(safe_json)
    df["latlng"]     = df["latlng"].apply(safe_json)

    # 2. Flatten languages into comma-separated string
    df["languages_str"] = df["languages"].apply(
        lambda x: ", ".join(x.values()) if isinstance(x, dict) else ""
    )
    df["language_count"] = df["languages"].apply(
        lambda x: len(x) if isinstance(x, dict) else 0
    )

    # 3. Flatten currencies
    df["currency_codes"] = df["currencies"].apply(
        lambda x: ", ".join(x.keys()) if isinstance(x, dict) else ""
    )
    df["currency_names"] = df["currencies"].apply(
        lambda x: ", ".join(v.get("name", "") for v in x.values()) if isinstance(x, dict) else ""
    )

    # 4. Flatten timezones
    df["timezone_count"] = df["timezones"].apply(
        lambda x: len(x) if isinstance(x, list) else (1 if x else 0)
    )

    # 5. Population density (people per km2)
    df["pop_density"] = (df["population"] / df["area_km2"]).round(2)

    # 6. Population & area tiers
    def pop_tier(p) -> str:
        if pd.isna(p): return "Unknown"
        if p < 1_000_000: return "Small (<1M)"
        elif p < 10_000_000: return "Medium (1-10M)"
        elif p < 100_000_000: return "Large (10-100M)"
        else: return "Very Large (>100M)"

    def area_tier(a) -> str:
        if pd.isna(a): return "Unknown"
        if a < 10_000: return "Tiny (<10k km2)"
        elif a < 100_000: return "Small (10-100k km2)"
        elif a < 1_000_000: return "Medium (100k-1M km2)"
        else: return "Large (>1M km2)"

    df["population_tier"] = df["population"].apply(pop_tier)
    df["area_tier"]       = df["area_km2"].apply(area_tier)

    # 7. Drop raw JSONB columns for the clean file
    drop_cols = ["languages", "currencies", "timezones", "latlng", "raw_json"]
    df_clean = df.drop(columns=[c for c in drop_cols if c in df.columns])

    out_path = PROCESSED_DIR / "countries_clean.csv"
    df_clean.to_csv(out_path, index=False)
    log.info(f"  Saved -> {out_path}  ({len(df_clean):,} rows)")
    return df_clean

# ---------------------------------------------------------------------------
# Load transformed data back to staging (clean tables)
# ---------------------------------------------------------------------------
CLEAN_DDL = """
DROP TABLE IF EXISTS staging.weather_clean CASCADE;
CREATE TABLE staging.weather_clean AS
    SELECT * FROM staging.weather_raw WHERE 1=0;

ALTER TABLE staging.weather_clean
    ADD COLUMN temp_range      NUMERIC(6,2),
    ADD COLUMN month           SMALLINT,
    ADD COLUMN month_name      TEXT,
    ADD COLUMN quarter         SMALLINT,
    ADD COLUMN year            SMALLINT,
    ADD COLUMN day_of_week     TEXT,
    ADD COLUMN is_weekend      BOOLEAN,
    ADD COLUMN weather_desc    TEXT,
    ADD COLUMN season          TEXT,
    ADD COLUMN precip_category TEXT,
    ADD COLUMN is_rainy        BOOLEAN,
    ADD COLUMN is_hot_day      BOOLEAN,
    ADD COLUMN is_freezing_day BOOLEAN;

DROP TABLE IF EXISTS staging.countries_clean CASCADE;
CREATE TABLE staging.countries_clean (
    id               SERIAL PRIMARY KEY,
    cca2             CHAR(2),
    cca3             CHAR(3),
    common_name      TEXT,
    official_name    TEXT,
    region           TEXT,
    subregion        TEXT,
    population       BIGINT,
    area_km2         NUMERIC(15,2),
    capital          TEXT,
    languages_str    TEXT,
    language_count   SMALLINT,
    currency_codes   TEXT,
    currency_names   TEXT,
    timezone_count   SMALLINT,
    pop_density      NUMERIC(12,2),
    population_tier  TEXT,
    area_tier        TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

def persist_clean_tables(conn, weather_df: pd.DataFrame, countries_df: pd.DataFrame):
    log.info("--- Writing clean tables to staging schema ---")
    with conn.cursor() as cur:
        cur.execute(CLEAN_DDL)
    conn.commit()

    # Weather
    weather_cols = [
        "city_name","country_code","latitude","longitude","date",
        "temp_max","temp_min","temp_mean","precipitation","windspeed_max","weathercode",
        "temp_range","month","month_name","quarter","year","day_of_week",
        "is_weekend","weather_desc","season","precip_category","is_rainy",
        "is_hot_day","is_freezing_day",
    ]
    weather_df["date"] = weather_df["date"].astype(str)
    rows_w = [tuple(r) for r in weather_df[weather_cols].itertuples(index=False)]
    cols_sql = ",".join(weather_cols)
    placeholders = ",".join(["%s"] * len(weather_cols))
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            f"INSERT INTO staging.weather_clean ({cols_sql}) VALUES ({placeholders})",
            rows_w, page_size=500,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows_w):,} rows -> staging.weather_clean")

    # Countries
    country_cols = [
        "cca2","cca3","common_name","official_name","region","subregion",
        "population","area_km2","capital","languages_str","language_count",
        "currency_codes","currency_names","timezone_count","pop_density",
        "population_tier","area_tier",
    ]
    rows_c = [tuple(r) for r in countries_df[country_cols].itertuples(index=False)]
    cols_sql_c = ",".join(country_cols)
    placeholders_c = ",".join(["%s"] * len(country_cols))
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            f"INSERT INTO staging.countries_clean ({cols_sql_c}) VALUES ({placeholders_c})",
            rows_c,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows_c):,} rows -> staging.countries_clean")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("DATA TRANSFORMATION PIPELINE  --  START")
    log.info("=" * 60)

    cfg  = load_db_config()
    conn = get_connection(cfg)
    try:
        weather_df   = transform_weather(conn)
        countries_df = transform_countries(conn)
        persist_clean_tables(conn, weather_df, countries_df)
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("DATA TRANSFORMATION PIPELINE  --  COMPLETE")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
