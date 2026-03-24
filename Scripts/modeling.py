"""
modeling.py
===========
Data Warehouse Schema Creation & Data Loading Script
------------------------------------------------------
Implements a STAR SCHEMA in the 'dw' PostgreSQL schema with:

Dimension Tables:
  dim_date       - Full date dimension (calendar hierarchy)
  dim_customer   - Customer SCD Type 1 dimension
  dim_product    - Product dimension with category hierarchy
  dim_location   - Country/region geography dimension
  dim_payment    - Payment method dimension

Fact Tables:
  fact_sales     - Grain: one row per order line item
  fact_orders    - Grain: one row per order (aggregate fact)
"""

import os
import json
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config(name):
    with open(os.path.join(BASE_DIR, "config", f"{name}.json")) as f:
        return json.load(f)


DB_CFG = load_config("db_config")


def get_connection():
    return psycopg2.connect(
        host=DB_CFG["host"],
        port=DB_CFG["port"],
        dbname=DB_CFG["database"],
        user=DB_CFG["user"],
        password=DB_CFG["password"],
    )


def read_staging(conn, table):
    return pd.read_sql(f'SELECT * FROM staging."{table}"', conn)


# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────
DW_DDL = """
CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_sales   CASCADE;
DROP TABLE IF EXISTS dw.fact_orders  CASCADE;
DROP TABLE IF EXISTS dw.dim_customer CASCADE;
DROP TABLE IF EXISTS dw.dim_product  CASCADE;
DROP TABLE IF EXISTS dw.dim_payment  CASCADE;
DROP TABLE IF EXISTS dw.dim_location CASCADE;
DROP TABLE IF EXISTS dw.dim_date     CASCADE;

-- ── DIMENSION: Date ──────────────────────────────────────────────────────────
CREATE TABLE dw.dim_date (
    date_key        INTEGER      PRIMARY KEY,
    full_date       DATE         NOT NULL UNIQUE,
    day_of_week     SMALLINT     NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_year     SMALLINT     NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    month_number    SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    month_short     VARCHAR(3)   NOT NULL,
    quarter         SMALLINT     NOT NULL,
    quarter_name    VARCHAR(6)   NOT NULL,
    year            SMALLINT     NOT NULL,
    fiscal_year     SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_holiday      BOOLEAN      DEFAULT FALSE,
    season          VARCHAR(10)  NOT NULL
);

-- ── DIMENSION: Location ──────────────────────────────────────────────────────
CREATE TABLE dw.dim_location (
    location_key    SERIAL       PRIMARY KEY,
    country_code    VARCHAR(3)   NOT NULL UNIQUE,
    country_name    VARCHAR(100) NOT NULL,
    region          VARCHAR(50)  NOT NULL,
    subregion       VARCHAR(80),
    population      BIGINT,
    currency_code   VARCHAR(10),
    currency_name   VARCHAR(80)
);

-- ── DIMENSION: Customer ──────────────────────────────────────────────────────
CREATE TABLE dw.dim_customer (
    customer_key    SERIAL       PRIMARY KEY,
    customer_id     INTEGER      NOT NULL UNIQUE,
    full_name       VARCHAR(120) NOT NULL,
    first_name      VARCHAR(60),
    last_name       VARCHAR(60),
    email           VARCHAR(120),
    gender          VARCHAR(20),
    birth_date      DATE,
    age             SMALLINT,
    age_group       VARCHAR(20),
    signup_date     DATE,
    loyalty_tier    VARCHAR(20),
    location_key    INTEGER      REFERENCES dw.dim_location(location_key),
    city            VARCHAR(80)
);

-- ── DIMENSION: Product ───────────────────────────────────────────────────────
CREATE TABLE dw.dim_product (
    product_key      SERIAL       PRIMARY KEY,
    product_id       INTEGER      NOT NULL UNIQUE,
    product_name     VARCHAR(200) NOT NULL,
    category         VARCHAR(80),
    subcategory      VARCHAR(80),
    brand            VARCHAR(80),
    supplier         VARCHAR(100),
    sku              VARCHAR(40),
    unit_price       NUMERIC(10,2),
    cost_price       NUMERIC(10,2),
    gross_margin_pct NUMERIC(6,2),
    weight_kg        NUMERIC(6,3),
    is_active        BOOLEAN
);

-- ── DIMENSION: Payment ───────────────────────────────────────────────────────
CREATE TABLE dw.dim_payment (
    payment_key     SERIAL       PRIMARY KEY,
    payment_method  VARCHAR(40)  NOT NULL UNIQUE,
    payment_type    VARCHAR(20)  NOT NULL,
    is_digital      BOOLEAN      NOT NULL
);

-- ── FACT: Sales (line-item grain) ────────────────────────────────────────────
CREATE TABLE dw.fact_sales (
    sales_key       BIGSERIAL    PRIMARY KEY,
    order_date_key  INTEGER      REFERENCES dw.dim_date(date_key),
    ship_date_key   INTEGER      REFERENCES dw.dim_date(date_key),
    customer_key    INTEGER      REFERENCES dw.dim_customer(customer_key),
    product_key     INTEGER      REFERENCES dw.dim_product(product_key),
    location_key    INTEGER      REFERENCES dw.dim_location(location_key),
    payment_key     INTEGER      REFERENCES dw.dim_payment(payment_key),
    order_id        INTEGER      NOT NULL,
    order_status    VARCHAR(20),
    currency_code   VARCHAR(10),
    quantity        INTEGER      NOT NULL,
    unit_price      NUMERIC(10,2),
    discount_pct    NUMERIC(5,2),
    discount_amt    NUMERIC(10,2),
    line_gross      NUMERIC(12,2),
    line_net        NUMERIC(12,2),
    line_cost       NUMERIC(12,2),
    line_margin     NUMERIC(12,2),
    margin_pct      NUMERIC(6,2),
    shipping_cost   NUMERIC(8,2),
    days_to_ship    SMALLINT
);

-- ── FACT: Orders (order-header grain) ────────────────────────────────────────
CREATE TABLE dw.fact_orders (
    order_fact_key  BIGSERIAL    PRIMARY KEY,
    order_date_key  INTEGER      REFERENCES dw.dim_date(date_key),
    ship_date_key   INTEGER      REFERENCES dw.dim_date(date_key),
    customer_key    INTEGER      REFERENCES dw.dim_customer(customer_key),
    location_key    INTEGER      REFERENCES dw.dim_location(location_key),
    payment_key     INTEGER      REFERENCES dw.dim_payment(payment_key),
    order_id        INTEGER      NOT NULL UNIQUE,
    order_status    VARCHAR(20),
    currency_code   VARCHAR(10),
    num_line_items  SMALLINT,
    total_quantity  INTEGER,
    gross_revenue   NUMERIC(12,2),
    total_discount  NUMERIC(10,2),
    net_revenue     NUMERIC(12,2),
    total_cost      NUMERIC(12,2),
    gross_margin    NUMERIC(12,2),
    shipping_cost   NUMERIC(8,2),
    days_to_ship    SMALLINT
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX idx_fact_sales_order_date ON dw.fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_customer   ON dw.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product    ON dw.fact_sales(product_key);
CREATE INDEX idx_fact_sales_location   ON dw.fact_sales(location_key);
CREATE INDEX idx_fact_sales_order_id   ON dw.fact_sales(order_id);
CREATE INDEX idx_fact_orders_date      ON dw.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_customer  ON dw.fact_orders(customer_key);
CREATE INDEX idx_dim_customer_loyalty  ON dw.dim_customer(loyalty_tier);
CREATE INDEX idx_dim_product_category  ON dw.dim_product(category);
CREATE INDEX idx_dim_date_year_month   ON dw.dim_date(year, month_number);
"""


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────
def date_to_key(d):
    """Convert a date value to YYYYMMDD integer key."""
    if d is None:
        return None
    if isinstance(d, float) and np.isnan(d):
        return None
    try:
        return int(pd.Timestamp(d).strftime("%Y%m%d"))
    except Exception:
        return None


def safe(v):
    """Convert numpy scalar types to plain Python for psycopg2."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


# ─────────────────────────────────────────────────────────────────────────────
# Dimension loaders
# ─────────────────────────────────────────────────────────────────────────────
def build_and_load_dim_date(conn, start="2021-01-01", end="2026-12-31"):
    logger.info(f"Building dim_date ({start} → {end}) …")
    dates = pd.date_range(start=start, end=end, freq="D")
    rows = []
    for d in dates:
        iso_dow = d.isoweekday()   # Mon=1 … Sun=7
        m = d.month
        season = (
            "Winter" if m in (12, 1, 2) else
            "Spring" if m in (3, 4, 5) else
            "Summer" if m in (6, 7, 8) else
            "Autumn"
        )
        q = d.quarter
        rows.append((
            int(d.strftime("%Y%m%d")),  # date_key
            d.date(),                    # full_date
            iso_dow,                     # day_of_week
            d.day_name(),               # day_name
            int(d.day),                 # day_of_month
            int(d.day_of_year),         # day_of_year
            int(d.isocalendar().week),  # week_of_year
            int(m),                     # month_number
            d.month_name(),             # month_name
            d.strftime("%b"),           # month_short
            int(q),                     # quarter
            f"Q{q}",                    # quarter_name
            int(d.year),               # year
            int(d.year),               # fiscal_year
            iso_dow >= 6,              # is_weekend
            False,                     # is_holiday
            season,                    # season
        ))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.dim_date RESTART IDENTITY CASCADE;")
        execute_values(cur,
            """INSERT INTO dw.dim_date
               (date_key, full_date, day_of_week, day_name, day_of_month,
                day_of_year, week_of_year, month_number, month_name, month_short,
                quarter, quarter_name, year, fiscal_year,
                is_weekend, is_holiday, season)
               VALUES %s""",
            rows, page_size=500)
    conn.commit()
    logger.info(f"  → dim_date: {len(rows):,} rows.")


def load_dim_location(conn, countries_df):
    """Load location dimension. Returns {country_code: location_key}."""
    logger.info("Loading dim_location …")
    cols = ["country_code", "country_name", "region", "subregion",
            "population", "currency_code", "currency_name"]
    rows = [tuple(safe(v) for v in r) for r in countries_df[cols].itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.dim_location RESTART IDENTITY CASCADE;")
        execute_values(cur,
            "INSERT INTO dw.dim_location "
            "(country_code,country_name,region,subregion,population,currency_code,currency_name) "
            "VALUES %s",
            rows, page_size=500)
    conn.commit()

    df = pd.read_sql("SELECT location_key, country_code FROM dw.dim_location", conn)
    logger.info(f"  → dim_location: {len(df)} rows.")
    return dict(zip(df["country_code"], df["location_key"]))


def load_dim_customer(conn, customers_df, loc_map):
    """Load customer dimension. Returns {customer_id: customer_key}."""
    logger.info("Loading dim_customer …")
    df = customers_df.copy()
    df["location_key"] = df["country_code"].map(loc_map)

    # Ensure full_name exists
    if "full_name" not in df.columns:
        df["full_name"] = df["first_name"].fillna("") + " " + df["last_name"].fillna("")
        df["full_name"] = df["full_name"].str.strip()

    cols = ["customer_id", "full_name", "first_name", "last_name", "email",
            "gender", "birth_date", "age", "age_group", "signup_date",
            "loyalty_tier", "location_key", "city"]
    rows = [tuple(safe(v) for v in r) for r in df[cols].itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.dim_customer RESTART IDENTITY CASCADE;")
        execute_values(cur,
            f"INSERT INTO dw.dim_customer ({','.join(cols)}) VALUES %s",
            rows, page_size=500)
    conn.commit()

    df2 = pd.read_sql("SELECT customer_key, customer_id FROM dw.dim_customer", conn)
    logger.info(f"  → dim_customer: {len(df2)} rows.")
    return dict(zip(df2["customer_id"], df2["customer_key"]))


def load_dim_product(conn, products_df):
    """Load product dimension. Returns {product_id: product_key}."""
    logger.info("Loading dim_product …")
    cols = ["product_id", "product_name", "category", "subcategory", "brand",
            "supplier", "sku", "unit_price", "cost_price", "gross_margin_pct",
            "weight_kg", "is_active"]
    rows = [tuple(safe(v) for v in r) for r in products_df[cols].itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.dim_product RESTART IDENTITY CASCADE;")
        execute_values(cur,
            f"INSERT INTO dw.dim_product ({','.join(cols)}) VALUES %s",
            rows, page_size=500)
    conn.commit()

    df = pd.read_sql("SELECT product_key, product_id FROM dw.dim_product", conn)
    logger.info(f"  → dim_product: {len(df)} rows.")
    return dict(zip(df["product_id"], df["product_key"]))


def load_dim_payment(conn, orders_df):
    """Load payment dimension. Returns {payment_method: payment_key}."""
    logger.info("Loading dim_payment …")
    methods = orders_df["payment_method"].dropna().unique()

    def classify(m):
        ml = str(m).lower()
        if "credit" in ml or "debit" in ml:
            return "Card", True
        if "paypal" in ml or "wallet" in ml:
            return "Digital Wallet", True
        if "bank" in ml or "transfer" in ml:
            return "Bank Transfer", False
        if "crypto" in ml:
            return "Crypto", True
        if "later" in ml or "bnpl" in ml:
            return "BNPL", True
        return "Other", False

    rows = []
    for m in methods:
        ptype, is_dig = classify(m)
        rows.append((str(m), ptype, is_dig))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.dim_payment RESTART IDENTITY CASCADE;")
        execute_values(cur,
            "INSERT INTO dw.dim_payment (payment_method, payment_type, is_digital) VALUES %s",
            rows, page_size=200)
    conn.commit()

    df = pd.read_sql("SELECT payment_key, payment_method FROM dw.dim_payment", conn)
    logger.info(f"  → dim_payment: {len(df)} rows.")
    return dict(zip(df["payment_method"], df["payment_key"]))


# ─────────────────────────────────────────────────────────────────────────────
# Fact table loaders
# ─────────────────────────────────────────────────────────────────────────────
def load_fact_sales(conn, items_df, orders_df, cust_map, prod_map, pay_map):
    logger.info("Loading fact_sales …")

    order_lookup_cols = ["order_id", "customer_id", "order_date", "ship_date",
                         "status", "payment_method", "currency_code",
                         "shipping_cost", "days_to_ship"]
    df = items_df.merge(orders_df[order_lookup_cols], on="order_id", how="left")

    # Customer → location lookup
    cust_loc_df = pd.read_sql(
        "SELECT customer_key, location_key FROM dw.dim_customer", conn)
    cust_loc = dict(zip(cust_loc_df["customer_key"], cust_loc_df["location_key"]))

    rows = []
    for _, r in df.iterrows():
        ck  = cust_map.get(safe(r.get("customer_id")))
        lk  = cust_loc.get(ck)
        pk  = prod_map.get(safe(r.get("product_id")))
        pyk = pay_map.get(r.get("payment_method"))
        odk = date_to_key(r.get("order_date"))
        sdk = date_to_key(r.get("ship_date"))

        rows.append((
            odk, sdk, ck, pk, lk, pyk,
            safe(r.get("order_id")),
            r.get("status"),
            r.get("currency_code"),
            safe(r.get("quantity")),
            safe(r.get("unit_price")),
            safe(r.get("discount_pct")),
            safe(r.get("discount_amt")),
            safe(r.get("line_gross")),
            safe(r.get("line_net")),
            safe(r.get("line_cost")),
            safe(r.get("line_margin")),
            safe(r.get("margin_pct")),
            safe(r.get("shipping_cost")),
            safe(r.get("days_to_ship")),
        ))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.fact_sales RESTART IDENTITY CASCADE;")
        execute_values(cur, """
            INSERT INTO dw.fact_sales (
                order_date_key, ship_date_key, customer_key, product_key,
                location_key, payment_key, order_id, order_status, currency_code,
                quantity, unit_price, discount_pct, discount_amt,
                line_gross, line_net, line_cost, line_margin, margin_pct,
                shipping_cost, days_to_ship
            ) VALUES %s
        """, rows, page_size=500)
    conn.commit()
    logger.info(f"  → fact_sales: {len(rows):,} rows.")


def load_fact_orders(conn, orders_df, cust_map, pay_map):
    logger.info("Loading fact_orders …")

    # Aggregate item measures to order level from staging
    items_agg = pd.read_sql("""
        SELECT
            order_id,
            COUNT(*)           AS num_line_items,
            SUM(quantity)      AS total_quantity,
            SUM(line_gross)    AS gross_revenue,
            SUM(discount_amt)  AS total_discount,
            SUM(line_net)      AS net_revenue,
            SUM(line_cost)     AS total_cost,
            SUM(line_margin)   AS gross_margin
        FROM staging.order_items
        GROUP BY order_id
    """, conn)

    df = orders_df.merge(items_agg, on="order_id", how="left")

    cust_loc_df = pd.read_sql(
        "SELECT customer_key, location_key FROM dw.dim_customer", conn)
    cust_loc = dict(zip(cust_loc_df["customer_key"], cust_loc_df["location_key"]))

    rows = []
    for _, r in df.iterrows():
        ck  = cust_map.get(safe(r.get("customer_id")))
        lk  = cust_loc.get(ck)
        pyk = pay_map.get(r.get("payment_method"))
        odk = date_to_key(r.get("order_date"))
        sdk = date_to_key(r.get("ship_date"))

        rows.append((
            odk, sdk, ck, lk, pyk,
            safe(r.get("order_id")),
            r.get("status"),
            r.get("currency_code"),
            safe(r.get("num_line_items")),
            safe(r.get("total_quantity")),
            safe(r.get("gross_revenue")),
            safe(r.get("total_discount")),
            safe(r.get("net_revenue")),
            safe(r.get("total_cost")),
            safe(r.get("gross_margin")),
            safe(r.get("shipping_cost")),
            safe(r.get("days_to_ship")),
        ))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.fact_orders RESTART IDENTITY CASCADE;")
        execute_values(cur, """
            INSERT INTO dw.fact_orders (
                order_date_key, ship_date_key, customer_key,
                location_key, payment_key, order_id, order_status, currency_code,
                num_line_items, total_quantity, gross_revenue, total_discount,
                net_revenue, total_cost, gross_margin, shipping_cost, days_to_ship
            ) VALUES %s
        """, rows, page_size=500)
    conn.commit()
    logger.info(f"  → fact_orders: {len(rows):,} rows.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("Data Warehouse Modeling Pipeline Starting")
    logger.info("=" * 60)

    conn = get_connection()
    try:
        # Step 1 – create schema
        logger.info("Creating DW schema …")
        with conn.cursor() as cur:
            cur.execute(DW_DDL)
        conn.commit()
        logger.info("  → Schema and tables created.")

        # Step 2 – date dimension
        build_and_load_dim_date(conn, "2021-01-01", "2026-12-31")

        # Step 3 – read staging
        logger.info("Reading staging tables …")
        countries_df = read_staging(conn, "countries")
        products_df  = read_staging(conn, "products")
        customers_df = read_staging(conn, "customers")
        orders_df    = read_staging(conn, "orders")
        items_df     = read_staging(conn, "order_items")

        # Step 4 – load dimensions (FK order matters)
        loc_map  = load_dim_location(conn, countries_df)
        cust_map = load_dim_customer(conn, customers_df, loc_map)
        prod_map = load_dim_product(conn, products_df)
        pay_map  = load_dim_payment(conn, orders_df)

        # Step 5 – load facts
        load_fact_sales(conn, items_df, orders_df, cust_map, prod_map, pay_map)
        load_fact_orders(conn, orders_df, cust_map, pay_map)

        # Step 6 – export DDL
        sql_dir = os.path.join(BASE_DIR, "sql")
        os.makedirs(sql_dir, exist_ok=True)
        with open(os.path.join(sql_dir, "dw_schema.sql"), "w") as fh:
            fh.write(DW_DDL)
        logger.info("  → DDL exported to sql/dw_schema.sql")

    finally:
        conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("Modeling complete — Data Warehouse is ready!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
