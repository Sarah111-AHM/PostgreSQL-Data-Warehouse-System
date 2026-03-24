"""
stats.py
========
Insights & Statistics Generator
----------------------------------
Queries the DW (dw schema) to produce 13 analytical reports and exports
a Markdown stats report to reports/stats_report.md.
"""

import os
import json
import logging
import pandas as pd
import numpy as np
import psycopg2
from tabulate import tabulate
from datetime import date
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


def load_config(name):
    with open(os.path.join(BASE_DIR, "config", f"{name}.json")) as f:
        return json.load(f)


DB_CFG = load_config("db_config")


def get_connection():
    return psycopg2.connect(
        host=DB_CFG["host"], port=DB_CFG["port"],
        dbname=DB_CFG["database"],
        user=DB_CFG["user"], password=DB_CFG["password"],
    )


def q(conn, query):
    return pd.read_sql(query, conn)


def fmt_usd(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "N/A"


def fmt_k(v):
    try:
        return f"{int(v):,}"
    except Exception:
        return "N/A"


def df_to_md(df):
    return tabulate(df, headers="keys", tablefmt="pipe", showindex=False, floatfmt=".2f")


# ── Queries ───────────────────────────────────────────────────────────────────

def kpi_summary(conn):
    df = q(conn, """
        SELECT
            COUNT(DISTINCT order_id)     AS total_orders,
            SUM(total_quantity)          AS units_sold,
            SUM(gross_revenue)           AS gross_revenue,
            SUM(total_discount)          AS total_discounts,
            SUM(net_revenue)             AS net_revenue,
            SUM(total_cost)              AS total_cogs,
            SUM(gross_margin)            AS gross_profit,
            SUM(shipping_cost)           AS total_shipping,
            AVG(net_revenue)             AS avg_order_value,
            COUNT(DISTINCT customer_key) AS active_customers
        FROM dw.fact_orders
        WHERE order_status NOT IN ('Cancelled', 'Returned')
    """)
    return df.iloc[0].to_dict()


def revenue_by_year(conn):
    return q(conn, """
        SELECT
            d.year,
            COUNT(DISTINCT fo.order_id)           AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)  AS net_revenue,
            ROUND(SUM(fo.gross_margin)::numeric,2) AS gross_profit
        FROM dw.fact_orders fo
        JOIN dw.dim_date d ON fo.order_date_key = d.date_key
        WHERE fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY d.year ORDER BY d.year
    """)


def revenue_by_quarter(conn):
    return q(conn, """
        SELECT
            d.year || ' ' || d.quarter_name        AS period,
            COUNT(DISTINCT fo.order_id)             AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)   AS net_revenue,
            ROUND(SUM(fo.gross_margin)::numeric,2)  AS gross_profit,
            ROUND(AVG(fo.net_revenue)::numeric,2)   AS avg_order_value
        FROM dw.fact_orders fo
        JOIN dw.dim_date d ON fo.order_date_key = d.date_key
        WHERE fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY d.year, d.quarter, d.quarter_name
        ORDER BY d.year, d.quarter
    """)


def top_products(conn, n=10):
    return q(conn, f"""
        SELECT
            dp.product_name,
            dp.category,
            dp.brand,
            SUM(fs.quantity)                       AS units_sold,
            ROUND(SUM(fs.line_net)::numeric,2)     AS net_revenue,
            ROUND(SUM(fs.line_margin)::numeric,2)  AS gross_profit,
            ROUND(AVG(fs.margin_pct)::numeric,1)   AS avg_margin_pct
        FROM dw.fact_sales fs
        JOIN dw.dim_product dp ON fs.product_key = dp.product_key
        WHERE fs.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dp.product_name, dp.category, dp.brand
        ORDER BY net_revenue DESC LIMIT {n}
    """)


def revenue_by_category(conn):
    return q(conn, """
        SELECT
            dp.category,
            COUNT(DISTINCT fs.order_id)            AS orders,
            SUM(fs.quantity)                       AS units_sold,
            ROUND(SUM(fs.line_net)::numeric,2)     AS net_revenue,
            ROUND(SUM(fs.line_margin)::numeric,2)  AS gross_profit,
            ROUND(AVG(fs.margin_pct)::numeric,1)   AS avg_margin_pct
        FROM dw.fact_sales fs
        JOIN dw.dim_product dp ON fs.product_key = dp.product_key
        WHERE fs.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dp.category ORDER BY net_revenue DESC
    """)


def customer_segments(conn):
    return q(conn, """
        SELECT
            dc.loyalty_tier,
            COUNT(DISTINCT dc.customer_key)                        AS customers,
            COUNT(DISTINCT fo.order_id)                            AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)                  AS net_revenue,
            ROUND(AVG(fo.net_revenue)::numeric,2)                  AS avg_order_value,
            ROUND((SUM(fo.net_revenue) /
                   NULLIF(COUNT(DISTINCT dc.customer_key),0))::numeric,2) AS revenue_per_customer
        FROM dw.dim_customer dc
        LEFT JOIN dw.fact_orders fo
            ON dc.customer_key = fo.customer_key
            AND fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dc.loyalty_tier
        ORDER BY net_revenue DESC NULLS LAST
    """)


def geo_top_countries(conn, n=10):
    return q(conn, f"""
        SELECT
            dl.country_name,
            dl.region,
            COUNT(DISTINCT fo.order_id)            AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)  AS net_revenue,
            ROUND(AVG(fo.net_revenue)::numeric,2)  AS avg_order_value
        FROM dw.fact_orders fo
        JOIN dw.dim_location dl ON fo.location_key = dl.location_key
        WHERE fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dl.country_name, dl.region
        ORDER BY net_revenue DESC LIMIT {n}
    """)


def geo_by_region(conn):
    return q(conn, """
        SELECT
            dl.region,
            COUNT(DISTINCT fo.order_id)            AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)  AS net_revenue,
            ROUND(AVG(fo.net_revenue)::numeric,2)  AS avg_order_value
        FROM dw.fact_orders fo
        JOIN dw.dim_location dl ON fo.location_key = dl.location_key
        WHERE fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dl.region ORDER BY net_revenue DESC
    """)


def payment_analysis(conn):
    return q(conn, """
        SELECT
            dp.payment_method,
            dp.payment_type,
            dp.is_digital,
            COUNT(DISTINCT fo.order_id)                         AS orders,
            ROUND(100.0 * COUNT(DISTINCT fo.order_id) /
                  SUM(COUNT(DISTINCT fo.order_id)) OVER (),1)   AS pct_of_orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)               AS net_revenue
        FROM dw.fact_orders fo
        JOIN dw.dim_payment dp ON fo.payment_key = dp.payment_key
        GROUP BY dp.payment_method, dp.payment_type, dp.is_digital
        ORDER BY orders DESC
    """)


def order_status_breakdown(conn):
    return q(conn, """
        SELECT
            order_status,
            COUNT(*)                                            AS orders,
            ROUND(100.0 * COUNT(*) /
                  SUM(COUNT(*)) OVER (),1)                     AS pct,
            ROUND(SUM(net_revenue)::numeric,2)                 AS net_revenue,
            ROUND(SUM(gross_margin)::numeric,2)                AS gross_margin
        FROM dw.fact_orders
        GROUP BY order_status ORDER BY orders DESC
    """)


def shipping_performance(conn):
    return q(conn, """
        SELECT
            CASE
                WHEN days_to_ship <= 2  THEN '1-2 days (Express)'
                WHEN days_to_ship <= 5  THEN '3-5 days (Standard)'
                WHEN days_to_ship <= 10 THEN '6-10 days (Slow)'
                ELSE '11+ days (Very Slow)'
            END                                                 AS shipping_band,
            COUNT(*)                                            AS orders,
            ROUND(100.0 * COUNT(*) /
                  SUM(COUNT(*)) OVER (),1)                     AS pct,
            ROUND(AVG(shipping_cost)::numeric,2)               AS avg_shipping_cost
        FROM dw.fact_orders
        WHERE days_to_ship IS NOT NULL
          AND order_status NOT IN ('Processing')
        GROUP BY 1 ORDER BY orders DESC
    """)


def age_group_revenue(conn):
    return q(conn, """
        SELECT
            dc.age_group,
            COUNT(DISTINCT dc.customer_key)            AS customers,
            COUNT(DISTINCT fo.order_id)                AS orders,
            ROUND(SUM(fo.net_revenue)::numeric,2)      AS net_revenue,
            ROUND(AVG(fo.net_revenue)::numeric,2)      AS avg_order_value
        FROM dw.dim_customer dc
        LEFT JOIN dw.fact_orders fo
            ON dc.customer_key = fo.customer_key
            AND fo.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dc.age_group
        ORDER BY net_revenue DESC NULLS LAST
    """)


def margin_by_category(conn):
    return q(conn, """
        SELECT
            dp.category,
            ROUND(AVG(fs.margin_pct)::numeric,1)    AS avg_margin_pct,
            ROUND(MIN(fs.margin_pct)::numeric,1)    AS min_margin_pct,
            ROUND(MAX(fs.margin_pct)::numeric,1)    AS max_margin_pct,
            ROUND(SUM(fs.line_margin)::numeric,2)   AS total_margin
        FROM dw.fact_sales fs
        JOIN dw.dim_product dp ON fs.product_key = dp.product_key
        WHERE fs.order_status NOT IN ('Cancelled','Returned')
        GROUP BY dp.category ORDER BY avg_margin_pct DESC
    """)


# ── Report builder ────────────────────────────────────────────────────────────

def generate_report(conn):
    logger.info("Running analytical queries …")

    kpis   = kpi_summary(conn)
    yearly = revenue_by_year(conn)
    qtly   = revenue_by_quarter(conn)
    top_p  = top_products(conn)
    cat_r  = revenue_by_category(conn)
    seg    = customer_segments(conn)
    geo    = geo_top_countries(conn)
    reg    = geo_by_region(conn)
    pay    = payment_analysis(conn)
    status = order_status_breakdown(conn)
    ship   = shipping_performance(conn)
    age    = age_group_revenue(conn)
    margin = margin_by_category(conn)

    # YoY growth column
    yearly = yearly.copy()
    if len(yearly) >= 2:
        yearly["yoy_growth_pct"] = (yearly["net_revenue"].pct_change() * 100).round(1)
    else:
        yearly["yoy_growth_pct"] = None

    today = date.today().strftime("%B %d, %Y")

    report = f"""# E-Commerce Data Warehouse — Statistics Report

**Generated:** {today}
**Database:** PostgreSQL — `dw` schema (Star Schema)
**Pipeline stages:** ingestion → transformation → modeling → stats

---

## 1. Executive KPI Summary

| Metric | Value |
|--------|-------|
| Total Orders (excl. cancellations/returns) | {fmt_k(kpis.get('total_orders', 0))} |
| Units Sold | {fmt_k(kpis.get('units_sold', 0))} |
| Gross Revenue | {fmt_usd(kpis.get('gross_revenue', 0))} |
| Total Discounts Given | {fmt_usd(kpis.get('total_discounts', 0))} |
| **Net Revenue** | **{fmt_usd(kpis.get('net_revenue', 0))}** |
| Total COGS | {fmt_usd(kpis.get('total_cogs', 0))} |
| **Gross Profit** | **{fmt_usd(kpis.get('gross_profit', 0))}** |
| Total Shipping Revenue | {fmt_usd(kpis.get('total_shipping', 0))} |
| Average Order Value | {fmt_usd(kpis.get('avg_order_value', 0))} |
| Unique Active Customers | {fmt_k(kpis.get('active_customers', 0))} |

---

## 2. Annual Revenue with Year-over-Year Growth

{df_to_md(yearly)}

---

## 3. Quarterly Revenue Trend

{df_to_md(qtly)}

---

## 4. Top 10 Products by Net Revenue

{df_to_md(top_p)}

---

## 5. Revenue & Margin by Product Category

{df_to_md(cat_r)}

---

## 6. Customer Segmentation by Loyalty Tier

{df_to_md(seg)}

---

## 7. Top 10 Countries by Net Revenue

{df_to_md(geo)}

---

## 8. Revenue by Region

{df_to_md(reg)}

---

## 9. Payment Method Analysis

{df_to_md(pay)}

---

## 10. Order Status Breakdown

{df_to_md(status)}

---

## 11. Shipping Performance Bands

{df_to_md(ship)}

---

## 12. Revenue by Customer Age Group

{df_to_md(age)}

---

## 13. Gross Margin Analysis by Category

{df_to_md(margin)}

---

## 14. Key Findings & Narrative Insights

"""

    insights = []

    if not cat_r.empty:
        b = cat_r.iloc[0]
        insights.append(
            f"- **{b['category']}** leads all categories with {fmt_usd(b['net_revenue'])} "
            f"net revenue across {fmt_k(b['orders'])} orders."
        )

    if not reg.empty:
        b = reg.iloc[0]
        insights.append(
            f"- **{b['region']}** is the strongest geographic region, "
            f"contributing {fmt_usd(b['net_revenue'])} in net revenue."
        )

    if not seg.empty:
        top_t = seg.dropna(subset=["net_revenue"]).sort_values("net_revenue", ascending=False)
        if not top_t.empty:
            t = top_t.iloc[0]
            insights.append(
                f"- The **{t['loyalty_tier']}** tier generates the most revenue "
                f"({fmt_usd(t['net_revenue'])}) with {fmt_k(t['customers'])} customers."
            )

    if not pay.empty:
        tp = pay.iloc[0]
        insights.append(
            f"- **{tp['payment_method']}** is the most popular payment method, "
            f"used in {tp['pct_of_orders']}% of all orders."
        )

    if not status.empty:
        c_pct = status[status["order_status"].isin(["Cancelled","Returned"])]["pct"].sum()
        insights.append(
            f"- The combined cancellation and return rate is **{c_pct:.1f}%**, "
            f"indicating a healthy fulfilment operation."
        )

    if not ship.empty:
        fast_pct = ship[ship["shipping_band"].str.contains("1-2|3-5", na=False)]["pct"].sum()
        insights.append(
            f"- **{fast_pct:.1f}%** of fulfilled orders arrive within 5 days "
            f"(express or standard shipping bands)."
        )

    if not margin.empty:
        bm = margin.iloc[0]
        insights.append(
            f"- **{bm['category']}** achieves the highest average gross margin "
            f"at {bm['avg_margin_pct']}%, totalling {fmt_usd(bm['total_margin'])} profit."
        )

    yoy_rows = yearly.dropna(subset=["yoy_growth_pct"])
    if not yoy_rows.empty:
        last = yoy_rows.iloc[-1]
        g = last["yoy_growth_pct"]
        direction = "grew" if g > 0 else "declined"
        insights.append(
            f"- Revenue {direction} by **{abs(g):.1f}%** in {int(last['year'])} "
            f"vs the prior year."
        )

    report += "\n".join(insights)
    report += "\n\n---\n*Report auto-generated by `scripts/stats.py` — E-Commerce Data Warehouse Project*\n"

    return report


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Generating Statistics & Insights Report")
    logger.info("=" * 60)

    conn = get_connection()
    try:
        report = generate_report(conn)
    finally:
        conn.close()

    report_path = os.path.join(REPORTS_DIR, "stats_report.md")
    with open(report_path, "w") as fh:
        fh.write(report)

    logger.info(f"\nFull report → {report_path}")
    print("\n" + "\n".join(report.split("\n")[:55]))
    print("\n… [see reports/stats_report.md for the full report]")


if __name__ == "__main__":
    main()
