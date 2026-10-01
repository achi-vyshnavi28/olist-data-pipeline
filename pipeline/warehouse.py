"""Load the gold star schema into an analytical warehouse and expose analyst views.

DuckDB is the default warehouse (a single file, no server). Analyst views mask PII columns, so the governance rule
"analysts see city level data, never zip codes" is enforced in the warehouse, not left to convention.
"""
import os

import duckdb

from pipeline.spark import LAKE, ROOT

GOLD = ["dim_customer", "dim_seller", "dim_product", "dim_date", "fact_orders", "fact_order_items", "mart_daily_kpis"]
WAREHOUSE = os.getenv("PIPELINE_WAREHOUSE", str(ROOT / "warehouse" / "olist.duckdb"))

VIEWS = {
    "v_monthly_revenue": """
        SELECT d.year, d.month, COUNT(*) AS orders, ROUND(SUM(f.payment_value), 2) AS revenue,
               ROUND(AVG(CASE WHEN f.is_late THEN 1 ELSE 0 END), 4) AS late_share
        FROM gold.fact_orders f JOIN gold.dim_date d ON d.date_key = f.purchase_date_key
        GROUP BY d.year, d.month ORDER BY d.year, d.month""",
    "v_category_performance": """
        SELECT p.category, COUNT(*) AS items, ROUND(SUM(i.price), 2) AS revenue,
               RANK() OVER (ORDER BY SUM(i.price) DESC) AS revenue_rank
        FROM gold.fact_order_items i JOIN gold.dim_product p USING (product_key)
        GROUP BY p.category""",
    "v_customers_masked": """
        SELECT customer_key, customer_city, customer_state FROM gold.dim_customer""",
}


def load(path: str = WAREHOUSE) -> dict:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    counts = {}
    for t in GOLD:
        src = (LAKE / "gold" / t).as_posix() + "/**/*.parquet"
        con.execute(f"CREATE OR REPLACE TABLE gold.{t} AS SELECT * FROM read_parquet('{src}', hive_partitioning = true)")
        counts[t] = con.execute(f"SELECT COUNT(*) FROM gold.{t}").fetchone()[0]
    for name, sql in VIEWS.items():
        con.execute(f"CREATE OR REPLACE VIEW analytics.{name} AS {sql}")
    con.close()
    return counts


def query(sql: str, path: str = WAREHOUSE):
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()
