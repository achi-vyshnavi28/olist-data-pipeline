"""Governance: a data catalog generated from the contracts and the lake itself.

For every table it records the layer, owner, columns with types, which columns are PII (masked in the analyst view),
row count, and lineage (which upstream tables it is built from). Written to lake/reports/catalog.json each run.
"""
import json

from pyspark.sql import SparkSession

from pipeline.contracts import SOURCES
from pipeline.spark import LAKE, read_table

LINEAGE = {
    ("silver", "orders"): ["bronze.orders"], ("silver", "order_items"): ["bronze.order_items"],
    ("silver", "customers"): ["bronze.customers"], ("silver", "sellers"): ["bronze.sellers"],
    ("silver", "products"): ["bronze.products", "bronze.category_translation"],
    ("silver", "payments"): ["bronze.payments"], ("silver", "reviews"): ["bronze.reviews"],
    ("gold", "dim_customer"): ["silver.customers"], ("gold", "dim_seller"): ["silver.sellers"],
    ("gold", "dim_product"): ["silver.products"], ("gold", "dim_date"): ["silver.orders"],
    ("gold", "fact_orders"): ["silver.orders", "silver.customers", "silver.order_items", "silver.payments", "silver.reviews"],
    ("gold", "fact_order_items"): ["silver.order_items", "silver.orders", "silver.customers"],
    ("gold", "mart_daily_kpis"): ["gold.fact_orders"],
}
OWNER = {"orders": "commerce", "order_items": "commerce", "customers": "crm", "sellers": "marketplace",
         "products": "catalog", "payments": "finance", "reviews": "cx"}
PII = {col for spec in SOURCES.values() for col in spec["pii"]}


def build(spark: SparkSession) -> dict:
    tables = []
    for (layer, name), upstream in LINEAGE.items():
        df = read_table(spark, layer, name)
        base = name.removeprefix("dim_").removeprefix("fact_").rstrip("s") + "s"
        tables.append({"table": f"{layer}.{name}", "owner": OWNER.get(name, OWNER.get(base, "analytics")),
                       "rows": df.count(), "upstream": upstream,
                       "columns": [{"name": f.name, "type": f.dataType.simpleString(), "pii": f.name in PII} for f in df.schema.fields]})
    out = {"tables": tables, "pii_columns": sorted(PII)}
    path = LAKE / "reports" / "catalog.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out
