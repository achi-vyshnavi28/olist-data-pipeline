"""Data quality gate between silver and gold, and again on gold.

Each check returns failing-row counts; "error" checks stop the pipeline, "warn" checks are reported. The report is
written to lake/reports/quality.json so every run leaves an auditable record.
"""
import json
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.contracts import ORDER_STATUSES
from pipeline.spark import LAKE, read_table


@dataclass
class Check:
    table: str
    name: str
    severity: str
    failed: int
    total: int

    @property
    def passed(self) -> bool:
        return self.failed == 0


def not_null(df, table, cols, severity="error"):
    total = df.count()
    return [Check(table, f"not_null:{c}", severity, df.filter(F.col(c).isNull()).count(), total) for c in cols]


def unique(df, table, keys, severity="error"):
    total = df.count()
    dupes = df.groupBy(*keys).count().filter("count > 1").agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
    return [Check(table, f"unique:{'+'.join(keys)}", severity, int(dupes), total)]


def references(df, table, col, parent, parent_col, parent_name, severity="error"):
    orphans = df.join(parent.select(F.col(parent_col).alias(col)).distinct(), col, "left_anti").count()
    return [Check(table, f"fk:{col}->{parent_name}", severity, orphans, df.count())]


def accepted(df, table, col, values, severity="error"):
    return [Check(table, f"accepted:{col}", severity, df.filter(~F.col(col).isin(values)).count(), df.count())]


def in_range(df, table, col, lo, hi, severity="warn"):
    return [Check(table, f"range:{col}[{lo},{hi}]", severity, df.filter(~F.col(col).between(lo, hi)).count(), df.count())]


def silver_checks(s: dict[str, DataFrame]) -> list[Check]:
    c = []
    c += not_null(s["orders"], "orders", ["order_id", "customer_id", "order_purchase_timestamp"])
    c += unique(s["orders"], "orders", ["order_id"])
    c += accepted(s["orders"], "orders", "order_status", ORDER_STATUSES)
    c += references(s["orders"], "orders", "customer_id", s["customers"], "customer_id", "customers")
    c += unique(s["order_items"], "order_items", ["order_id", "order_item_id"])
    c += references(s["order_items"], "order_items", "order_id", s["orders"], "order_id", "orders")
    c += references(s["order_items"], "order_items", "product_id", s["products"], "product_id", "products")
    c += references(s["order_items"], "order_items", "seller_id", s["sellers"], "seller_id", "sellers")
    c += in_range(s["order_items"], "order_items", "price", 0, 10000)
    c += in_range(s["reviews"], "reviews", "review_score", 1, 5, "error")
    c += in_range(s["orders"].filter("delivery_days is not null"), "orders", "delivery_days", 0, 120)
    return c


def gold_checks(g: dict[str, DataFrame]) -> list[Check]:
    c = []
    for dim, key in [("dim_customer", "customer_key"), ("dim_seller", "seller_key"), ("dim_product", "product_key"), ("dim_date", "date_key")]:
        c += unique(g[dim], dim, [key])
    c += unique(g["fact_orders"], "fact_orders", ["order_id"])
    c += unique(g["fact_order_items"], "fact_order_items", ["order_id", "order_item_id"])
    c += references(g["fact_order_items"], "fact_order_items", "product_key", g["dim_product"], "product_key", "dim_product")
    c += references(g["fact_order_items"], "fact_order_items", "seller_key", g["dim_seller"], "seller_key", "dim_seller")
    c += references(g["fact_orders"], "fact_orders", "purchase_date_key", g["dim_date"], "date_key", "dim_date")
    return c


class QualityError(RuntimeError):
    pass


def report(checks: list[Check], stage: str) -> dict:
    out = {"stage": stage, "checks": len(checks), "failed_errors": sum(1 for x in checks if not x.passed and x.severity == "error"),
           "warnings": sum(1 for x in checks if not x.passed and x.severity == "warn"),
           "results": [{**asdict(x), "passed": x.passed} for x in checks]}
    path = LAKE / "reports" / f"quality_{stage}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


def gate(spark: SparkSession, stage: str) -> dict:
    names = (["orders", "order_items", "customers", "sellers", "products", "reviews"] if stage == "silver" else
             ["dim_customer", "dim_seller", "dim_product", "dim_date", "fact_orders", "fact_order_items"])
    tables = {n: read_table(spark, stage, n) for n in names}
    out = report(silver_checks(tables) if stage == "silver" else gold_checks(tables), stage)
    if out["failed_errors"]:
        failing = [r["name"] + " on " + r["table"] for r in out["results"] if not r["passed"] and r["severity"] == "error"]
        raise QualityError(f"{stage} quality gate failed: {failing}")
    return out
