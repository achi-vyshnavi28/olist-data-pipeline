"""Silver: cleaned, typed, deduplicated tables. Rows that break a rule are quarantined with the reason, not dropped
silently, so the quality report can say exactly what was removed and why."""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.contracts import ORDER_STATUSES, PAYMENT_TYPES
from pipeline.spark import read_table, write_table

_META = ["_source_file", "_ingested_at"]


def _split(df: DataFrame, bad: F.Column, reason: str):
    flagged = df.withColumn("_reject_reason", F.when(bad, F.lit(reason)))
    return flagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason"), flagged.filter(F.col("_reject_reason").isNotNull())


def _latest_per_key(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep one row per key, the most recent by order_col (Olist reviews repeat review_id across answers)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_orders(df: DataFrame):
    df = df.withColumn("order_status", F.lower(F.trim("order_status")))
    good, rej1 = _split(df, F.col("order_id").isNull() | F.col("order_purchase_timestamp").isNull(), "missing order id or purchase time")
    good, rej2 = _split(good, ~F.col("order_status").isin(ORDER_STATUSES), "unknown order status")
    good, rej3 = _split(good, F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"), "delivered before purchase")
    good = good.dropDuplicates(["order_id"]).withColumn(
        "delivery_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp")).withColumn(
        "is_late", F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
    return good.drop(*_META), rej1.unionByName(rej2).unionByName(rej3)


def clean_items(df: DataFrame):
    good, rej = _split(df, (F.col("price") < 0) | (F.col("freight_value") < 0) | F.col("product_id").isNull(), "negative amount or missing product")
    return good.dropDuplicates(["order_id", "order_item_id"]).drop(*_META), rej


def clean_places(df: DataFrame, prefix: str) -> DataFrame:
    return (df.withColumn(f"{prefix}_city", F.initcap(F.trim(F.col(f"{prefix}_city"))))
              .withColumn(f"{prefix}_state", F.upper(F.trim(F.col(f"{prefix}_state"))))
              .withColumn(f"{prefix}_zip_code_prefix", F.lpad(f"{prefix}_zip_code_prefix", 5, "0"))
              .drop(*_META))


def clean_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    return (products.join(translation.drop(*_META), "product_category_name", "left")
            .withColumn("category", F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")))
            .drop("product_category_name_english").dropDuplicates(["product_id"]).drop(*_META))


def clean_payments(df: DataFrame):
    good, rej = _split(df, ~F.col("payment_type").isin(PAYMENT_TYPES) | (F.col("payment_value") < 0), "bad payment type or value")
    return good.drop(*_META), rej


def clean_reviews(df: DataFrame):
    good, rej = _split(df, ~F.col("review_score").between(1, 5), "review score outside 1 to 5")
    return _latest_per_key(good, ["review_id", "order_id"], "review_answer_timestamp").drop(*_META), rej


def run(spark: SparkSession) -> dict:
    b = {n: read_table(spark, "bronze", n) for n in
         ["orders", "order_items", "customers", "sellers", "products", "payments", "reviews", "category_translation"]}
    orders, r_orders = clean_orders(b["orders"])
    items, r_items = clean_items(b["order_items"])
    payments, r_pay = clean_payments(b["payments"])
    reviews, r_rev = clean_reviews(b["reviews"])
    tables = {
        "orders": orders, "order_items": items, "payments": payments, "reviews": reviews,
        "customers": clean_places(b["customers"], "customer").dropDuplicates(["customer_id"]),
        "sellers": clean_places(b["sellers"], "seller").dropDuplicates(["seller_id"]),
        "products": clean_products(b["products"], b["category_translation"]),
    }
    for name, df in tables.items():
        write_table(df, "silver", name)
    rejects = (r_orders.select(F.lit("orders").alias("table"), F.col("order_id").alias("key"), "_reject_reason")
               .unionByName(r_items.select(F.lit("order_items").alias("table"), F.col("order_id").alias("key"), "_reject_reason"))
               .unionByName(r_pay.select(F.lit("payments").alias("table"), F.col("order_id").alias("key"), "_reject_reason"))
               .unionByName(r_rev.select(F.lit("reviews").alias("table"), F.col("review_id").alias("key"), "_reject_reason")))
    write_table(rejects, "quarantine", "rejected_rows")
    return {"rows": {n: df.count() for n, df in tables.items()}, "rejected": rejects.count()}
