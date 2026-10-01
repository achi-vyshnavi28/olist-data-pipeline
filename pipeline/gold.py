"""Gold: a star schema for analytics.

    fact_order_items  grain: one row per order line     -> dim_product, dim_seller, dim_customer, dim_date
    fact_orders       grain: one row per order          -> dim_customer, dim_date
    mart_daily_kpis   grain: one row per purchase date  (orders, revenue, late share, average review)

Dimensions use stable surrogate keys (xxhash64 of the natural key), so facts join on integers and reloads are
deterministic.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.spark import read_table, write_table


def _sk(*cols) -> F.Column:
    return F.xxhash64(*cols)


def dim_date(orders: DataFrame) -> DataFrame:
    bounds = orders.select(F.min(F.to_date("order_purchase_timestamp")).alias("lo"),
                           F.max(F.to_date("order_estimated_delivery_date")).alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return days.select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                       F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
                       F.dayofweek("date").alias("day_of_week"), F.dayofweek("date").isin(1, 7).alias("is_weekend"))


def _date_key(col) -> F.Column:
    return F.date_format(F.to_date(col), "yyyyMMdd").cast("int")


def build(s: dict[str, DataFrame]) -> dict[str, DataFrame]:
    dim_customer = s["customers"].select(_sk("customer_unique_id").alias("customer_key"), "customer_unique_id",
                                         "customer_city", "customer_state").dropDuplicates(["customer_key"])
    dim_seller = s["sellers"].select(_sk("seller_id").alias("seller_key"), "seller_id", "seller_city", "seller_state")
    dim_product = s["products"].select(_sk("product_id").alias("product_key"), "product_id", "category",
                                       "product_weight_g", "product_photos_qty")
    order_customer = s["orders"].join(s["customers"].select("customer_id", "customer_unique_id"), "customer_id", "left")

    paid = s["payments"].groupBy("order_id").agg(F.sum("payment_value").alias("payment_value"),
                                                 F.first("payment_type").alias("payment_type"))
    review = s["reviews"].groupBy("order_id").agg(F.avg("review_score").alias("review_score"))
    lines = s["order_items"].groupBy("order_id").agg(F.count("*").alias("items"), F.sum("price").alias("items_value"),
                                                    F.sum("freight_value").alias("freight_value"))

    fact_orders = (order_customer.join(lines, "order_id", "left").join(paid, "order_id", "left").join(review, "order_id", "left")
                   .select("order_id", _sk("customer_unique_id").alias("customer_key"),
                           _date_key("order_purchase_timestamp").alias("purchase_date_key"), "order_status",
                           F.coalesce("items", F.lit(0)).alias("items"), "items_value", "freight_value", "payment_value",
                           "payment_type", "delivery_days", "is_late", "review_score"))
    fact_order_items = (s["order_items"].join(order_customer.select("order_id", "customer_unique_id", "order_purchase_timestamp"), "order_id")
                        .select("order_id", "order_item_id", _sk("product_id").alias("product_key"),
                                _sk("seller_id").alias("seller_key"), _sk("customer_unique_id").alias("customer_key"),
                                _date_key("order_purchase_timestamp").alias("purchase_date_key"), "price", "freight_value"))
    mart_daily_kpis = (fact_orders.groupBy("purchase_date_key").agg(
        F.count("*").alias("orders"),
        F.round(F.sum("payment_value"), 2).alias("revenue"),
        F.round(F.avg(F.col("is_late").cast("int")), 4).alias("late_share"),
        F.round(F.avg(F.col("order_status").eqNullSafe("canceled").cast("int")), 4).alias("cancel_share"),
        F.round(F.avg("review_score"), 3).alias("avg_review")))
    return {"dim_customer": dim_customer, "dim_seller": dim_seller, "dim_product": dim_product,
            "dim_date": dim_date(s["orders"]), "fact_orders": fact_orders, "fact_order_items": fact_order_items,
            "mart_daily_kpis": mart_daily_kpis}


def run(spark: SparkSession) -> dict:
    silver = {n: read_table(spark, "silver", n) for n in
              ["orders", "order_items", "customers", "sellers", "products", "payments", "reviews"]}
    tables = build(silver)
    for name, df in tables.items():
        write_table(df, "gold", name)
    return {n: df.count() for n, df in tables.items()}
