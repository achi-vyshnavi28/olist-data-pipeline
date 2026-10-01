"""Unit tests for the cleaning rules and quality checks, on small hand-made frames with known bad rows."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from pipeline import quality, silver

T = datetime


def orders_df(spark, rows):
    return spark.createDataFrame(rows, "order_id STRING, customer_id STRING, order_status STRING, "
                                       "order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, "
                                       "order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP, "
                                       "order_estimated_delivery_date TIMESTAMP, _source_file STRING, _ingested_at TIMESTAMP")


def test_orders_are_cleaned_and_bad_rows_quarantined_with_a_reason(spark):
    now = T(2026, 1, 1)
    df = orders_df(spark, [
        ("o1", "c1", " Delivered ", T(2018, 1, 1), None, None, T(2018, 1, 11), T(2018, 1, 8), "f", now),   # late
        ("o2", "c2", "delivered", T(2018, 1, 1), None, None, T(2018, 1, 5), T(2018, 1, 8), "f", now),      # on time
        ("o3", "c3", "lost_in_space", T(2018, 1, 1), None, None, None, T(2018, 1, 8), "f", now),          # bad status
        ("o4", "c4", "delivered", T(2018, 1, 9), None, None, T(2018, 1, 2), T(2018, 1, 20), "f", now),     # time travel
        ("o2", "c2", "delivered", T(2018, 1, 1), None, None, T(2018, 1, 5), T(2018, 1, 8), "f", now),      # duplicate
    ])
    good, rejected = silver.clean_orders(df)
    rows = {r.order_id: r for r in good.collect()}
    assert set(rows) == {"o1", "o2"} and "_source_file" not in good.columns
    assert rows["o1"].order_status == "delivered" and rows["o1"].is_late and rows["o1"].delivery_days == 10
    assert not rows["o2"].is_late
    reasons = {r.order_id: r._reject_reason for r in rejected.collect()}
    assert reasons == {"o3": "unknown order status", "o4": "delivered before purchase"}


def test_reviews_keep_the_latest_answer_and_reject_impossible_scores(spark):
    df = spark.createDataFrame([
        ("r1", "o1", 3, None, None, T(2018, 1, 1), T(2018, 1, 2), "f", T(2026, 1, 1)),
        ("r1", "o1", 5, None, None, T(2018, 1, 1), T(2018, 1, 9), "f", T(2026, 1, 1)),
        ("r2", "o2", 7, None, None, T(2018, 1, 1), T(2018, 1, 2), "f", T(2026, 1, 1)),
    ], "review_id STRING, order_id STRING, review_score INT, review_comment_title STRING, review_comment_message STRING, "
       "review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP, _source_file STRING, _ingested_at TIMESTAMP")
    good, rejected = silver.clean_reviews(df)
    assert [(r.review_id, r.review_score) for r in good.collect()] == [("r1", 5)]
    assert [r.review_id for r in rejected.collect()] == ["r2"]


def test_places_are_standardized(spark):
    df = spark.createDataFrame([("c1", "u1", "1234", "  sao paulo ", "sp", "f", T(2026, 1, 1))],
                               "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, "
                               "customer_city STRING, customer_state STRING, _source_file STRING, _ingested_at TIMESTAMP")
    r = silver.clean_places(df, "customer").first()
    assert (r.customer_zip_code_prefix, r.customer_city, r.customer_state) == ("01234", "Sao Paulo", "SP")


def test_quality_checks_find_duplicates_orphans_and_bad_values(spark):
    parent = spark.createDataFrame([("p1",), ("p2",)], "product_id STRING")
    child = spark.createDataFrame([("o1", 1, "p1"), ("o1", 1, "p1"), ("o2", 1, "p9")], "order_id STRING, order_item_id INT, product_id STRING")
    dupes = quality.unique(child, "items", ["order_id", "order_item_id"])[0]
    orphans = quality.references(child, "items", "product_id", parent, "product_id", "products")[0]
    nulls = quality.not_null(child.withColumn("x", F.lit(None).cast("string")), "items", ["x"])[0]
    assert (dupes.failed, orphans.failed, nulls.failed) == (2, 1, 3)
    assert not dupes.passed and quality.unique(parent, "products", ["product_id"])[0].passed


def test_report_counts_errors_and_warnings_separately():
    checks = [quality.Check("t", "a", "error", 1, 10), quality.Check("t", "b", "warn", 2, 10), quality.Check("t", "c", "error", 0, 10)]
    out = quality.report(checks, "unit")
    assert (out["checks"], out["failed_errors"], out["warnings"]) == (3, 1, 1)
