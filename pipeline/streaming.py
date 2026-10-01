"""Streaming: real Olist orders replayed as a live event stream, aggregated with Spark Structured Streaming.

    replay()    writes orders as small JSON event files into a landing folder, in purchase-time order
    run()       reads the folder as a stream, applies a 2 hour watermark on event time, and keeps hourly
                tumbling-window counts and revenue; late events beyond the watermark are dropped by Spark

Needs Spark's local file system support, so it runs on Linux (CI, Docker, a cluster), not on plain Windows.
"""
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EVENT_SCHEMA = "order_id STRING, customer_id STRING, order_status STRING, event_time TIMESTAMP, payment_value DOUBLE"


def replay(spark: SparkSession, landing: Path, files: int = 5, limit: int | None = None) -> int:
    """Turn silver orders plus payments into ordered event files (one file = one micro-batch)."""
    from pipeline.spark import read_table

    orders = read_table(spark, "silver", "orders")
    paid = read_table(spark, "silver", "payments").groupBy("order_id").agg(F.sum("payment_value").alias("payment_value"))
    events = (orders.join(paid, "order_id", "left")
              .select("order_id", "customer_id", "order_status",
                      F.date_format("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss").alias("event_time"), "payment_value")
              .orderBy("event_time"))
    rows = [r.asDict() for r in (events.limit(limit) if limit else events).collect()]
    landing.mkdir(parents=True, exist_ok=True)
    size = max(1, -(-len(rows) // files))
    for i in range(0, len(rows), size):
        (landing / f"events_{i // size:04d}.json").write_text("\n".join(json.dumps(r) for r in rows[i:i + size]), encoding="utf-8")
    return len(rows)


def hourly_stream(spark: SparkSession, landing: Path):
    events = spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 1).json(str(landing))
    return (events.withWatermark("event_time", "2 hours")
            .groupBy(F.window("event_time", "1 hour").alias("w"))
            .agg(F.count("*").alias("orders"), F.round(F.sum("payment_value"), 2).alias("revenue"),
                 F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias("canceled"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "orders", "revenue", "canceled"))


def run(spark: SparkSession, landing: Path, checkpoint: Path, sink: str = "memory"):
    """Process everything in the landing folder and return the hourly aggregates (memory sink) as rows."""
    q = (hourly_stream(spark, landing).writeStream.outputMode("update").format(sink).queryName("hourly_orders")
         .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start())
    q.awaitTermination()
    return spark.sql("SELECT window_start, MAX(orders) AS orders, MAX(revenue) AS revenue, MAX(canceled) AS canceled "
                     "FROM hourly_orders GROUP BY window_start ORDER BY window_start").collect()
