"""Bronze: raw files into parquet with the contract schema and ingestion metadata. No business logic here."""
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.contracts import SOURCES
from pipeline.spark import ROOT, write_table

RAW = Path(os.getenv("PIPELINE_RAW", ROOT / "data" / "sample"))


def read_source(spark: SparkSession, name: str, raw_dir: Path = RAW):
    spec = SOURCES[name]
    return (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(spec["schema"]).csv(str(raw_dir / spec["file"]))
            .withColumn("_source_file", F.lit(spec["file"]))
            .withColumn("_ingested_at", F.current_timestamp()))


def run(spark: SparkSession, raw_dir: Path = RAW) -> dict:
    counts = {}
    for name in SOURCES:
        df = read_source(spark, name, raw_dir)
        write_table(df, "bronze", name)
        counts[name] = df.count()
    return counts
