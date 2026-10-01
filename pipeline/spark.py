"""Spark session and table I/O shared by every stage.

Table I/O goes through write_table() and read_table(). On Linux (CI, Docker, a cluster) these are normal Spark
parquet reads and writes. On Windows without Hadoop's native helpers (winutils.exe, hadoop.dll), Spark cannot use the
local filesystem for parquet, so tables move through Arrow with pyarrow instead; this only applies to local
development and is logged. All transformations still run in Spark either way.
"""
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("pipeline")
ROOT = Path(__file__).resolve().parents[1]
LAKE = Path(os.getenv("PIPELINE_LAKE", ROOT / "lake"))


def spark_session(app: str = "olist-pipeline") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[*]")).appName(app)
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate())


def native_writes() -> bool:
    """False only on Windows without HADOOP_HOME, where Spark's local file writes fail."""
    return os.name != "nt" or bool(os.getenv("HADOOP_HOME"))


def table_path(layer: str, name: str) -> Path:
    return LAKE / layer / name


def write_table(df: DataFrame, layer: str, name: str, partition_by: str | None = None) -> Path:
    path = table_path(layer, name)
    if native_writes():
        writer = df.write.mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.parquet(str(path))
    else:
        import shutil

        import pyarrow.parquet as pq

        log.info("local Windows run: writing %s/%s through Arrow", layer, name)
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        table = df.toArrow()
        if partition_by:
            pq.write_to_dataset(table, str(path), partition_cols=[partition_by])
        else:
            pq.write_table(table, str(path / "part-0.parquet"))
    return path


def read_table(spark: SparkSession, layer: str, name: str) -> DataFrame:
    path = table_path(layer, name)
    if native_writes():
        return spark.read.parquet(str(path))
    import pyarrow.dataset as ds

    # same Windows limitation as writes: hand the Arrow table to Spark instead of reading through Hadoop
    return spark.createDataFrame(ds.dataset(str(path), format="parquet", partitioning="hive").to_table())
