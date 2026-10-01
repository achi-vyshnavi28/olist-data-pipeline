"""Run the batch pipeline end to end, or one stage at a time (the Airflow DAG calls the stages individually).

    python -m pipeline.run                     all stages on data/sample
    python -m pipeline.run bronze silver       selected stages
    PIPELINE_RAW=path/to/olist python -m pipeline.run   on the full dataset
"""
import json
import logging
import sys
import time

from pipeline import bronze, catalog, gold, quality, silver, warehouse
from pipeline.spark import LAKE, spark_session

STAGES = ["bronze", "silver", "quality_silver", "gold", "quality_gold", "catalog", "warehouse"]


def run_stage(stage: str, spark=None) -> dict:
    if stage == "warehouse":
        return warehouse.load()
    spark = spark or spark_session()
    return {"bronze": lambda: bronze.run(spark), "silver": lambda: silver.run(spark),
            "quality_silver": lambda: quality.gate(spark, "silver"), "gold": lambda: gold.run(spark),
            "quality_gold": lambda: quality.gate(spark, "gold"),
            "catalog": lambda: {"tables": len(catalog.build(spark)["tables"])}}[stage]()


def main(stages: list[str]) -> dict:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = spark_session()
    summary = {}
    for stage in stages:
        t = time.perf_counter()
        out = run_stage(stage, spark)
        summary[stage] = {"seconds": round(time.perf_counter() - t, 1),
                          "result": {k: v for k, v in out.items() if k != "results"} if isinstance(out, dict) else out}
        logging.info("%s done in %.1fs", stage, summary[stage]["seconds"])
    (LAKE / "reports").mkdir(parents=True, exist_ok=True)
    (LAKE / "reports" / "run_summary.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    spark.stop()
    return summary


if __name__ == "__main__":
    print(json.dumps(main(sys.argv[1:] or STAGES), indent=1, default=str))
