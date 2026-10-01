"""Airflow DAG: the daily Olist batch pipeline, one task per stage, with retries and a quality gate that stops the
run before bad data reaches the warehouse.

    bronze >> silver >> quality_silver >> gold >> quality_gold >> [catalog, warehouse]
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def _stage(name: str):
    def run(**_):
        from pipeline.run import run_stage

        return run_stage(name)
    return run


default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(hours=1)}

with DAG(
    dag_id="olist_batch_pipeline",
    description="Raw Olist files to a governed star schema in the warehouse",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["olist", "spark", "batch"],
) as dag:
    tasks = {name: PythonOperator(task_id=name, python_callable=_stage(name)) for name in
             ["bronze", "silver", "quality_silver", "gold", "quality_gold", "catalog", "warehouse"]}
    (tasks["bronze"] >> tasks["silver"] >> tasks["quality_silver"] >> tasks["gold"] >> tasks["quality_gold"]
     >> [tasks["catalog"], tasks["warehouse"]])
