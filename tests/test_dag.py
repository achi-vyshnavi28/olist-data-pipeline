"""The Airflow DAG imports cleanly and wires the stages in the right order (runs in CI, where Airflow is installed)."""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")


def test_dag_loads_with_the_expected_task_order(monkeypatch):
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"))  # examples off via AIRFLOW__CORE__LOAD_EXAMPLES
    assert bag.import_errors == {}
    dag = bag.get_dag("olist_batch_pipeline")
    assert dag is not None and dag.max_active_runs == 1
    down = lambda t: sorted(dag.get_task(t).downstream_task_ids)
    assert down("bronze") == ["silver"]
    assert down("silver") == ["quality_silver"]
    assert down("quality_silver") == ["gold"]
    assert down("gold") == ["quality_gold"]
    assert down("quality_gold") == ["catalog", "warehouse"]
    assert dag.get_task("bronze").retries == 2
