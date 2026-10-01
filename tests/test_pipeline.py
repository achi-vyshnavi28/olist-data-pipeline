"""End to end on data/sample (2,000 real Olist orders and every row that belongs to them)."""
import json

import pytest

from pipeline import catalog, quality, warehouse
from pipeline.run import STAGES, run_stage
from pipeline.spark import LAKE, read_table


@pytest.fixture(scope="module")
def ran(spark):
    return {stage: run_stage(stage, spark) for stage in STAGES}


def test_every_stage_runs_and_both_quality_gates_pass(ran):
    assert ran["bronze"]["orders"] == 2000 and ran["bronze"]["category_translation"] == 71
    assert ran["silver"]["rows"]["orders"] == 2000
    assert ran["quality_silver"]["failed_errors"] == 0 and ran["quality_gold"]["failed_errors"] == 0


def test_star_schema_grain_and_keys(spark, ran):
    facts = read_table(spark, "gold", "fact_orders")
    items = read_table(spark, "gold", "fact_order_items")
    assert facts.count() == facts.select("order_id").distinct().count() == 2000
    assert items.count() == ran["silver"]["rows"]["order_items"]
    assert ran["gold"]["dim_date"] > 0 and ran["gold"]["mart_daily_kpis"] > 0


def test_warehouse_views_answer_questions_and_mask_pii(ran):
    months = warehouse.query("SELECT orders FROM analytics.v_monthly_revenue")
    assert sum(r[0] for r in months) == 2000
    cols = [r[0] for r in warehouse.query("DESCRIBE analytics.v_customers_masked")]
    assert "customer_zip_code_prefix" not in cols and "customer_city" in cols


def test_catalog_records_lineage_owners_and_pii(ran):
    cat = json.loads((LAKE / "reports" / "catalog.json").read_text(encoding="utf-8"))
    by_name = {t["table"]: t for t in cat["tables"]}
    assert by_name["gold.fact_orders"]["upstream"][0] == "silver.orders"
    zip_col = next(c for c in by_name["silver.customers"]["columns"] if c["name"] == "customer_zip_code_prefix")
    assert zip_col["pii"] is True


def test_gold_gate_stops_the_pipeline_on_a_broken_fact(spark, ran):
    from pipeline.spark import write_table

    facts = read_table(spark, "gold", "fact_orders")
    write_table(facts.unionByName(facts.limit(1)), "gold", "fact_orders")  # duplicate one order
    with pytest.raises(quality.QualityError, match="unique:order_id on fact_orders"):
        quality.gate(spark, "gold")
    write_table(facts, "gold", "fact_orders")
