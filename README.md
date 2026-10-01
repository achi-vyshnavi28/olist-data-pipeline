# Olist Data Pipeline

**Batch and streaming data pipelines with Apache Spark on real e-commerce data, orchestrated with Airflow, gated by
data quality checks, governed with contracts and a catalog, and loaded into a star schema warehouse.**

Data: the real [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(99,441 orders, 112,650 order lines, 8 source tables, CC BY-NC-SA 4.0). `data/sample/` holds a consistent slice of
2,000 orders and every row that belongs to them, for tests and CI.

## Architecture

```
raw CSVs ─► bronze (parquet, contract schemas, ingestion metadata)
          ─► silver (typed, cleaned, deduplicated; bad rows quarantined with a reason)
          ─► quality gate (13 checks: not null, unique keys, foreign keys, accepted values, ranges)
          ─► gold star schema (4 dimensions, 2 facts, 1 daily KPI mart, surrogate keys)
          ─► quality gate (9 checks on grain, keys and joins)
          ─► catalog (owners, PII columns, lineage)  +  warehouse (DuckDB, analyst views with PII masked)

order events ─► Spark Structured Streaming ─► 2 hour watermark ─► hourly tumbling windows (orders, revenue, cancels)
```

| Piece | Where |
|---|---|
| Spark batch stages | `pipeline/bronze.py`, `silver.py`, `gold.py` |
| Spark Structured Streaming | `pipeline/streaming.py` (real orders replayed as event files) |
| Data quality gates | `pipeline/quality.py`, reports in `lake/reports/quality_*.json` |
| Governance | `pipeline/contracts.py` (schemas, keys, owners, PII), `pipeline/catalog.py` (catalog + lineage) |
| Star schema and marts | `pipeline/gold.py` |
| Warehouse | `pipeline/warehouse.py` (DuckDB; `analytics.v_customers_masked` hides zip codes) |
| Orchestration | `dags/olist_pipeline.py` (Airflow: daily, retries, quality gate stops the run) |
| CI/CD | `.github/workflows/ci.yml` (GitHub Actions): Spark tests incl. streaming, a full Airflow DAG run, Docker build and run; `Jenkinsfile`: the same test, pipeline, quality gate and Docker stages for Jenkins |
| Container | `Dockerfile` (Python 3.12 + OpenJDK 17) |

## Results on the full dataset

Full run, local Spark 4.2 (8 cores): about 3 minutes end to end.

| Stage | Output |
|---|---|
| Bronze | 99,441 orders, 112,650 items, 103,886 payments, 99,224 reviews, 99,441 customers, 3,095 sellers, 32,951 products |
| Silver | 0 rows quarantined on the real data (rules proven on bad rows in the unit tests) |
| Silver gate | 13 checks, 0 errors, 1 warning: 43 orders took more than 120 days to deliver |
| Gold | `dim_customer` 96,096 (unique customers), `dim_seller` 3,095, `dim_product` 32,951, `dim_date` 800, `fact_orders` 99,441, `fact_order_items` 112,650, `mart_daily_kpis` 634 days |
| Gold gate | 9 checks, 0 errors |
| Warehouse | late deliveries 8.1% of delivered orders; top category by revenue: health_beauty |

## Run

```bash
pip install -r requirements.txt          # needs Java 17 for Spark
python -m pipeline.run                    # all stages on data/sample
PIPELINE_RAW=/path/to/olist python -m pipeline.run
pytest -q                                 # 13 tests
```

## Honest notes

- On Windows without Hadoop's native helpers (`winutils.exe`), Spark cannot read or write parquet on the local disk,
  so local runs move tables through Arrow (`pipeline/spark.py`); all transformations still run in Spark. On Linux
  (CI, Docker) every read and write is native Spark, and the streaming and Airflow tests run there.
- The warehouse here is DuckDB. The same star schema pattern loaded into Snowflake and BigQuery lives in my other
  repos (`whatsapp-order-ops-analytics`, `Business-health-dashboard`).
