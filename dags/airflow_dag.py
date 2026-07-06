"""
Production Airflow DAG definition for the marketing analytics ELT pipeline.

IMPORTANT -- READ BEFORE ASSUMING THIS RUNS:
This file is a DEPLOYMENT REFERENCE, not something executed as part of this
project's verified pipeline. Airflow requires its own scheduler, webserver,
and metadata database processes running continuously, which is out of scope
for this sandbox (no persistent background services, no port-exposed
webserver). The ACTUAL, VERIFIED pipeline in this repository is the custom
DAGRunner in src/pipeline/dag.py, which was run end-to-end in this
environment with a passing result -- see the README's "What's verified vs.
what's scaffolded" section.

This file exists because in a real production deployment, you would not
hand-roll a DAG runner -- you'd use Airflow (or Dagster/Prefect). It's
included as a syntactically valid, structurally faithful translation of the
exact same DAG (same 5 tasks, same dependencies, same DQ gate) so a platform
engineer reviewing this repo can see the intended production shape, not just
the sandbox-friendly stand-in.

To actually run this: `pip install apache-airflow`, place this file in your
Airflow DAGs folder, and ensure src/ is on the Airflow worker's PYTHONPATH.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _generate_sources():
    import numpy as np

    from src.ingestion.generate_sources import (
        GenerationConfig,
        generate_ad_spend,
        generate_crm_sales,
        generate_web_events,
    )

    config = GenerationConfig()
    rng = np.random.default_rng(config.random_seed)
    ad_spend, campaign_ids = generate_ad_spend(config, rng)
    ad_spend.to_csv("data/raw/ad_spend.csv", index=False)
    web_events = generate_web_events(config, rng, campaign_ids)
    web_events.to_json("data/raw/web_events.jsonl", orient="records", lines=True)
    crm_sales = generate_crm_sales(config, rng, web_events)
    crm_sales.to_csv("data/raw/crm_sales.csv", index=False)


def _load_staging():
    import duckdb

    from src.pipeline.staging import load_staging_tables

    con = duckdb.connect("data/warehouse/marketing.duckdb")
    load_staging_tables(con, __import__("pathlib").Path("data/raw"))
    con.close()


def _staging_quality_checks():
    import duckdb

    from src.quality.checks import run_staging_checks, summarize

    con = duckdb.connect("data/warehouse/marketing.duckdb")
    summary = summarize(run_staging_checks(con))
    con.close()
    print(f"Staging DQ (informational): {summary['passed']}/{summary['total_checks']} passed")


def _transform():
    import duckdb

    from src.pipeline.transform import build_intermediate_layer, build_mart_layer

    con = duckdb.connect("data/warehouse/marketing.duckdb")
    build_intermediate_layer(con)
    build_mart_layer(con)
    con.close()


def _mart_quality_checks():
    import duckdb

    from src.quality.checks import run_mart_checks, summarize

    con = duckdb.connect("data/warehouse/marketing.duckdb")
    summary = summarize(run_mart_checks(con))
    con.close()
    if summary["fatal_failures"] > 0:
        raise ValueError(
            f"{summary['fatal_failures']} fatal mart-level DQ check(s) failed -- stopping DAG"
        )
    print(f"Mart DQ gate passed: {summary['passed']}/{summary['total_checks']}")


with DAG(
    dag_id="marketing_analytics_elt",
    default_args=default_args,
    description="Multi-source marketing ELT: ad spend + web events + CRM -> BI-ready marts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "marketing-analytics", "duckdb"],
) as dag:
    generate_sources = PythonOperator(task_id="generate_sources", python_callable=_generate_sources)
    load_staging = PythonOperator(task_id="load_staging", python_callable=_load_staging)
    staging_quality_checks = PythonOperator(
        task_id="staging_quality_checks", python_callable=_staging_quality_checks
    )
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    mart_quality_checks = PythonOperator(
        task_id="mart_quality_checks", python_callable=_mart_quality_checks
    )

    (generate_sources >> load_staging >> staging_quality_checks >> transform >> mart_quality_checks)
