"""
Staging layer: load raw multi-format source files into DuckDB staging tables.

This is intentionally a thin, mostly-untransformed load (the "L" before the
"T" in ELT) -- staging tables should mirror the source data as closely as
possible, including its data quality problems, so those problems are
visible and checkable BEFORE any cleaning logic has a chance to silently
paper over them.

Usage:
    python -m src.pipeline.staging --raw-dir data/raw --db-path data/warehouse/marketing.duckdb
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)


def load_staging_tables(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE stg_ad_spend AS
        SELECT
            CAST(date AS DATE) AS date,
            channel,
            campaign_id,
            campaign_name,
            CAST(spend AS DOUBLE) AS spend,
            CAST(impressions AS BIGINT) AS impressions,
            CAST(clicks AS DOUBLE) AS clicks
        FROM read_csv_auto('{raw_dir / "ad_spend.csv"}', header=true)
        """)
    n_ad_spend = con.execute("SELECT COUNT(*) FROM stg_ad_spend").fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TABLE stg_web_events AS
        SELECT
            event_id,
            CAST(timestamp AS TIMESTAMP) AS timestamp,
            user_id,
            event_type,
            utm_channel,
            utm_campaign_id,
            revenue
        FROM read_json_auto('{raw_dir / "web_events.jsonl"}', format='newline_delimited')
        """)
    n_web_events = con.execute("SELECT COUNT(*) FROM stg_web_events").fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TABLE stg_crm_sales AS
        SELECT
            deal_id,
            user_id,
            CAST(close_date AS TIMESTAMP) AS close_date,
            CAST(deal_value AS DOUBLE) AS deal_value,
            sales_rep,
            lead_source
        FROM read_csv_auto('{raw_dir / "crm_sales.csv"}', header=true)
        """)
    n_crm_sales = con.execute("SELECT COUNT(*) FROM stg_crm_sales").fetchone()[0]

    logger.info(
        "Loaded staging tables: stg_ad_spend=%d rows, stg_web_events=%d rows, stg_crm_sales=%d rows",
        n_ad_spend,
        n_web_events,
        n_crm_sales,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw sources into DuckDB staging tables")
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--db-path", type=str, default="data/warehouse/marketing.duckdb")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    load_staging_tables(con, Path(args.raw_dir))
    con.close()
    logger.info("Staging complete. Warehouse: %s", db_path)


if __name__ == "__main__":
    main()
