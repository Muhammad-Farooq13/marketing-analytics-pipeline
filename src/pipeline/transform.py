"""
Transformation layer: staging -> intermediate (cleaned) -> marts.

Mirrors a dbt-style layered structure even though these are plain SQL
statements executed via DuckDB rather than actual dbt models:
  - staging (stg_*): raw, as-loaded (see src/pipeline/staging.py)
  - intermediate (int_*): deduplicated and cleaned, with orphan references
    and invalid values explicitly handled (not silently dropped -- every
    cleaning decision below is logged with a row count so the transform is
    auditable)
  - marts (mart_*): business-ready aggregated tables a BI tool or analyst
    would query directly

Usage:
    python -m src.pipeline.transform --db-path data/warehouse/marketing.duckdb
"""

from __future__ import annotations

import argparse
import logging

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)


def build_intermediate_layer(con: duckdb.DuckDBPyConnection) -> None:
    """Clean staging data: dedupe, drop invalid values, flag orphan references."""

    # --- int_ad_spend: dedupe (keep one row per campaign_id+date), drop negative spend ---
    before = con.execute("SELECT COUNT(*) FROM stg_ad_spend").fetchone()[0]
    con.execute("""
        CREATE OR REPLACE TABLE int_ad_spend AS
        SELECT DISTINCT ON (campaign_id, date) *
        FROM stg_ad_spend
        WHERE spend >= 0
        ORDER BY campaign_id, date
        """)
    after = con.execute("SELECT COUNT(*) FROM int_ad_spend").fetchone()[0]
    logger.info(
        "int_ad_spend: %d -> %d rows (removed %d: duplicates deduped + negative-spend rows dropped)",
        before,
        after,
        before - after,
    )

    # --- int_web_events: dedupe by event_id, null out orphan campaign references
    #     rather than dropping the row entirely -- the page view / signup / purchase
    #     still happened and is real traffic; only the campaign attribution is
    #     unreliable, so it becomes "unattributed" rather than being discarded. ---
    before = con.execute("SELECT COUNT(*) FROM stg_web_events").fetchone()[0]
    con.execute("""
        CREATE OR REPLACE TABLE int_web_events AS
        SELECT
            event_id,
            timestamp,
            user_id,
            event_type,
            utm_channel,
            CASE
                WHEN utm_campaign_id IS NOT NULL
                     AND utm_campaign_id NOT IN (SELECT DISTINCT campaign_id FROM int_ad_spend)
                THEN NULL
                ELSE utm_campaign_id
            END AS utm_campaign_id,
            revenue
        FROM (
            SELECT DISTINCT ON (event_id) *
            FROM stg_web_events
            ORDER BY event_id
        )
        """)
    after = con.execute("SELECT COUNT(*) FROM int_web_events").fetchone()[0]
    n_orphans_nulled = con.execute("""
        SELECT COUNT(*) FROM int_web_events w
        WHERE w.utm_campaign_id IS NULL AND w.utm_channel IN
            (SELECT DISTINCT channel FROM int_ad_spend)
        """).fetchone()[0]
    logger.info(
        "int_web_events: %d -> %d rows (removed %d duplicate event_ids); "
        "%d orphan campaign references nulled out (kept as unattributed traffic, not dropped)",
        before,
        after,
        before - after,
        n_orphans_nulled,
    )

    # --- int_crm_sales: dedupe by deal_id, keep rows with null close_date
    #     (still-open deals) but flag them rather than dropping ---
    before = con.execute("SELECT COUNT(*) FROM stg_crm_sales").fetchone()[0]
    con.execute("""
        CREATE OR REPLACE TABLE int_crm_sales AS
        SELECT DISTINCT ON (deal_id) *,
            (close_date IS NULL) AS is_open_deal
        FROM stg_crm_sales
        ORDER BY deal_id
        """)
    after = con.execute("SELECT COUNT(*) FROM int_crm_sales").fetchone()[0]
    logger.info(
        "int_crm_sales: %d -> %d rows (removed %d duplicate deal_ids); open deals flagged, not dropped",
        before,
        after,
        before - after,
    )


def build_mart_layer(con: duckdb.DuckDBPyConnection) -> None:
    """Build business-ready aggregate tables."""

    # --- mart_channel_performance_daily: spend, clicks, conversions, revenue, ROAS by channel/day ---
    con.execute("""
        CREATE OR REPLACE TABLE mart_channel_performance_daily AS
        WITH spend AS (
            SELECT date, channel, SUM(spend) AS spend, SUM(impressions) AS impressions,
                   SUM(clicks) AS clicks
            FROM int_ad_spend
            GROUP BY date, channel
        ),
        events AS (
            SELECT
                CAST(timestamp AS DATE) AS date,
                utm_channel AS channel,
                COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
                COUNT(*) FILTER (WHERE event_type = 'signup') AS signups,
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                COALESCE(SUM(revenue) FILTER (WHERE event_type = 'purchase'), 0) AS revenue
            FROM int_web_events
            GROUP BY CAST(timestamp AS DATE), utm_channel
        )
        SELECT
            COALESCE(s.date, e.date) AS date,
            COALESCE(s.channel, e.channel) AS channel,
            COALESCE(s.spend, 0) AS spend,
            COALESCE(s.impressions, 0) AS impressions,
            COALESCE(s.clicks, 0) AS clicks,
            COALESCE(e.page_views, 0) AS page_views,
            COALESCE(e.signups, 0) AS signups,
            COALESCE(e.purchases, 0) AS purchases,
            COALESCE(e.revenue, 0) AS revenue,
            CASE WHEN COALESCE(s.spend, 0) > 0
                 THEN ROUND(COALESCE(e.revenue, 0) / s.spend, 3)
                 ELSE NULL END AS roas
        FROM spend s
        FULL OUTER JOIN events e ON s.date = e.date AND s.channel = e.channel
        ORDER BY date, channel
        """)
    n_rows = con.execute("SELECT COUNT(*) FROM mart_channel_performance_daily").fetchone()[0]
    logger.info("mart_channel_performance_daily: %d rows", n_rows)

    # --- mart_funnel_conversion: page_view -> signup -> purchase conversion by channel ---
    con.execute("""
        CREATE OR REPLACE TABLE mart_funnel_conversion AS
        SELECT
            utm_channel AS channel,
            COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
            COUNT(*) FILTER (WHERE event_type = 'signup') AS signups,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
            ROUND(
                COUNT(*) FILTER (WHERE event_type = 'signup')::DOUBLE /
                NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0), 4
            ) AS page_view_to_signup_rate,
            ROUND(
                COUNT(*) FILTER (WHERE event_type = 'purchase')::DOUBLE /
                NULLIF(COUNT(*) FILTER (WHERE event_type = 'signup'), 0), 4
            ) AS signup_to_purchase_rate
        FROM int_web_events
        GROUP BY utm_channel
        ORDER BY channel
        """)
    n_rows = con.execute("SELECT COUNT(*) FROM mart_funnel_conversion").fetchone()[0]
    logger.info("mart_funnel_conversion: %d rows", n_rows)

    # --- mart_customer_acquisition_cost: total spend / new customers, by channel ---
    con.execute("""
        CREATE OR REPLACE TABLE mart_customer_acquisition_cost AS
        WITH channel_spend AS (
            SELECT channel, SUM(spend) AS total_spend
            FROM int_ad_spend
            GROUP BY channel
        ),
        new_customers AS (
            SELECT w.utm_channel AS channel, COUNT(DISTINCT c.user_id) AS n_customers,
                   SUM(c.deal_value) AS total_revenue
            FROM int_crm_sales c
            JOIN int_web_events w ON c.user_id = w.user_id AND w.event_type = 'purchase'
            WHERE c.is_open_deal = FALSE
            GROUP BY w.utm_channel
        )
        SELECT
            s.channel,
            s.total_spend,
            COALESCE(n.n_customers, 0) AS new_customers,
            COALESCE(n.total_revenue, 0) AS total_revenue,
            CASE WHEN COALESCE(n.n_customers, 0) > 0
                 THEN ROUND(s.total_spend / n.n_customers, 2)
                 ELSE NULL END AS cac
        FROM channel_spend s
        LEFT JOIN new_customers n ON s.channel = n.channel
        ORDER BY s.channel
        """)
    n_rows = con.execute("SELECT COUNT(*) FROM mart_customer_acquisition_cost").fetchone()[0]
    logger.info("mart_customer_acquisition_cost: %d rows", n_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run transformation layer (staging -> intermediate -> marts)"
    )
    parser.add_argument("--db-path", type=str, default="data/warehouse/marketing.duckdb")
    args = parser.parse_args()

    con = duckdb.connect(args.db_path)
    build_intermediate_layer(con)
    build_mart_layer(con)
    con.close()
    logger.info("Transformation complete.")


if __name__ == "__main__":
    main()
