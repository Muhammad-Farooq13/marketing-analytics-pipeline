"""
Synthetic multi-source marketing data generator.

NOTE ON DATA PROVENANCE
------------------------
This build environment cannot reach real marketing-platform exports (Google
Ads, Meta Ads Manager, Salesforce/HubSpot CRM exports), so this module
generates three synthetic "source systems" instead: ad spend (CSV), web
analytics events (JSON Lines), and CRM sales records (CSV) -- deliberately
in three different formats, as real multi-source ingestion actually looks.

Realistic DATA QUALITY PROBLEMS are injected on purpose, not as bugs but as
the actual point of the exercise: real marketing data has duplicate rows
from double-loaded exports, orphaned campaign references (a web event
tagged with a UTM campaign ID that was never in the ad spend export, often
because someone changed a URL parameter by hand), and negative/null values
from manual data entry. A pipeline that only ever sees clean data doesn't
demonstrate anything about data quality engineering -- so this generator
injects the problems, and the rest of the pipeline is built to catch and
handle them, which is documented explicitly in the README.

Usage:
    python -m src.ingestion.generate_sources --n-days 60
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

CHANNELS = [
    "google_ads",
    "facebook_ads",
    "tiktok_ads",
    "linkedin_ads",
    "organic",
    "direct",
]
PAID_CHANNELS = ["google_ads", "facebook_ads", "tiktok_ads", "linkedin_ads"]


@dataclass(frozen=True)
class GenerationConfig:
    n_days: int = 60
    n_campaigns_per_channel: int = 4
    random_seed: int = 42


def generate_ad_spend(config: GenerationConfig, rng: np.random.Generator) -> pd.DataFrame:
    """Generate daily ad spend by channel/campaign, with injected data quality issues."""
    rows = []
    campaign_ids = {}
    for channel in PAID_CHANNELS:
        campaign_ids[channel] = [
            f"{channel[:2].upper()}-{i:03d}" for i in range(1, config.n_campaigns_per_channel + 1)
        ]

    base_date = pd.Timestamp("2026-01-01")
    channel_daily_budget = {
        "google_ads": 800,
        "facebook_ads": 600,
        "tiktok_ads": 400,
        "linkedin_ads": 300,
    }

    for day in range(config.n_days):
        date = base_date + pd.Timedelta(days=day)
        for channel in PAID_CHANNELS:
            for campaign_id in campaign_ids[channel]:
                spend = max(
                    0,
                    rng.normal(
                        channel_daily_budget[channel] / config.n_campaigns_per_channel,
                        15,
                    ),
                )
                impressions = int(spend * rng.uniform(80, 150))
                clicks = int(impressions * rng.uniform(0.01, 0.04))
                rows.append(
                    {
                        "date": date,
                        "channel": channel,
                        "campaign_id": campaign_id,
                        "campaign_name": f"{channel}_{campaign_id}_campaign",
                        "spend": round(float(spend), 2),
                        "impressions": impressions,
                        "clicks": clicks,
                    }
                )

    df = pd.DataFrame(rows)

    # --- Inject realistic data quality issues ---
    n = len(df)
    # 1. Duplicate rows (simulates a double-loaded daily export file)
    dup_idx = rng.choice(n, size=int(n * 0.02), replace=False)
    df = pd.concat([df, df.iloc[dup_idx]], ignore_index=True)
    # 2. A few negative spend values (manual correction/refund entered wrong)
    neg_idx = rng.choice(len(df), size=5, replace=False)
    df.loc[neg_idx, "spend"] = -abs(df.loc[neg_idx, "spend"])
    # 3. A few nulls in clicks (tracking pixel failure)
    null_idx = rng.choice(len(df), size=8, replace=False)
    df.loc[null_idx, "clicks"] = np.nan

    logger.info(
        "Generated ad_spend: %d rows (%d channels x %d campaigns x %d days), with injected "
        "duplicates/negatives/nulls for data-quality testing",
        len(df),
        len(PAID_CHANNELS),
        config.n_campaigns_per_channel,
        config.n_days,
    )
    return df, campaign_ids


def generate_web_events(
    config: GenerationConfig, rng: np.random.Generator, campaign_ids: dict
) -> pd.DataFrame:
    """Generate web events (page_view, signup, purchase) with UTM attribution."""
    base_date = pd.Timestamp("2026-01-01")

    rows = []
    event_counter = 0
    n_users = 3000

    for day in range(config.n_days):
        date = base_date + pd.Timedelta(days=day)
        n_events_today = rng.integers(150, 400)
        for _ in range(n_events_today):
            user_id = int(rng.integers(0, n_users))
            channel = rng.choice(CHANNELS, p=[0.25, 0.20, 0.10, 0.08, 0.22, 0.15])
            is_paid = channel in PAID_CHANNELS
            utm_campaign_id = (
                rng.choice(campaign_ids[channel])
                if is_paid and rng.random() > 0.05
                # 5% of paid-channel events reference a campaign ID that doesn't
                # exist in ad_spend -- e.g. someone hand-edited a URL parameter
                else (f"{channel[:2].upper()}-999" if is_paid else None)
            )
            event_type = rng.choice(["page_view", "signup", "purchase"], p=[0.85, 0.10, 0.05])
            revenue = round(float(rng.uniform(20, 300)), 2) if event_type == "purchase" else None
            timestamp = date + pd.Timedelta(seconds=int(rng.integers(0, 86400)))

            rows.append(
                {
                    "event_id": f"EVT{event_counter:07d}",
                    "timestamp": timestamp,
                    "user_id": user_id,
                    "event_type": event_type,
                    "utm_channel": channel,
                    "utm_campaign_id": utm_campaign_id,
                    "revenue": revenue,
                }
            )
            event_counter += 1

    df = pd.DataFrame(rows)

    # --- Inject data quality issues ---
    # Duplicate event_ids (simulates an at-least-once delivery event stream)
    dup_idx = rng.choice(len(df), size=int(len(df) * 0.01), replace=False)
    dup_rows = df.iloc[dup_idx].copy()
    df = pd.concat([df, dup_rows], ignore_index=True)

    orphan_count = int((df["utm_campaign_id"].astype(str).str.endswith("-999")).sum())
    logger.info(
        "Generated web_events: %d rows, including %d orphan campaign references "
        "(utm_campaign_id not present in ad_spend) and %d duplicate event_ids for "
        "data-quality testing",
        len(df),
        orphan_count,
        len(dup_idx),
    )
    return df


def generate_crm_sales(
    config: GenerationConfig, rng: np.random.Generator, web_events: pd.DataFrame
) -> pd.DataFrame:
    """Generate CRM sales records for a subset of users who made purchases."""
    purchasers = web_events[web_events.event_type == "purchase"]["user_id"].unique()
    rows = []
    for i, user_id in enumerate(purchasers):
        user_events = web_events[
            (web_events.user_id == user_id) & (web_events.event_type == "purchase")
        ]
        close_date = user_events["timestamp"].max()
        deal_value = float(user_events["revenue"].sum())
        rows.append(
            {
                "deal_id": f"DEAL{i:06d}",
                "user_id": int(user_id),
                "close_date": close_date,
                "deal_value": round(deal_value, 2),
                "sales_rep": rng.choice(["alice", "bob", "carol", "dave"]),
                "lead_source": rng.choice(["inbound", "outbound", "referral"]),
            }
        )
    df = pd.DataFrame(rows)

    # --- Inject data quality issues ---
    # A few duplicate deal_ids (CRM sync ran twice)
    if len(df) > 5:
        dup_idx = rng.choice(len(df), size=min(3, len(df)), replace=False)
        df = pd.concat([df, df.iloc[dup_idx]], ignore_index=True)
    # A few deals with missing close_date (still open, exported prematurely)
    null_idx = rng.choice(len(df), size=min(4, len(df)), replace=False)
    df.loc[null_idx, "close_date"] = pd.NaT

    logger.info("Generated crm_sales: %d rows, with injected duplicates/nulls", len(df))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic multi-source marketing data")
    parser.add_argument("--n-days", type=int, default=60)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default="data/raw")
    args = parser.parse_args()

    config = GenerationConfig(n_days=args.n_days, random_seed=args.random_seed)
    rng = np.random.default_rng(config.random_seed)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ad_spend, campaign_ids = generate_ad_spend(config, rng)
    ad_spend.to_csv(output_dir / "ad_spend.csv", index=False)

    web_events = generate_web_events(config, rng, campaign_ids)
    with open(output_dir / "web_events.jsonl", "w", encoding="utf-8") as f:
        for _, row in web_events.iterrows():
            record = row.to_dict()
            record["timestamp"] = record["timestamp"].isoformat()
            # pandas NaN for a missing numeric (e.g. revenue on a non-purchase event)
            # must become JSON `null`, not the literal NaN token -- Python's json
            # module will happily WRITE non-standard NaN by default, but real JSON
            # consumers (BigQuery, most JSON parsers outside Python) reject it.
            record = {
                k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in record.items()
            }
            f.write(json.dumps(record, default=str) + "\n")

    crm_sales = generate_crm_sales(config, rng, web_events)
    crm_sales.to_csv(output_dir / "crm_sales.csv", index=False)

    logger.info("Saved all 3 source files to %s", output_dir)


if __name__ == "__main__":
    main()
