"""
Lightweight data quality check framework.

Mirrors the core check types found in tools like Great Expectations or dbt
tests, implemented directly against DuckDB so the whole pipeline has no
heavyweight external dependency:
  - completeness (null rate below a threshold)
  - uniqueness (no duplicate primary keys)
  - referential integrity (foreign keys resolve to an existing parent row)
  - value range (e.g. spend must be >= 0)
  - freshness (dates fall within an expected range)

Each check returns a structured result with a SEVERITY (fail vs warn) so the
pipeline can distinguish "this must block downstream transforms" from "this
is worth surfacing but not fatal" -- collapsing everything to a single
pass/fail flag is a common and costly oversimplification in real data
quality tooling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)

Severity = Literal["fail", "warn"]


@dataclass
class CheckResult:
    check_name: str
    table: str
    passed: bool
    severity: Severity
    n_violations: int
    detail: str
    fatal: bool = field(init=False)

    def __post_init__(self):
        self.fatal = (not self.passed) and self.severity == "fail"


def check_uniqueness(
    con: duckdb.DuckDBPyConnection,
    table: str,
    key_col: str,
    severity: Severity = "fail",
) -> CheckResult:
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    distinct = con.execute(f"SELECT COUNT(DISTINCT {key_col}) FROM {table}").fetchone()[0]
    n_dupes = total - distinct
    passed = n_dupes == 0
    return CheckResult(
        check_name=f"uniqueness({key_col})",
        table=table,
        passed=passed,
        severity=severity,
        n_violations=n_dupes,
        detail=(
            f"{n_dupes} duplicate {key_col} value(s) out of {total} rows"
            if not passed
            else "no duplicates"
        ),
    )


def check_completeness(
    con: duckdb.DuckDBPyConnection,
    table: str,
    col: str,
    max_null_rate: float = 0.0,
    severity: Severity = "warn",
) -> CheckResult:
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    n_null = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
    null_rate = n_null / total if total else 0.0
    passed = null_rate <= max_null_rate
    return CheckResult(
        check_name=f"completeness({col})",
        table=table,
        passed=passed,
        severity=severity,
        n_violations=n_null,
        detail=f"{n_null}/{total} nulls ({null_rate:.1%}), threshold {max_null_rate:.1%}",
    )


def check_value_range(
    con: duckdb.DuckDBPyConnection,
    table: str,
    col: str,
    min_value: float | None = None,
    max_value: float | None = None,
    severity: Severity = "fail",
) -> CheckResult:
    conditions = []
    if min_value is not None:
        conditions.append(f"{col} < {min_value}")
    if max_value is not None:
        conditions.append(f"{col} > {max_value}")
    where_clause = " OR ".join(conditions) if conditions else "FALSE"
    n_violations = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_clause}").fetchone()[0]
    passed = n_violations == 0
    return CheckResult(
        check_name=f"value_range({col})",
        table=table,
        passed=passed,
        severity=severity,
        n_violations=n_violations,
        detail=(
            f"{n_violations} row(s) outside [{min_value}, {max_value}]"
            if not passed
            else "all values in range"
        ),
    )


def check_referential_integrity(
    con: duckdb.DuckDBPyConnection,
    child_table: str,
    child_col: str,
    parent_table: str,
    parent_col: str,
    severity: Severity = "warn",
    allow_null: bool = True,
) -> CheckResult:
    null_clause = f"{child_col} IS NOT NULL AND" if allow_null else ""
    query = f"""
        SELECT COUNT(*) FROM {child_table}
        WHERE {null_clause} {child_col} NOT IN (SELECT DISTINCT {parent_col} FROM {parent_table})
    """
    n_orphans = con.execute(query).fetchone()[0]
    passed = n_orphans == 0
    return CheckResult(
        check_name=f"referential_integrity({child_table}.{child_col} -> {parent_table}.{parent_col})",
        table=child_table,
        passed=passed,
        severity=severity,
        n_violations=n_orphans,
        detail=(
            f"{n_orphans} orphan reference(s) with no matching {parent_table}.{parent_col}"
            if not passed
            else "all references resolve"
        ),
    )


def run_staging_checks(con: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    """
    Checks run against RAW staging tables, before any cleaning. Every check here
    is WARN severity (informational, never blocks the pipeline) -- these issues
    are EXPECTED in raw data and are exactly what the intermediate transform
    layer is designed to fix. Gating the pipeline on a staging-level failure
    would be wrong: it would stop the pipeline before the step that fixes the
    problem ever gets a chance to run. The real gate is run_mart_checks below.
    """
    results = [
        check_uniqueness(con, "stg_ad_spend", "campaign_id || date", severity="warn"),
        check_completeness(con, "stg_ad_spend", "clicks", max_null_rate=0.02, severity="warn"),
        check_value_range(con, "stg_ad_spend", "spend", min_value=0, severity="warn"),
        check_uniqueness(con, "stg_web_events", "event_id", severity="warn"),
        check_referential_integrity(
            con,
            "stg_web_events",
            "utm_campaign_id",
            "stg_ad_spend",
            "campaign_id",
            severity="warn",
        ),
        check_uniqueness(con, "stg_crm_sales", "deal_id", severity="warn"),
        check_completeness(con, "stg_crm_sales", "close_date", max_null_rate=0.0, severity="warn"),
    ]
    return results


def run_mart_checks(con: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    """
    Checks run against the CLEANED intermediate/mart tables, after
    transformation. These are the ones that must actually pass -- if a
    mart-level check fails, the pipeline's cleaning logic has a real bug.
    """
    results = [
        check_uniqueness(con, "int_ad_spend", "campaign_id || date", severity="fail"),
        check_uniqueness(con, "int_web_events", "event_id", severity="fail"),
        check_uniqueness(con, "int_crm_sales", "deal_id", severity="fail"),
        check_referential_integrity(
            con,
            "int_web_events",
            "utm_campaign_id",
            "int_ad_spend",
            "campaign_id",
            severity="fail",
        ),
        check_value_range(con, "int_ad_spend", "spend", min_value=0, severity="fail"),
    ]
    return results


def summarize(results: list[CheckResult]) -> dict:
    return {
        "total_checks": len(results),
        "passed": sum(r.passed for r in results),
        "failed": sum(not r.passed for r in results),
        "fatal_failures": sum(r.fatal for r in results),
        "details": [
            {
                "check": r.check_name,
                "table": r.table,
                "passed": r.passed,
                "severity": r.severity,
                "n_violations": r.n_violations,
                "detail": r.detail,
            }
            for r in results
        ],
    }
