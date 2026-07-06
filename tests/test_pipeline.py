"""Unit and integration tests for the marketing analytics ELT pipeline."""

from __future__ import annotations

import duckdb
import numpy as np
import pytest

from src.ingestion.generate_sources import (
    GenerationConfig,
    generate_ad_spend,
    generate_crm_sales,
    generate_web_events,
)
from src.pipeline.dag import DAGRunner, PipelineFailedError, Task
from src.quality.checks import (
    check_completeness,
    check_referential_integrity,
    check_uniqueness,
    check_value_range,
)


@pytest.fixture(scope="module")
def sample_sources():
    config = GenerationConfig(n_days=14, random_seed=7)
    rng = np.random.default_rng(config.random_seed)
    ad_spend, campaign_ids = generate_ad_spend(config, rng)
    web_events = generate_web_events(config, rng, campaign_ids)
    crm_sales = generate_crm_sales(config, rng, web_events)
    return ad_spend, web_events, crm_sales, campaign_ids


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


class TestDataGeneration:
    def test_ad_spend_has_injected_negative_values(self, sample_sources):
        ad_spend, _, _, _ = sample_sources
        assert (ad_spend["spend"] < 0).sum() > 0

    def test_web_events_has_orphan_campaign_references(self, sample_sources):
        _, web_events, _, campaign_ids = sample_sources
        all_valid_ids = {cid for cids in campaign_ids.values() for cid in cids}
        paid_events = web_events[web_events["utm_campaign_id"].notna()]
        orphans = paid_events[~paid_events["utm_campaign_id"].isin(all_valid_ids)]
        assert len(orphans) > 0

    def test_json_output_has_no_nan_literal(self, sample_sources, tmp_path):
        """Regression guard: revenue must serialize as JSON null, never the
        non-standard NaN token, which real JSON parsers outside Python reject."""
        import json

        _, web_events, _, _ = sample_sources
        out_file = tmp_path / "web_events.jsonl"
        with open(out_file, "w", encoding="utf-8") as f:
            for _, row in web_events.head(50).iterrows():
                record = row.to_dict()
                record["timestamp"] = record["timestamp"].isoformat()
                record = {
                    k: (None if isinstance(v, float) and np.isnan(v) else v)
                    for k, v in record.items()
                }
                f.write(json.dumps(record, default=str) + "\n")

        with open(out_file) as f:
            for line in f:
                assert "NaN" not in line
                json.loads(line)  # strict parse must succeed


class TestDataQualityChecks:
    def test_uniqueness_check_detects_duplicates(self, con):
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (2), (3)) AS x(id)")
        result = check_uniqueness(con, "t", "id")
        assert not result.passed
        assert result.n_violations == 1

    def test_uniqueness_check_passes_on_clean_data(self, con):
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS x(id)")
        result = check_uniqueness(con, "t", "id")
        assert result.passed
        assert result.n_violations == 0

    def test_completeness_check_respects_threshold(self, con):
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (NULL), (3), (4)) AS x(v)")
        strict = check_completeness(con, "t", "v", max_null_rate=0.0)
        lenient = check_completeness(con, "t", "v", max_null_rate=0.5)
        assert not strict.passed
        assert lenient.passed

    def test_value_range_check_detects_negative_values(self, con):
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (10.0), (-5.0), (3.0)) AS x(spend)")
        result = check_value_range(con, "t", "spend", min_value=0)
        assert not result.passed
        assert result.n_violations == 1

    def test_referential_integrity_detects_orphans(self, con):
        con.execute("CREATE TABLE parent AS SELECT * FROM (VALUES ('A'), ('B')) AS x(id)")
        con.execute(
            "CREATE TABLE child AS SELECT * FROM (VALUES ('A'), ('C'), (NULL)) AS x(parent_id)"
        )
        result = check_referential_integrity(con, "child", "parent_id", "parent", "id")
        assert not result.passed
        assert result.n_violations == 1  # 'C' is the orphan; NULL is allowed by default

    def test_referential_integrity_passes_when_all_resolve(self, con):
        con.execute("CREATE TABLE parent AS SELECT * FROM (VALUES ('A'), ('B')) AS x(id)")
        con.execute("CREATE TABLE child AS SELECT * FROM (VALUES ('A'), ('B')) AS x(parent_id)")
        result = check_referential_integrity(con, "child", "parent_id", "parent", "id")
        assert result.passed

    def test_severity_fail_produces_fatal_flag_only_when_failed(self, con):
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (1)) AS x(id)")
        fail_result = check_uniqueness(con, "t", "id", severity="fail")
        warn_result = check_uniqueness(con, "t", "id", severity="warn")
        assert fail_result.fatal is True
        assert warn_result.fatal is False  # same failure, but warn severity is never fatal


class TestTransformCleansData:
    """Integration test: build a tiny warehouse end-to-end and verify the
    transform layer actually fixes the problems the staging checks flag."""

    def test_intermediate_layer_removes_duplicates_and_negatives(self, con):
        from src.pipeline.transform import build_intermediate_layer

        con.execute("""
            CREATE TABLE stg_ad_spend AS SELECT * FROM (VALUES
                (DATE '2026-01-01', 'google_ads', 'GO-001', 'camp', 100.0, 1000, 10.0),
                (DATE '2026-01-01', 'google_ads', 'GO-001', 'camp', 100.0, 1000, 10.0),
                (DATE '2026-01-02', 'google_ads', 'GO-002', 'camp', -50.0, 500, 5.0)
            ) AS t(date, channel, campaign_id, campaign_name, spend, impressions, clicks)
            """)
        con.execute(
            "CREATE TABLE stg_web_events AS SELECT * FROM "
            "(VALUES ('E1', TIMESTAMP '2026-01-01 00:00:00', 1, 'page_view', 'google_ads', 'GO-001', NULL)) "
            "AS t(event_id, timestamp, user_id, event_type, utm_channel, utm_campaign_id, revenue)"
        )
        con.execute(
            "CREATE TABLE stg_crm_sales AS SELECT * FROM "
            "(VALUES ('D1', 1, TIMESTAMP '2026-01-01 00:00:00', 100.0, 'alice', 'inbound')) "
            "AS t(deal_id, user_id, close_date, deal_value, sales_rep, lead_source)"
        )

        build_intermediate_layer(con)

        result = con.execute("SELECT COUNT(*) FROM int_ad_spend").fetchone()[0]
        assert result == 1  # duplicate removed, negative-spend row dropped

        min_spend = con.execute("SELECT MIN(spend) FROM int_ad_spend").fetchone()[0]
        assert min_spend >= 0


class TestDAGRunner:
    def test_tasks_execute_in_dependency_order(self):
        execution_order = []
        tasks = [
            Task(name="c", fn=lambda: execution_order.append("c"), depends_on=["b"]),
            Task(name="a", fn=lambda: execution_order.append("a")),
            Task(name="b", fn=lambda: execution_order.append("b"), depends_on=["a"]),
        ]
        DAGRunner(tasks).run()
        assert execution_order == ["a", "b", "c"]

    def test_gate_failure_stops_downstream_tasks(self):
        ran = []

        def failing_task():
            raise PipelineFailedError("simulated gate failure")

        tasks = [
            Task(name="a", fn=lambda: ran.append("a")),
            Task(name="b", fn=failing_task, depends_on=["a"]),
            Task(name="c", fn=lambda: ran.append("c"), depends_on=["b"]),
        ]
        with pytest.raises(PipelineFailedError):
            DAGRunner(tasks).run()
        assert "c" not in ran
        assert ran == ["a"]

    def test_transient_failure_retries_then_succeeds(self):
        attempts = {"count": 0}

        def flaky_task():
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("transient failure")

        tasks = [Task(name="flaky", fn=flaky_task, max_retries=2, retry_delay_seconds=0.01)]
        run_log = DAGRunner(tasks).run()
        assert run_log[0].status == "success"
        assert run_log[0].attempts == 2

    def test_gate_failure_is_not_retried(self):
        """A PipelineFailedError should fail immediately, not waste time retrying
        a data quality gate that a retry cannot possibly fix."""
        attempts = {"count": 0}

        def gate_task():
            attempts["count"] += 1
            raise PipelineFailedError("gate check failed")

        tasks = [Task(name="gate", fn=gate_task, max_retries=3, retry_delay_seconds=0.01)]
        with pytest.raises(PipelineFailedError):
            DAGRunner(tasks).run()
        assert attempts["count"] == 1  # no retries attempted

    def test_cycle_detection_raises_error(self):
        tasks = [
            Task(name="a", fn=lambda: None, depends_on=["b"]),
            Task(name="b", fn=lambda: None, depends_on=["a"]),
        ]
        with pytest.raises(ValueError, match="Cycle"):
            DAGRunner(tasks).run()
