"""
Lightweight DAG runner for the ELT pipeline.

This is a deliberately small, custom orchestrator (not Airflow) so the whole
pipeline runs and is verified inside this sandbox with no external scheduler
or webserver process required. A structurally equivalent Airflow DAG
definition is provided separately in dags/airflow_dag.py as the production
deployment artifact -- see that file's module docstring for why it is not
executed here.

Pipeline DAG:
    generate_sources -> load_staging -> staging_quality_checks (informational)
        -> transform (builds intermediate + marts)
        -> mart_quality_checks (GATE: pipeline fails loudly if these fail)

Usage:
    python -m src.pipeline.dag
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)


class PipelineFailedError(Exception):
    """Raised when a gating check fails and the DAG must stop."""


@dataclass
class Task:
    name: str
    fn: Callable[[], None]
    depends_on: list[str] = field(default_factory=list)
    max_retries: int = 2
    retry_delay_seconds: float = 1.0


@dataclass
class TaskRun:
    task_name: str
    status: str  # "success" | "failed"
    attempts: int
    duration_seconds: float
    error: str | None = None


class DAGRunner:
    """Executes tasks in dependency order with retry logic, and records a run log."""

    def __init__(self, tasks: list[Task]):
        self.tasks = {t.name: t for t in tasks}
        self.run_log: list[TaskRun] = []

    def _topological_order(self) -> list[str]:
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str, stack: set[str]):
            if name in visited:
                return
            if name in stack:
                raise ValueError(f"Cycle detected in DAG at task '{name}'")
            stack = stack | {name}
            for dep in self.tasks[name].depends_on:
                visit(dep, stack)
            visited.add(name)
            order.append(name)

        for name in self.tasks:
            visit(name, set())
        return order

    def run(self) -> list[TaskRun]:
        order = self._topological_order()
        logger.info("DAG execution order: %s", " -> ".join(order))

        for name in order:
            task = self.tasks[name]
            attempt = 0
            start = time.perf_counter()
            while True:
                attempt += 1
                try:
                    logger.info(
                        "Running task '%s' (attempt %d/%d)",
                        name,
                        attempt,
                        task.max_retries + 1,
                    )
                    task.fn()
                    duration = time.perf_counter() - start
                    self.run_log.append(
                        TaskRun(
                            task_name=name,
                            status="success",
                            attempts=attempt,
                            duration_seconds=round(duration, 3),
                        )
                    )
                    logger.info(
                        "Task '%s' succeeded in %.2fs (attempt %d)",
                        name,
                        duration,
                        attempt,
                    )
                    break
                except PipelineFailedError:
                    # Gating failures are not retried -- retrying a failed data
                    # quality gate would just waste time re-running the same
                    # broken transform, not fix the underlying issue.
                    duration = time.perf_counter() - start
                    self.run_log.append(
                        TaskRun(
                            task_name=name,
                            status="failed",
                            attempts=attempt,
                            duration_seconds=round(duration, 3),
                            error="gate check failed",
                        )
                    )
                    logger.error("Task '%s' failed a gating check -- DAG stopped.", name)
                    raise
                except Exception as exc:  # noqa: BLE001
                    if attempt > task.max_retries:
                        duration = time.perf_counter() - start
                        self.run_log.append(
                            TaskRun(
                                task_name=name,
                                status="failed",
                                attempts=attempt,
                                duration_seconds=round(duration, 3),
                                error=str(exc),
                            )
                        )
                        logger.error("Task '%s' failed after %d attempts: %s", name, attempt, exc)
                        raise
                    logger.warning(
                        "Task '%s' failed (attempt %d/%d): %s -- retrying in %.1fs",
                        name,
                        attempt,
                        task.max_retries + 1,
                        exc,
                        task.retry_delay_seconds,
                    )
                    time.sleep(task.retry_delay_seconds)
        return self.run_log


def build_pipeline(raw_dir: str, db_path: str) -> DAGRunner:
    import duckdb

    from src.ingestion.generate_sources import (
        GenerationConfig,
        generate_ad_spend,
        generate_crm_sales,
        generate_web_events,
    )
    from src.pipeline.staging import load_staging_tables
    from src.pipeline.transform import build_intermediate_layer, build_mart_layer
    from src.quality.checks import run_mart_checks, run_staging_checks, summarize

    raw_path = Path(raw_dir)
    db_path_obj = Path(db_path)

    def task_generate_sources():
        import numpy as np

        raw_path.mkdir(parents=True, exist_ok=True)
        config = GenerationConfig()
        rng = np.random.default_rng(config.random_seed)
        ad_spend, campaign_ids = generate_ad_spend(config, rng)
        ad_spend.to_csv(raw_path / "ad_spend.csv", index=False)
        web_events = generate_web_events(config, rng, campaign_ids)
        import json as json_module

        with open(raw_path / "web_events.jsonl", "w", encoding="utf-8") as f:
            for _, row in web_events.iterrows():
                record = row.to_dict()
                record["timestamp"] = record["timestamp"].isoformat()
                record = {
                    k: (None if isinstance(v, float) and np.isnan(v) else v)
                    for k, v in record.items()
                }
                f.write(json_module.dumps(record, default=str) + "\n")
        crm_sales = generate_crm_sales(config, rng, web_events)
        crm_sales.to_csv(raw_path / "crm_sales.csv", index=False)

    def task_load_staging():
        db_path_obj.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path_obj))
        load_staging_tables(con, raw_path)
        con.close()

    def task_staging_quality_checks():
        con = duckdb.connect(str(db_path_obj))
        results = run_staging_checks(con)
        summary = summarize(results)
        con.close()
        logger.info(
            "Staging DQ checks (informational, non-gating): %d/%d passed. "
            "See docs/dq_report_staging.json for full detail.",
            summary["passed"],
            summary["total_checks"],
        )
        Path("docs").mkdir(exist_ok=True)
        with open("docs/dq_report_staging.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    def task_transform():
        con = duckdb.connect(str(db_path_obj))
        build_intermediate_layer(con)
        build_mart_layer(con)
        con.close()

    def task_mart_quality_checks():
        con = duckdb.connect(str(db_path_obj))
        results = run_mart_checks(con)
        summary = summarize(results)
        con.close()
        Path("docs").mkdir(exist_ok=True)
        with open("docs/dq_report_marts.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        if summary["fatal_failures"] > 0:
            raise PipelineFailedError(
                f"{summary['fatal_failures']} fatal mart-level data quality check(s) failed"
            )
        logger.info(
            "Mart DQ gate passed: %d/%d checks green.",
            summary["passed"],
            summary["total_checks"],
        )

    tasks = [
        Task(name="generate_sources", fn=task_generate_sources),
        Task(name="load_staging", fn=task_load_staging, depends_on=["generate_sources"]),
        Task(
            name="staging_quality_checks",
            fn=task_staging_quality_checks,
            depends_on=["load_staging"],
        ),
        Task(name="transform", fn=task_transform, depends_on=["staging_quality_checks"]),
        Task(
            name="mart_quality_checks",
            fn=task_mart_quality_checks,
            depends_on=["transform"],
        ),
    ]
    return DAGRunner(tasks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the marketing analytics ELT pipeline DAG")
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--db-path", type=str, default="data/warehouse/marketing.duckdb")
    args = parser.parse_args()

    runner = build_pipeline(args.raw_dir, args.db_path)
    run_log = runner.run()

    Path("docs").mkdir(exist_ok=True)
    with open("docs/dag_run_log.json", "w", encoding="utf-8") as f:
        json.dump([vars(r) for r in run_log], f, indent=2)

    logger.info("Pipeline completed successfully. Run log: docs/dag_run_log.json")


if __name__ == "__main__":
    main()
