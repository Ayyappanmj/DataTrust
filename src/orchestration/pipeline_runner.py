"""End-to-end pipeline: the same step functions back the CLI (`run`) and the Airflow DAG.

Steps (in order)
  1 discover_files   2 validate_files   3 ingest_bronze   4 schema_checks
  5 bronze_to_silver (Spark, writes to staging only)
  6 quality_rules    (rule results -> gate -> journalled commit of Silver + Quarantine)
  7 load_duckdb      8 dbt_models       9 final_quality_checks
  10 record_lineage  11 run_summary

State between steps lives in files/databases (never large objects): a small JSON run-state
file, the manifest, the staging plan and the metadata store.
"""
from __future__ import annotations

import json
import os
import secrets
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.config import Settings, validate_safe_value
from src.common.exceptions import (
    InjectedFault,
    PipelineLockedError,
    ProcessingError,
    QualityGateError,
    SchemaValidationError,
)
from src.common.logging_config import get_logger
from src.ingestion.bronze_ingest import bronze_path, ingest_file
from src.ingestion.file_discovery import DiscoveredFile, FileValidation, discover_files, validate_file
from src.ingestion.ingestion_manifest import (
    PlanEntry,
    mark_failed,
    mark_ingested,
    mark_processed,
    plan_files,
    register_file,
)
from src.metadata import lineage
from src.metadata.models import (
    DATASET_BRONZE,
    DATASET_QUARANTINE,
    DATASET_SILVER,
    GOLD_TABLES,
    RUN_FAILED,
    RUN_PARTIAL,
    RUN_SUCCEEDED,
    gold_dataset,
)
from src.metadata.repository import MetadataRepository, open_repository
from src.processing.incremental_processing import build_workset
from src.processing.schema_validation import check_schema, schema_fingerprint
from src.quality.quality_report import build_report, write_report
from src.quality.quality_runner import (
    build_results,
    enforce_gate,
    now_iso,
    persist_results,
    run_sql_quality,
)
from src.quality.rules import FILE_RULE_INDEX, load_rules
from src.storage.parquet_storage import StagingArea, partition_stats, recover_incomplete_commits
from src.storage.partition_manager import parse_date, partitions_for_range
from src.warehouse import warehouse_loader
from src.warehouse.duckdb_client import connect, execute, table_exists

log = get_logger(__name__)

STEPS = [
    "discover_files", "validate_files", "ingest_bronze", "schema_checks", "bronze_to_silver",
    "quality_rules", "load_duckdb", "dbt_models", "final_quality_checks", "record_lineage",
    "run_summary",
]
PIPELINE_NAME = "datatrust_batch"


def new_run_id() -> str:
    return f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"


def airflow_run_id(airflow_run: str) -> str:
    """Deterministic, path-safe run id derived from an Airflow run id (same in every task)."""
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in airflow_run)
    return f"af-{cleaned}"[:80]


@dataclass
class RunContext:
    run_id: str
    input_dir: str = ""
    force: bool = False
    force_models: bool = False
    mode: str = "incremental"  # incremental | backfill
    backfill_start: str = ""
    backfill_end: str = ""
    plan: list[dict] = field(default_factory=list)
    validations: dict[str, dict] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)  # checksum -> reason
    unsupported: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=lambda: {
        "records_read": 0, "records_accepted": 0, "records_rejected": 0, "records_written": 0})
    steps_done: list[str] = field(default_factory=list)
    models_built: list[str] = field(default_factory=list)
    warehouse: dict = field(default_factory=dict)
    committed_checksums: list[str] = field(default_factory=list)

    def entries(self) -> list[PlanEntry]:
        return [PlanEntry.from_dict(p) for p in self.plan]

    def state_path(self, settings: Settings) -> Path:
        return settings.state_dir / f"{self.run_id}.json"

    def save(self, settings: Settings) -> None:
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path(settings).with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self), indent=2), encoding="utf-8")
        tmp.replace(self.state_path(settings))

    @classmethod
    def load(cls, settings: Settings, run_id: str) -> RunContext | None:
        path = settings.state_dir / f"{run_id}.json"
        if not path.is_file():
            return None
        return cls(**json.loads(path.read_text(encoding="utf-8")))


class RunLock:
    """Single-run guard for CLI runs. A lock older than the timeout is treated as stale."""

    def __init__(self, settings: Settings, run_id: str) -> None:
        self.path = settings.lock_path
        self.run_id = run_id
        self.timeout = timedelta(minutes=settings.lock_timeout_minutes)

    def __enter__(self) -> RunLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                age = datetime.now() - datetime.fromtimestamp(self.path.stat().st_mtime)
                if age > self.timeout:
                    log.warning("Removing stale pipeline lock", extra={"age": str(age)})
                    self.path.unlink(missing_ok=True)
                    continue
                raise PipelineLockedError(
                    f"Another run holds {self.path} ({self.path.read_text(errors='ignore')[:80]!r}). "
                    "Wait for it, or delete the file if you are sure no run is active."
                ) from None
            with os.fdopen(fd, "w") as handle:
                handle.write(self.run_id)
            return self
        raise PipelineLockedError(f"Could not acquire {self.path}")

    def __exit__(self, *exc: object) -> None:
        try:
            if self.path.read_text(errors="ignore") == self.run_id:
                self.path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass


class PipelineRunner:
    def __init__(self, settings: Settings, repo: MetadataRepository | None = None) -> None:
        self.settings = settings
        settings.validate()
        settings.ensure_dirs()
        self._own_repo = repo is None
        self.repo = repo or open_repository(settings)
        self._spark: Any = None
        self.rules = load_rules(settings)

    # ------------------------------------------------------------------ plumbing
    @property
    def spark(self) -> Any:
        if self._spark is None:
            from src.processing.spark_session import get_spark

            self._spark = get_spark(self.settings)
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
        if self._own_repo:
            self.repo.close()

    def _fault(self, point: str) -> None:
        """Fault-injection hook for tests/demos: DATATRUST_FAULT_INJECTION=fail_at=<step>."""
        for item in self.settings.fault_injection.split(","):
            if item.strip() == f"fail_at={point}":
                raise InjectedFault(f"Injected fault at {point}")

    def _record_results(self, ctx: RunContext, stage: str, dataset: str, results: list) -> dict:
        persist_results(self.repo, results)
        report = build_report(ctx.run_id, stage, results)
        write_report(report, self.settings.reports_dir)
        return report

    # ------------------------------------------------------------------ public API
    def run(self, input_dir: Path, force: bool = False, run_id: str | None = None,
            trigger: str = "cli", force_models: bool = False,
            steps: list[str] | None = None) -> RunContext:
        run_id = validate_safe_value(run_id or new_run_id(), "run id")
        ctx = RunContext(run_id=run_id, input_dir=str(input_dir), force=force,
                         force_models=force_models)
        return self._execute(ctx, steps or STEPS, trigger)

    def backfill(self, start: str, end: str, run_id: str | None = None,
                 trigger: str = "cli") -> RunContext:
        parse_date(start), parse_date(end)
        run_id = validate_safe_value(run_id or new_run_id().replace("run-", "backfill-"), "run id")
        ctx = RunContext(run_id=run_id, mode="backfill", backfill_start=start, backfill_end=end)
        return self._execute(ctx, STEPS, trigger)

    def _execute(self, ctx: RunContext, steps: list[str], trigger: str) -> RunContext:
        with RunLock(self.settings, ctx.run_id):
            stale = self.repo.fail_stale_runs(ctx.run_id, self.settings.lock_timeout_minutes)
            if stale:
                log.warning("Marked interrupted runs as FAILED", extra={"runs": stale})
            self.repo.start_run(ctx.run_id, PIPELINE_NAME, trigger, {
                "input": ctx.input_dir, "force": ctx.force, "mode": ctx.mode,
                "backfill": [ctx.backfill_start, ctx.backfill_end], "partition_scheme":
                self.settings.partition_scheme, "modeling_engine": self.settings.modeling_engine})
            if ctx.mode == "backfill":
                values = partitions_for_range(parse_date(ctx.backfill_start),
                                              parse_date(ctx.backfill_end),
                                              self.settings.partition_scheme)
                self.repo.start_backfill(ctx.run_id, ctx.run_id, ctx.backfill_start,
                                         ctx.backfill_end, values)
            for step in steps:
                self.run_step(step, ctx)
        return ctx

    def run_step(self, step: str, ctx: RunContext) -> None:
        """Run one step; on any failure record it, fail the run and re-raise."""
        started = time.time()
        log.info("Step started", extra={"run_id": ctx.run_id, "step": step})
        try:
            self._fault(step)
            getattr(self, f"step_{step}")(ctx)
        except BaseException as exc:
            self._fail_run(ctx, step, exc)
            raise
        ctx.steps_done.append(step)
        ctx.save(self.settings)
        log.info("Step finished", extra={"run_id": ctx.run_id, "step": step,
                                         "seconds": round(time.time() - started, 2)})

    def _fail_run(self, ctx: RunContext, step: str, exc: BaseException) -> None:
        message = f"{step}: {type(exc).__name__}: {exc}"
        log.error("Step failed", extra={"run_id": ctx.run_id, "step": step, "error": str(exc)})
        try:
            self.repo.record_error(ctx.run_id, step, exc, {"steps_done": ctx.steps_done})
            self.repo.finish_run(ctx.run_id, RUN_FAILED, message[:1500], **ctx.counts)
            if ctx.mode == "backfill":
                self.repo.finish_backfill(ctx.run_id, "FAILED", None, message[:1500])
            ctx.save(self.settings)
        except Exception:  # noqa: BLE001 - never mask the original failure
            log.exception("Could not record failure in metadata store")

    def fail_run_externally(self, run_id: str, message: str) -> None:
        """Used by the Airflow failure callback so a dead task never leaves a RUNNING run."""
        run = self.repo.get_run(run_id)
        if run and run["status"] == "RUNNING":
            self.repo.record_error(run_id, "airflow", message)
            self.repo.finish_run(run_id, RUN_FAILED, message[:1500])

    def run_single_step(self, step: str, run_id: str, input_dir: str = "", force: bool = False,
                        backfill: tuple[str, str] | None = None, trigger: str = "airflow") -> RunContext:
        """Run one step in its own process (Airflow task). Context is reloaded from disk."""
        if step not in STEPS:
            raise ProcessingError(f"Unknown step {step!r}; choose from {STEPS}")
        validate_safe_value(run_id, "run id")
        ctx = RunContext.load(self.settings, run_id)
        if ctx is None:
            ctx = RunContext(run_id=run_id, input_dir=input_dir, force=force)
            if backfill:
                ctx.mode, (ctx.backfill_start, ctx.backfill_end) = "backfill", backfill
        if step == STEPS[0]:
            self.repo.start_run(run_id, PIPELINE_NAME, trigger, {"input": input_dir, "force": force})
        if step in ctx.steps_done:
            ctx.steps_done.remove(step)  # a retried task simply re-runs the (idempotent) step
        self.run_step(step, ctx)
        return ctx

    # ------------------------------------------------------------------ steps
    def step_discover_files(self, ctx: RunContext) -> None:
        recovered = recover_incomplete_commits(self.settings)
        if recovered:
            log.warning("Recovered interrupted commits", extra={"runs": recovered})
        if ctx.mode == "backfill":
            active = [r for r in self.repo.list_manifest(active_only=True)
                      if r["process_status"] == "SUCCEEDED"]
            if not active:
                raise ProcessingError("Nothing to backfill: no processed source files in the manifest")
            ctx.plan = [PlanEntry(
                path=r["file_path"], file_format=r["file_format"], size_bytes=r["size_bytes"],
                checksum=r["checksum"], action="BACKFILL", needs_ingest=False, needs_process=True,
                reason="backfill from Bronze").to_dict() for r in active]
            return
        files, skipped = discover_files(Path(ctx.input_dir))
        ctx.unsupported = [f"{s.path.name}: {s.reason}" for s in skipped]
        entries = plan_files(self.repo, files, ctx.force)
        ctx.plan = [e.to_dict() for e in entries]
        for e in entries:
            log.info("Planned", extra={"file": Path(e.path).name, "action": e.action, "why": e.reason})
        if not files:
            log.warning("No supported input files found", extra={"input": ctx.input_dir})

    def _file_for(self, entry: PlanEntry) -> DiscoveredFile:
        return DiscoveredFile(Path(entry.path), entry.file_format, entry.size_bytes, entry.checksum)

    def _fail_file(self, ctx: RunContext, entry: PlanEntry, stage: str, message: str) -> None:
        ctx.failed[entry.checksum] = message
        register_file(self.repo, entry, ctx.run_id)
        mark_failed(self.repo, entry.checksum, stage, message, ctx.run_id)
        self.repo.record_error(ctx.run_id, f"{stage}_file", message, file_path=entry.path)
        if self.settings.on_file_error == "abort":
            raise ProcessingError(f"{entry.path}: {message}")

    def step_validate_files(self, ctx: RunContext) -> None:
        if ctx.mode == "backfill":
            return
        todo = [e for e in ctx.entries() if e.needs_ingest and not e.skipped]
        unreadable = malformed = rows = 0
        for entry in todo:
            validation = validate_file(self._file_for(entry), self.settings.csv_delimiter)
            ctx.validations[entry.checksum] = asdict(validation)
            if not validation.ok:
                unreadable += 1
                self._fail_file(ctx, entry, "ingest", f"invalid source file: {validation.error}")
            else:
                malformed += validation.malformed_lines
                rows += validation.row_count or 0
        if todo:
            results = build_results("source_files", "ingestion", ctx.run_id,
                                    [FILE_RULE_INDEX["source_file_readable"]],
                                    {"source_file_readable": (len(todo), unreadable)})
            results += build_results("source_files", "ingestion", ctx.run_id,
                                     [FILE_RULE_INDEX["malformed_lines"]],
                                     {"malformed_lines": (rows, malformed)})
            self._record_results(ctx, "ingestion", "source_files", results)

    def step_ingest_bronze(self, ctx: RunContext) -> None:
        if ctx.mode == "backfill":
            return
        for entry in ctx.entries():
            if entry.skipped or entry.checksum in ctx.failed:
                continue
            register_file(self.repo, entry, ctx.run_id)
            if not entry.needs_ingest:
                continue
            validation = FileValidation(**ctx.validations[entry.checksum])
            try:
                count = ingest_file(self.spark, self.settings, self._file_for(entry), validation,
                                    ctx.run_id)
            except Exception as exc:  # noqa: BLE001 - recorded, then handled per file policy
                self._fail_file(ctx, entry, "ingest", f"{type(exc).__name__}: {exc}")
                continue
            mark_ingested(self.repo, entry.checksum, count, json.dumps(validation.columns),
                          schema_fingerprint(validation.columns),
                          bronze_path(self.settings, entry.checksum).as_posix(), ctx.run_id)
            lineage.record_ingest(self.repo, ctx.run_id, entry.path, entry.checksum, count)

    def step_schema_checks(self, ctx: RunContext) -> None:
        if ctx.mode == "backfill":
            return
        checked = missing = unexpected = drift = 0
        active_prints = {r["schema_fingerprint"] for r in self.repo.list_manifest(active_only=True)
                         if r["schema_fingerprint"]}
        for entry in ctx.entries():
            if entry.skipped or entry.checksum in ctx.failed:
                continue
            row = self.repo.get_manifest(entry.checksum) or {}
            columns = (ctx.validations.get(entry.checksum, {}).get("columns")
                       or json.loads(row.get("columns_json") or "[]"))
            try:
                previous = next(iter(active_prints - {schema_fingerprint(columns)}), None) \
                    if active_prints else None
                result = check_schema(columns, self.settings.unexpected_columns, previous)
            except SchemaValidationError as exc:
                self._fail_file(ctx, entry, "process", str(exc))
                missing += 1
                checked += 1
                continue
            checked += 1
            missing += bool(result.missing_required)
            unexpected += bool(result.unexpected)
            drift += result.drift_from_previous
            for message in result.messages:
                log.warning("Schema check", extra={"file": Path(entry.path).name, "msg": message})
            if not result.ok:
                self._fail_file(ctx, entry, "process", "; ".join(result.messages))
        if checked:
            rules = [FILE_RULE_INDEX[n] for n in
                     ("required_columns_present", "unexpected_columns", "schema_drift")]
            stats = {"required_columns_present": (checked, missing),
                     "unexpected_columns": (checked, unexpected), "schema_drift": (checked, drift)}
            self._record_results(ctx, "ingestion", "source_files",
                                 build_results("source_files", "ingestion", ctx.run_id, rules, stats))

    def step_bronze_to_silver(self, ctx: RunContext) -> None:
        from src.processing.silver_job import run_silver_job

        if ctx.mode == "backfill":
            values = partitions_for_range(parse_date(ctx.backfill_start), parse_date(ctx.backfill_end),
                                          self.settings.partition_scheme)
            checksums = [e.checksum for e in ctx.entries()]
            run_silver_job(self.spark, self.settings, ctx.run_id, self.rules, checksums, set(), values)
            return
        work = build_workset(ctx.entries(), set(ctx.failed))
        if work.empty:
            log.info("Nothing to process")
            return
        run_silver_job(self.spark, self.settings, ctx.run_id, self.rules, work.process_checksums,
                       work.remove_checksums)

    def step_quality_rules(self, ctx: RunContext) -> None:
        stage = StagingArea(self.settings, ctx.run_id)
        if not stage.plan_path.is_file():
            log.info("No staged data; skipping quality gate")
            return
        plan = stage.read_plan()
        usable = [r for r in self.rules if r.name in plan["rule_stats"] and r.enabled]
        stats = {k: tuple(v) for k, v in plan["rule_stats"].items()}
        results = build_results("silver.trips", "silver", ctx.run_id, usable, stats)
        report = self._record_results(ctx, "silver", "silver.trips", results)
        log.info("Quality report", extra={"overall": report["overall_status"]})
        try:
            enforce_gate(results, self.settings)
        except QualityGateError as exc:
            for checksum in plan["process_checksums"]:
                if ctx.mode != "backfill":
                    mark_failed(self.repo, checksum, "process", str(exc), ctx.run_id)
            raise
        self._fault("commit")
        stage.commit(fault_hook=self._fault)
        self._after_commit(ctx, plan)
        stage.discard()

    def _after_commit(self, ctx: RunContext, plan: dict) -> None:
        pcol = plan["partition_column"]
        stats = partition_stats(self.settings.silver_dir, pcol, plan["affected_values"])
        for value in plan["affected_values"]:
            files, size = stats.get(value, (0, 0))
            self.repo.upsert_partition(DATASET_SILVER, value, plan["partition_rows"].get(value, 0),
                                       size, files, ctx.run_id)
        entries = {e.checksum: e for e in ctx.entries()}
        for checksum in plan["process_checksums"]:
            if ctx.mode != "backfill":
                mark_processed(self.repo, entries[checksum], ctx.run_id)
            lineage.record_silver(self.repo, ctx.run_id, checksum,
                                  plan["per_file_accepted"].get(checksum[:20], 0))
            ctx.committed_checksums.append(checksum)
        for name, layer, loc, col, desc in (
            (DATASET_BRONZE, "bronze", self.settings.bronze_dir, "source_checksum",
             "Raw ingested records, one folder per source-file checksum"),
            (DATASET_SILVER, "silver", self.settings.silver_dir, pcol,
             "Cleaned, typed, de-duplicated trips"),
            (DATASET_QUARANTINE, "quarantine", self.settings.quarantine_dir, "source_checksum",
             "Rejected records with rejection_reason"),
        ):
            self.repo.upsert_dataset(name, layer, str(loc), col, desc)
        for key, value in plan["counts"].items():
            if key in ctx.counts:
                ctx.counts[key] += value
        self.repo.update_run(ctx.run_id, **ctx.counts)

    def step_load_duckdb(self, ctx: RunContext) -> None:
        ctx.warehouse = warehouse_loader.sync_warehouse(self.settings, self.repo)

    def step_dbt_models(self, ctx: RunContext) -> None:
        needs = (ctx.warehouse.get("partitions_synced", 0) > 0 or ctx.force_models
                 or not self._gold_exists())
        if not needs:
            log.info("Models are up to date; skipping rebuild")
            return
        if self.settings.modeling_engine == "dbt":
            warehouse_loader.run_dbt(self.settings, "build")
            ctx.models_built = list(warehouse_loader.model_files())
        else:
            ctx.models_built = warehouse_loader.build_models(self.settings)
        self._fault("after_models")

    def _gold_exists(self) -> bool:
        with connect(self.settings.warehouse_path) as conn:
            return table_exists(conn, "gold", "daily_trip_metrics")

    def step_final_quality_checks(self, ctx: RunContext) -> None:
        from src.quality.rules import RuleResult
        from src.quality.validators import validate_gold_contract

        with connect(self.settings.warehouse_path, read_only=True) as conn:
            results = run_sql_quality(conn, "silver_trips", "warehouse.silver_trips", "warehouse",
                                      ctx.run_id, rules=self.rules)
            if table_exists(conn, "gold", "daily_trip_metrics"):
                frame = execute(conn, "SELECT * FROM gold.daily_trip_metrics").df()
                if len(frame):
                    problems = validate_gold_contract(frame)
                    results.append(RuleResult(
                        "gold.daily_trip_metrics", "gold_contract_pandera",
                        "Daily metrics satisfy the Pandera schema (unique dates, non-negative revenue).",
                        len(frame), len(problems), "ERROR", "flag", 0.0, "warehouse", ctx.run_id,
                        now_iso()))
        results = [r for r in results if r.rule_name != "rejection_rate_acceptable"]
        self._record_results(ctx, "warehouse", "warehouse.silver_trips", results)
        enforce_gate(results, self.settings)

    def step_record_lineage(self, ctx: RunContext) -> None:
        has_gold_edges = bool(self.repo.lineage_upstream(gold_dataset("daily_trip_metrics")))
        if not ctx.models_built and has_gold_edges:
            return  # models were not rebuilt and lineage already exists
        counts: dict[str, int] = {}
        with connect(self.settings.warehouse_path, read_only=True) as conn:
            for table in GOLD_TABLES:
                schema = "intermediate" if table == "clean_trips" else "gold"
                if table_exists(conn, schema, table):
                    counts[table] = execute(conn, f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        lineage.record_gold(self.repo, ctx.run_id, counts)
        for table in GOLD_TABLES:
            self.repo.upsert_dataset(gold_dataset(table), "gold", f"duckdb:gold.{table}", None,
                                     f"Analytical model {table}")

    def step_run_summary(self, ctx: RunContext) -> None:
        status = RUN_PARTIAL if ctx.failed else RUN_SUCCEEDED
        error = "; ".join(f"{Path(e.path).name}: {ctx.failed[e.checksum]}"
                          for e in ctx.entries() if e.checksum in ctx.failed) or None
        summary = {
            "run_id": ctx.run_id, "status": status, "mode": ctx.mode, "counts": ctx.counts,
            "files": [{"path": e.path, "action": e.action, "reason": e.reason,
                       "failed": ctx.failed.get(e.checksum)} for e in ctx.entries()],
            "unsupported_files": ctx.unsupported, "warehouse": ctx.warehouse,
            "models_built": ctx.models_built, "finished_at": now_iso(),
        }
        out = self.settings.reports_dir / "runs"
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{ctx.run_id}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        self.repo.finish_run(ctx.run_id, status, error, **ctx.counts)
        if ctx.mode == "backfill":
            self.repo.finish_backfill(ctx.run_id, "SUCCEEDED", ctx.counts["records_written"])
        ctx.save(self.settings)
        log.info("Run finished", extra={"run_id": ctx.run_id, "status": status, **ctx.counts})


def quality_only(settings: Settings, repo: MetadataRepository, run_id: str | None = None) -> list:
    """Re-run quality rules on the warehouse Silver table (CLI: `quality`)."""
    run_id = run_id or new_run_id().replace("run-", "quality-")
    rules = load_rules(settings)
    repo.start_run(run_id, "datatrust_quality", "cli", {})
    try:
        with connect(settings.warehouse_path, read_only=True) as conn:
            results = run_sql_quality(conn, "silver_trips", "warehouse.silver_trips", "warehouse",
                                      run_id, rules=rules)
        persist_results(repo, results)
        write_report(build_report(run_id, "warehouse", results), settings.reports_dir)
        enforce_gate(results, settings)
        repo.finish_run(run_id, RUN_SUCCEEDED)
    except BaseException as exc:
        repo.record_error(run_id, "quality", exc)
        repo.finish_run(run_id, RUN_FAILED, str(exc)[:1500])
        raise
    return results
