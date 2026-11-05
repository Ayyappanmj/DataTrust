"""Bronze -> Silver Spark job. Writes ONLY to the staging area; nothing is committed here."""
from __future__ import annotations

import time
from typing import Any

from src.common.config import Settings
from src.common.exceptions import ProcessingError
from src.common.logging_config import get_logger
from src.ingestion.bronze_ingest import bronze_path
from src.processing.deduplication import split_against_existing, split_in_batch_duplicates
from src.processing.transformations import QUARANTINE_COLUMNS, SILVER_COLUMNS, to_typed
from src.quality.rules import Rule, applicable
from src.quality.validators import spark_reject_reasons, spark_rule_stats
from src.storage.parquet_storage import StagingArea, read_selected_partitions, uri

log = get_logger(__name__)


def _distinct_values(df: Any, column: str) -> list[str]:
    return sorted(r[0] for r in df.select(column).where(f"{column} IS NOT NULL").distinct().collect())


def run_silver_job(
    spark: Any, settings: Settings, run_id: str, rules: list[Rule], process_checksums: list[str],
    remove_checksums: set[str], backfill_values: list[str] | None = None,
) -> dict:
    """Clean, validate, de-duplicate and stage Silver + Quarantine. Returns the plan dict.

    Incremental mode  : only ``process_checksums`` are read; Silver partitions touched by them
                        (or by ``remove_checksums``) are rebuilt from existing rows + new rows.
    Backfill mode     : all given checksums are read, filtered to ``backfill_values`` partitions,
                        and those partitions are rebuilt from Bronze only.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started = time.time()
    pcol = settings.partition_column
    backfill = backfill_values is not None
    paths = [bronze_path(settings, c) for c in process_checksums]
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise ProcessingError(f"Bronze data missing for: {[p.name for p in missing]}")
    stage = StagingArea(settings, run_id)
    stage.reset()

    typed = to_typed(spark.read.parquet(*[uri(p) for p in paths]), settings, run_id)
    if backfill:
        typed = typed.filter(F.col(pcol).isin(backfill_values))
    typed = typed.persist(StorageLevel.MEMORY_AND_DISK)
    cleanup: list[Any] = [typed]
    try:
        usable = applicable(rules, set(typed.columns))
        records_read, stats = spark_rule_stats(typed, usable)
        flagged = typed.withColumn("rejection_reasons", spark_reject_reasons(usable))
        valid = flagged.filter(F.size("rejection_reasons") == 0)
        invalid = flagged.filter(F.size("rejection_reasons") > 0)

        fresh, dup_batch = split_in_batch_duplicates(valid)
        remove = sorted(remove_checksums)
        existing = None
        old_values: list[str] = []
        if not backfill:
            silver_all = None
            if settings.silver_dir.is_dir() and remove and any(settings.silver_dir.rglob("*.parquet")):
                silver_all = spark.read.option("basePath", uri(settings.silver_dir)).parquet(
                    uri(settings.silver_dir))
                old_values = _distinct_values(silver_all.filter(F.col("source_checksum").isin(
                    [r[:20] for r in remove])), pcol)
            affected = sorted(set(_distinct_values(fresh, pcol)) | set(old_values))
            existing = read_selected_partitions(spark, settings.silver_dir, pcol, affected)
            if existing is not None and remove:
                existing = existing.filter(~F.col("source_checksum").isin([r[:20] for r in remove]))
        else:
            affected = sorted(backfill_values)
        new_rows, dup_existing = split_against_existing(fresh, existing)
        merged = new_rows.select(*SILVER_COLUMNS)
        if existing is not None:
            merged = existing.select(*SILVER_COLUMNS).unionByName(merged)
        merged = merged.persist(StorageLevel.MEMORY_AND_DISK)
        cleanup.append(merged)

        part_rows = {r[pcol]: int(r["count"]) for r in merged.groupBy(pcol).count().collect()}
        (merged.repartition(F.col(pcol)).write.mode("overwrite").partitionBy(pcol)
         .option("maxRecordsPerFile", 1_000_000).parquet(uri(stage.silver_root)))

        # ---- quarantine: validity rejects + duplicates, merged with older rejects of same files
        def as_quarantine(df: Any, reason: Any) -> Any:
            return (df.withColumn("rejection_reason", reason)
                    .withColumn("run_id", F.lit(run_id))
                    .withColumn("rejected_at", F.current_timestamp())
                    .select(*QUARANTINE_COLUMNS))

        parts = [as_quarantine(invalid, F.concat_ws(";", "rejection_reasons")),
                 as_quarantine(dup_batch, F.lit("duplicate_record"))]
        if dup_existing is not None:
            parts.append(as_quarantine(dup_existing, F.lit("duplicate_record")))
        rejected = parts[0]
        for extra in parts[1:]:
            rejected = rejected.unionByName(extra)
        q_values = sorted({c[:20] for c in process_checksums})
        old_q = read_selected_partitions(spark, settings.quarantine_dir, "source_checksum", q_values)
        if old_q is not None:
            scope_keys = typed.select("source_checksum", "source_row_id")
            kept_old = old_q.select(*QUARANTINE_COLUMNS)
            kept_old = kept_old.join(scope_keys, ["source_checksum", "source_row_id"], "left_anti")
            rejected = rejected.unionByName(kept_old)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        cleanup.append(rejected)
        (rejected.repartition(F.col("source_checksum")).write.mode("overwrite")
         .partitionBy("source_checksum").parquet(uri(stage.quarantine_root)))

        per_file_read = {r["source_checksum"]: int(r["count"])
                         for r in typed.groupBy("source_checksum").count().collect()}
        per_file_accepted = {r["source_checksum"]: int(r["count"])
                             for r in new_rows.groupBy("source_checksum").count().collect()}
        n_invalid = invalid.count()
        n_dup_batch = dup_batch.count()
        n_dup_existing = dup_existing.count() if dup_existing is not None else 0
        n_accepted = sum(per_file_accepted.values())
        total_rejected = n_invalid + n_dup_batch + n_dup_existing
        evaluated_valid = records_read - n_invalid
        stats["duplicate_record"] = (evaluated_valid, n_dup_batch + n_dup_existing)
        stats["rejection_rate_acceptable"] = (records_read, total_rejected)
        plan = {
            "run_id": run_id, "mode": "backfill" if backfill else "incremental",
            "partition_column": pcol, "affected_values": affected,
            "quarantine_values": q_values, "process_checksums": process_checksums,
            "remove_checksums": remove, "partition_rows": part_rows,
            "per_file_read": per_file_read, "per_file_accepted": per_file_accepted,
            "counts": {
                "records_read": records_read, "records_accepted": n_accepted,
                "records_rejected": total_rejected, "records_written": sum(part_rows.values()),
                "rejected_validity": n_invalid, "duplicates_in_batch": n_dup_batch,
                "duplicates_vs_existing": n_dup_existing,
            },
            "rule_stats": {k: list(v) for k, v in stats.items()},
            "duration_seconds": round(time.time() - started, 2),
        }
        stage.write_plan(plan)
        log.info("Silver staged", extra={"run_id": run_id, **plan["counts"]})
        return plan
    finally:
        for frame in cleanup:
            frame.unpersist()
