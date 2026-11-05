"""Parquet layout helpers: URIs, staging area, journalled commit and crash recovery.

Write protocol (used for Silver and Quarantine):
  1. Spark writes the NEW version of every affected partition into ``data/_staging/<run_id>``.
  2. Quality gate runs on the staged result. If it fails nothing under ``data/silver`` changes.
  3. ``StagingArea.commit`` writes a journal, then swaps only the affected partitions.
  4. After a crash during step 3, ``recover_incomplete_commits`` rolls the swap forward.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from src.common.config import Settings
from src.common.exceptions import ProcessingError
from src.common.logging_config import get_logger
from src.storage.partition_manager import (
    apply_partition_actions,
    list_partitions,
    partition_dir,
    select_partitions,
    write_journal,
)

log = get_logger(__name__)


def uri(path: Path) -> str:
    """file:// URI that Spark/Hadoop understands on Windows and Linux."""
    return unquote(Path(path).resolve().as_uri())


def read_selected_partitions(spark: Any, root: Path, column: str, values: list[str]) -> Any | None:
    """Read only the requested partitions (partition pruning by path). None if none exist."""
    paths = select_partitions(root, column, values)
    if not paths:
        return None
    return spark.read.option("basePath", uri(root)).parquet(*[uri(p) for p in paths])


def partition_stats(root: Path, column: str, values: list[str]) -> dict[str, tuple[int, int]]:
    """{value: (file_count, size_bytes)} for the given partitions that exist on disk."""
    existing = list_partitions(root, column)
    return {v: (existing[v].file_count, existing[v].size_bytes) for v in values if v in existing}


class StagingArea:
    """Per-run staging directory with a plan file and a commit journal."""

    def __init__(self, settings: Settings, run_id: str) -> None:
        self.settings = settings
        self.run_id = run_id
        self.root = settings.staging_dir / run_id
        self.silver_root = self.root / "silver"
        self.quarantine_root = self.root / "quarantine"
        self.bronze_root = self.root / "bronze"
        self.plan_path = self.root / "plan.json"
        self.journal_path = self.root / "journal.json"

    def reset(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)

    def write_plan(self, plan: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        write_journal(self.plan_path, plan)

    def read_plan(self) -> dict:
        if not self.plan_path.is_file():
            raise ProcessingError(f"No staged plan for run {self.run_id}; run bronze_to_silver first")
        return json.loads(self.plan_path.read_text(encoding="utf-8"))

    def commit(self, fault_hook: Any = None) -> dict:
        """Swap staged Silver and Quarantine partitions into place (journalled)."""
        plan = self.read_plan()
        pcol = plan["partition_column"]
        silver_actions = {
            v: "replace" if partition_dir(self.silver_root, pcol, v).exists() else "delete"
            for v in plan["affected_values"]
        }
        quarantine_actions = {
            c: "replace" if partition_dir(self.quarantine_root, "source_checksum", c).exists()
            else "delete" for c in plan["quarantine_values"]
        }
        trash = self.settings.trash_dir / self.run_id
        write_journal(self.journal_path, {
            "state": "COMMITTING", "partition_column": pcol, "silver": silver_actions,
            "quarantine": quarantine_actions,
        })
        apply_partition_actions(self.silver_root, self.settings.silver_dir, pcol, silver_actions, trash)
        if fault_hook:
            fault_hook("commit_mid")
        apply_partition_actions(self.quarantine_root, self.settings.quarantine_dir,
                                "source_checksum", quarantine_actions, trash)
        write_journal(self.journal_path, {
            "state": "COMMITTED", "partition_column": pcol, "silver": silver_actions,
            "quarantine": quarantine_actions,
        })
        if trash.exists():
            shutil.rmtree(trash, ignore_errors=True)
        return {"silver": silver_actions, "quarantine": quarantine_actions}

    def discard(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


def recover_incomplete_commits(settings: Settings) -> list[str]:
    """Roll forward any commit that was interrupted mid-swap. Returns recovered run ids."""
    recovered: list[str] = []
    if not settings.staging_dir.is_dir():
        return recovered
    for run_dir in sorted(settings.staging_dir.iterdir()):
        journal = run_dir / "journal.json"
        if not journal.is_file():
            continue
        data = json.loads(journal.read_text(encoding="utf-8"))
        if data.get("state") != "COMMITTING":
            continue
        area = StagingArea(settings, run_dir.name)
        log.warning("Rolling forward interrupted commit", extra={"run_id": run_dir.name})
        apply_partition_actions(area.silver_root, settings.silver_dir, data["partition_column"],
                                data["silver"], settings.trash_dir / run_dir.name)
        apply_partition_actions(area.quarantine_root, settings.quarantine_dir, "source_checksum",
                                data["quarantine"], settings.trash_dir / run_dir.name)
        data["state"] = "COMMITTED"
        write_journal(journal, data)
        recovered.append(run_dir.name)
    return recovered
