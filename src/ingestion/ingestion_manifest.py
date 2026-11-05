"""File-level incremental planning on top of the ingestion manifest.

For every discovered file the planner decides what work is needed:

* NEW               - never seen: ingest and process.
* UNCHANGED         - same checksum already fully processed: skip.
* CHANGED           - same path, different checksum: ingest the new version and replace the old
                      version's rows in Silver (the old checksum is superseded at commit time).
* RETRY             - same checksum, but an earlier attempt did not finish: resume the missing stage.
* REACTIVATE        - a previously superseded version came back: process it again.
* DUPLICATE_CONTENT - same bytes already known under another path: skipped (reported).
* FORCE             - user asked for a controlled reprocessing of an unchanged file.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.ingestion.file_discovery import DiscoveredFile
from src.metadata.models import STAGE_SUCCEEDED

SKIP_ACTIONS = {"UNCHANGED", "DUPLICATE_CONTENT"}


@dataclass
class PlanEntry:
    path: str
    file_format: str
    size_bytes: int
    checksum: str
    action: str
    needs_ingest: bool
    needs_process: bool
    supersedes: str | None = None  # checksum of the previous version of the same path
    refresh: bool = False  # Silver may already hold rows from this checksum: replace them
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> PlanEntry:
        return cls(**data)

    @property
    def skipped(self) -> bool:
        return self.action in SKIP_ACTIONS


def plan_files(repo: Any, files: list[DiscoveredFile], force: bool = False) -> list[PlanEntry]:
    """Compare discovered files with the manifest and decide the work for each."""
    entries: list[PlanEntry] = []
    seen_in_batch: dict[str, str] = {}
    for f in files:
        path = f.path.as_posix()
        base = dict(path=path, file_format=f.file_format, size_bytes=f.size_bytes, checksum=f.checksum)
        if f.checksum in seen_in_batch:
            entries.append(PlanEntry(**base, action="DUPLICATE_CONTENT", needs_ingest=False,
                                     needs_process=False,
                                     reason=f"identical bytes to {seen_in_batch[f.checksum]}"))
            continue
        seen_in_batch[f.checksum] = path
        row = repo.get_manifest(f.checksum)
        if row is not None:
            done = (row["ingest_status"] == STAGE_SUCCEEDED
                    and row["process_status"] == STAGE_SUCCEEDED)
            if row["active"] and done:
                if force:
                    entries.append(PlanEntry(**base, action="FORCE", needs_ingest=True,
                                             needs_process=True, refresh=True,
                                             reason="forced reprocessing"))
                elif row["file_path"] != path:
                    entries.append(PlanEntry(
                        **base, action="DUPLICATE_CONTENT", needs_ingest=False, needs_process=False,
                        reason=f"identical bytes already ingested from {row['file_path']}"))
                else:
                    entries.append(PlanEntry(**base, action="UNCHANGED", needs_ingest=False,
                                             needs_process=False, reason="checksum already processed"))
            elif row["ingest_status"] == STAGE_SUCCEEDED and done and not row["active"]:
                prev = repo.get_active_manifest_for_path(path)
                entries.append(PlanEntry(
                    **base, action="REACTIVATE", needs_ingest=force, needs_process=True,
                    supersedes=prev["checksum"] if prev and prev["checksum"] != f.checksum else None,
                    refresh=True, reason="previously superseded version is current again"))
            else:
                prev = repo.get_active_manifest_for_path(path)
                entries.append(PlanEntry(
                    **base, action="RETRY",
                    needs_ingest=row["ingest_status"] != STAGE_SUCCEEDED or force,
                    needs_process=True,
                    supersedes=prev["checksum"] if prev and prev["checksum"] != f.checksum else None,
                    refresh=True, reason="earlier attempt did not complete"))
            continue
        prev = repo.get_active_manifest_for_path(path)
        if prev is not None:
            entries.append(PlanEntry(**base, action="CHANGED", needs_ingest=True, needs_process=True,
                                     supersedes=prev["checksum"],
                                     reason=f"content changed (previous {prev['checksum'][:12]})"))
        else:
            entries.append(PlanEntry(**base, action="NEW", needs_ingest=True, needs_process=True,
                                     reason="first time seen"))
    return entries


def register_file(repo: Any, entry: PlanEntry, run_id: str) -> None:
    """Create/refresh the manifest row for a file about to be processed (not yet active)."""
    repo.upsert_manifest(
        entry.checksum, file_path=entry.path, file_name=Path(entry.path).name,
        file_format=entry.file_format, size_bytes=entry.size_bytes, last_run_id=run_id,
        **({"ingest_status": "PENDING"} if entry.needs_ingest else {}),
        **({"process_status": "PENDING"} if entry.needs_process else {}),
    )


def mark_ingested(repo: Any, checksum: str, record_count: int, columns_json: str,
                  fingerprint: str, bronze_path: str, run_id: str) -> None:
    from src.metadata.repository import utc_now

    repo.upsert_manifest(checksum, ingest_status=STAGE_SUCCEEDED, record_count=record_count,
                         columns_json=columns_json, schema_fingerprint=fingerprint,
                         bronze_path=bronze_path, ingested_at=utc_now(), last_run_id=run_id,
                         error_message=None)


def mark_failed(repo: Any, checksum: str, stage: str, message: str, run_id: str) -> None:
    column = "ingest_status" if stage == "ingest" else "process_status"
    repo.upsert_manifest(checksum, **{column: "FAILED"}, error_message=message[:2000],
                         last_run_id=run_id)


def mark_processed(repo: Any, entry: PlanEntry, run_id: str) -> None:
    """Commit-time update: the new version becomes active and the old one is superseded."""
    from src.metadata.repository import utc_now

    repo.upsert_manifest(entry.checksum, process_status=STAGE_SUCCEEDED, active=1,
                         processed_at=utc_now(), last_run_id=run_id, error_message=None)
    if entry.supersedes:
        repo.set_active(entry.supersedes, False)
