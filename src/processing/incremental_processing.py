"""Incremental processing helpers: which inputs need work, and which partitions they touch.

File-level incremental processing (what DataTrust implements): decisions are made per input
file using its SHA-256 checksum. A file is processed once; a changed file replaces the rows
that its previous version contributed.

Record-level incremental processing / change-data-capture (NOT implemented): that would track
individual inserts, updates and deletes inside a source system. Public TLC files are monthly
snapshots with no change feed, so file-level tracking plus record-level de-duplication is used.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from src.ingestion.ingestion_manifest import PlanEntry


@dataclass
class WorkSet:
    """What the Bronze->Silver step must do for one run."""

    process_checksums: list[str] = field(default_factory=list)
    remove_checksums: set[str] = field(default_factory=set)  # rows to drop before re-adding
    entries: list[PlanEntry] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.process_checksums


def build_workset(entries: list[PlanEntry], failed_checksums: set[str]) -> WorkSet:
    """Select entries that still need processing and are not already failed this run."""
    work = WorkSet()
    for entry in entries:
        if entry.skipped or not entry.needs_process or entry.checksum in failed_checksums:
            continue
        work.entries.append(entry)
        work.process_checksums.append(entry.checksum)
        if entry.refresh:
            work.remove_checksums.add(entry.checksum)
        if entry.supersedes:
            work.remove_checksums.add(entry.supersedes)
    return work
