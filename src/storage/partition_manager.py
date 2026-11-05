"""Hive-style partition directories: listing, range selection and safe replacement.

Partitions are replaced with a *journal* so an interrupted commit can be rolled forward and
unrelated partitions are never touched.
"""
from __future__ import annotations

import json
import re
import shutil
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from src.common.config import validate_safe_value
from src.common.exceptions import ProcessingError

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


@dataclass(frozen=True)
class PartitionInfo:
    column: str
    value: str
    path: Path
    file_count: int
    size_bytes: int


def partition_dir(root: Path, column: str, value: str) -> Path:
    validate_safe_value(column, "partition column")
    validate_safe_value(value, "partition value")
    return root / f"{column}={value}"


def data_files(directory: Path) -> list[Path]:
    """Parquet data files in a directory (ignores _SUCCESS and .crc files)."""
    return sorted(p for p in directory.rglob("*.parquet") if p.is_file())


def list_partitions(root: Path, column: str) -> dict[str, PartitionInfo]:
    """Existing partitions under ``root`` that contain at least one Parquet file."""
    found: dict[str, PartitionInfo] = {}
    if not root.is_dir():
        return found
    prefix = f"{column}="
    for child in sorted(root.iterdir()):
        if child.is_dir() and child.name.startswith(prefix):
            files = data_files(child)
            if files:
                value = child.name[len(prefix):]
                found[value] = PartitionInfo(column, value, child, len(files),
                                             sum(f.stat().st_size for f in files))
    return found


def select_partitions(root: Path, column: str, values: list[str]) -> list[Path]:
    """Directories of the requested partitions that exist (partition selection / pruning)."""
    existing = list_partitions(root, column)
    return [existing[v].path for v in values if v in existing]


def parse_date(value: str) -> date:
    if not _DATE_RE.match(value):
        raise ProcessingError(f"Expected a date as YYYY-MM-DD, got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ProcessingError(f"Invalid calendar date {value!r}") from exc


def partitions_for_range(start: date, end: date, scheme: str) -> list[str]:
    """Partition values covering [start, end] inclusive (whole months for 'monthly')."""
    if end < start:
        raise ProcessingError(f"Backfill end {end} is before start {start}")
    if scheme == "daily":
        return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
    if scheme == "monthly":
        values, year, month = [], start.year, start.month
        while (year, month) <= (end.year, end.month):
            values.append(f"{year:04d}-{month:02d}")
            month += 1
            if month == 13:
                year, month = year + 1, 1
        return values
    raise ProcessingError(f"Unknown partition scheme {scheme!r}")


def month_bounds(value: str) -> tuple[date, date]:
    if not _MONTH_RE.match(value):
        raise ProcessingError(f"Expected YYYY-MM, got {value!r}")
    year, month = int(value[:4]), int(value[5:])
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


# ---------------------------------------------------------------- safe replacement
def _remove(path: Path, trash: Path | None) -> None:
    if not path.exists():
        return
    if trash is None:
        shutil.rmtree(path)
        return
    trash.mkdir(parents=True, exist_ok=True)
    destination = trash / path.name
    if destination.exists():
        shutil.rmtree(destination)
    shutil.move(str(path), str(destination))


def apply_partition_actions(
    staging_root: Path, target_root: Path, column: str, actions: dict[str, str],
    trash: Path | None = None,
) -> None:
    """Idempotently apply ``{value: 'replace'|'delete'}`` to ``target_root``.

    replace: staged partition (if still present) takes the place of the target partition.
    delete : target partition is removed.
    Running this twice with the same arguments is safe (roll-forward after a crash).
    """
    target_root.mkdir(parents=True, exist_ok=True)
    for value, action in sorted(actions.items()):
        target = partition_dir(target_root, column, value)
        staged = partition_dir(staging_root, column, value)
        if action == "replace":
            if staged.exists():
                _remove(target, trash)
                shutil.move(str(staged), str(target))
            elif not target.exists():
                raise ProcessingError(
                    f"Cannot replace partition {column}={value}: staged data is missing")
        elif action == "delete":
            _remove(target, trash)
        else:
            raise ProcessingError(f"Unknown partition action {action!r}")


def write_journal(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
