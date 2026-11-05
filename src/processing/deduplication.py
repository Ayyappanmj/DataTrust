"""Duplicate detection. A duplicate = same business key (see RECORD_KEY_COLUMNS).

TLC data has no trip id, so two genuinely different trips that match on every key column would
be treated as one. This is a documented limitation.
"""
from __future__ import annotations

from typing import Any


def split_in_batch_duplicates(valid: Any) -> tuple[Any, Any]:
    """Return (kept, duplicates); the earliest (checksum, row id) of each record_id is kept."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("record_id").orderBy("source_checksum", "source_row_id")
    ranked = valid.withColumn("_rn", F.row_number().over(window))
    return ranked.filter("_rn = 1").drop("_rn"), ranked.filter("_rn > 1").drop("_rn")


def split_against_existing(fresh: Any, existing: Any | None) -> tuple[Any, Any | None]:
    """Return (new_rows, duplicates_of_existing)."""
    if existing is None:
        return fresh, None
    ids = existing.select("record_id").distinct()
    return fresh.join(ids, "record_id", "left_anti"), fresh.join(ids, "record_id", "left_semi")
