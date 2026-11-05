"""Dataset-level lineage recording and querying.

Implemented: which source files, checksums and runs fed each dataset (file -> bronze ->
silver -> gold tables). NOT implemented: column-level lineage.
"""
from __future__ import annotations

from typing import Any

from src.metadata.models import DATASET_BRONZE, DATASET_SILVER, GOLD_TABLES, gold_dataset


def record_ingest(repo: Any, run_id: str, path: str, checksum: str, records: int,
                  status: str = "SUCCEEDED") -> None:
    repo.add_lineage_edge(run_id, f"file:{path}", "csv_parquet_to_bronze", DATASET_BRONZE,
                          checksum, records, status)


def record_silver(repo: Any, run_id: str, checksum: str, accepted: int,
                  status: str = "SUCCEEDED") -> None:
    repo.add_lineage_edge(run_id, DATASET_BRONZE, "bronze_to_silver_clean_dedupe", DATASET_SILVER,
                          checksum, accepted, status)


def record_gold(repo: Any, run_id: str, counts: dict[str, int], status: str = "SUCCEEDED") -> None:
    for table in GOLD_TABLES:
        repo.add_lineage_edge(run_id, DATASET_SILVER, "duckdb_dbt_models", gold_dataset(table),
                              None, counts.get(table), status)


def contributing_sources(repo: Any, dataset: str) -> list[dict]:
    """Answer: which source files and processing runs contributed to this dataset?"""
    return repo.contributing_files(dataset)
