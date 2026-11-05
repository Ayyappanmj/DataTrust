"""Small constants and dataclasses shared by the metadata layer."""
from __future__ import annotations

from dataclasses import dataclass

RUN_RUNNING = "RUNNING"
RUN_SUCCEEDED = "SUCCEEDED"
RUN_PARTIAL = "PARTIAL_FAILURE"
RUN_FAILED = "FAILED"
SUCCESS_STATES = (RUN_SUCCEEDED,)

STAGE_PENDING = "PENDING"
STAGE_SUCCEEDED = "SUCCEEDED"
STAGE_FAILED = "FAILED"

DATASET_BRONZE = "bronze.trips"
DATASET_SILVER = "silver.trips"
DATASET_QUARANTINE = "quarantine.trips"
GOLD_TABLES = (
    "clean_trips", "daily_trip_metrics", "hourly_trip_metrics", "location_metrics",
    "payment_type_metrics", "data_quality_summary", "pipeline_run_summary",
)


def gold_dataset(table: str) -> str:
    return f"gold.{table}"


@dataclass(frozen=True)
class ManifestRow:
    checksum: str
    file_path: str
    file_name: str
    file_format: str
    size_bytes: int
    ingest_status: str
    process_status: str
    active: bool
    record_count: int | None = None
    schema_fingerprint: str | None = None
    columns_json: str | None = None

    @classmethod
    def from_dict(cls, row: dict) -> ManifestRow:
        return cls(
            checksum=row["checksum"], file_path=row["file_path"], file_name=row["file_name"],
            file_format=row["file_format"], size_bytes=row["size_bytes"],
            ingest_status=row["ingest_status"], process_status=row["process_status"],
            active=bool(row["active"]), record_count=row.get("record_count"),
            schema_fingerprint=row.get("schema_fingerprint"), columns_json=row.get("columns_json"),
        )
