"""Column-name normalisation and schema policy checks (pure Python, no Spark needed)."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from src.common.exceptions import SchemaValidationError

# Canonical (snake_case) name for each known TLC source column (after lower/underscore cleaning).
ALIASES = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "corrupt_record": "corrupt_record",
}

REQUIRED_COLUMNS = (
    "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
    "fare_amount", "total_amount",
)
OPTIONAL_COLUMNS = (
    "vendor_id", "passenger_count", "trip_distance", "rate_code_id", "store_and_fwd_flag",
    "payment_type", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "trip_type", "ehail_fee",
    "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
)
NUMERIC_AMOUNT_COLUMNS = (
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee",
)
INT_COLUMNS = (
    "vendor_id", "passenger_count", "rate_code_id", "pu_location_id", "do_location_id",
    "payment_type",
)
COORDINATE_COLUMNS = (
    "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
)
METADATA_COLUMNS = ("source_file", "source_checksum", "ingested_at", "source_row_id",
                    "corrupt_record")


def normalize_column_name(name: str) -> str:
    """'PULocationID' -> 'pu_location_id', 'tpep_pickup_datetime' -> 'pickup_datetime'."""
    cleaned = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return ALIASES.get(cleaned, cleaned)


def normalize_columns(columns: list[str]) -> dict[str, str]:
    """Map each source column to its canonical name; reject collisions."""
    mapping = {col: normalize_column_name(col) for col in columns}
    seen: dict[str, str] = {}
    for original, canonical in mapping.items():
        if canonical in seen:
            raise SchemaValidationError(
                f"Columns {seen[canonical]!r} and {original!r} both normalise to {canonical!r}"
            )
        seen[canonical] = original
    return mapping


def schema_fingerprint(columns: list[str]) -> str:
    """Order-independent fingerprint of the canonical column set."""
    canonical = sorted(normalize_column_name(c) for c in columns)
    return hashlib.sha256("|".join(canonical).encode()).hexdigest()[:16]


@dataclass
class SchemaCheckResult:
    status: str  # PASSED | WARN | FAILED
    missing_required: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)
    missing_optional: list[str] = field(default_factory=list)
    drift_from_previous: bool = False
    messages: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status != "FAILED"


def check_schema(
    columns: list[str],
    unexpected_policy: str = "warn",
    previous_fingerprint: str | None = None,
) -> SchemaCheckResult:
    """Apply the schema policy to a file's column list.

    * Missing required columns always FAIL.
    * Unexpected columns follow ``unexpected_policy``: warn | fail | ignore.
    * A fingerprint different from the previous file's is reported as drift (a warning).
    """
    canonical = set(normalize_columns(columns).values()) - set(METADATA_COLUMNS)
    known = set(REQUIRED_COLUMNS) | set(OPTIONAL_COLUMNS)
    result = SchemaCheckResult(status="PASSED")
    result.missing_required = sorted(set(REQUIRED_COLUMNS) - canonical)
    result.unexpected = sorted(canonical - known)
    result.missing_optional = sorted(set(OPTIONAL_COLUMNS) - canonical)
    if previous_fingerprint and previous_fingerprint != schema_fingerprint(columns):
        result.drift_from_previous = True
        result.messages.append("Column set differs from the previously ingested file")
        result.status = "WARN"
    if result.missing_required:
        result.status = "FAILED"
        result.messages.append(f"Missing required columns: {result.missing_required}")
    if result.unexpected and unexpected_policy != "ignore":
        result.messages.append(f"Unexpected columns: {result.unexpected}")
        if unexpected_policy == "fail":
            result.status = "FAILED"
        elif result.status == "PASSED":
            result.status = "WARN"
    return result
