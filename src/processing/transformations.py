"""Bronze -> typed Silver columns (PySpark).

Business definitions
* fare_revenue    = fare_amount + extra            (metered fare plus extras)
* tip_amount      = tip (card tips only; cash tips are not recorded by TLC)
* tolls_amount    = tolls (pass-through, not revenue)
* taxes_and_fees  = mta_tax + improvement_surcharge + congestion_surcharge + airport_fee
                    + cbd_congestion_fee (pass-through, not revenue)
* total_amount    = what the passenger paid (all of the above)
Timestamps are naive local wall-clock values and are never shifted.
"""
from __future__ import annotations

from typing import Any

from src.common.config import Settings
from src.processing.schema_validation import (
    COORDINATE_COLUMNS,
    INT_COLUMNS,
    NUMERIC_AMOUNT_COLUMNS,
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    normalize_column_name,
)

RECORD_KEY_COLUMNS = (
    "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
    "passenger_count", "trip_distance", "fare_amount", "total_amount",
)

SILVER_COLUMNS = [
    "record_id", "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
    "rate_code_id", "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    "trip_duration_minutes", "pickup_hour", "pickup_dow", "pickup_month", "pickup_date",
    "source_file", "source_checksum", "source_row_id", "ingested_at", "silver_run_id",
]

QUARANTINE_COLUMNS = [
    "source_file", "source_checksum", "source_row_id", "raw_record", "rejection_reason",
    "run_id", "rejected_at", "pickup_date",
]


def rename_to_canonical(df: Any) -> Any:
    """Normalise every column name (PULocationID -> pu_location_id, _source_file -> source_file)."""
    for name in df.columns:
        canonical = normalize_column_name(name)
        if canonical != name:
            df = df.withColumnRenamed(name, canonical)
    return df


def to_typed(df: Any, settings: Settings, run_id: str) -> Any:
    """Return the typed Silver frame (plus raw_record and corrupt_record helper columns)."""
    from pyspark.sql import functions as F

    original_data_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "raw_record", F.to_json(F.struct(*[F.col(f"`{c}`") for c in original_data_cols])))
    df = rename_to_canonical(df)
    present = set(df.columns)
    for col in (*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS):
        if col not in present and col not in COORDINATE_COLUMNS:
            df = df.withColumn(col, F.lit(None).cast("string"))
    df = df.withColumn("corrupt_record", F.col("corrupt_record").cast("string")) \
        if "corrupt_record" in df.columns else df.withColumn("corrupt_record", F.lit(None).cast("string"))

    def ts(name: str) -> Any:
        parsed = [F.to_timestamp(F.trim(F.col(name)), fmt) for fmt in settings.timestamp_formats]
        return F.coalesce(*parsed)

    df = df.withColumn("pickup_ts", ts("pickup_datetime")).withColumn(
        "dropoff_ts", ts("dropoff_datetime"))
    for col in INT_COLUMNS:
        df = df.withColumn(col, F.trim(F.col(col)).cast("double").cast("int"))
    for col in (*NUMERIC_AMOUNT_COLUMNS, *[c for c in COORDINATE_COLUMNS if c in df.columns]):
        df = df.withColumn(col, F.trim(F.col(col)).cast("double"))
    df = df.withColumn("store_and_fwd_flag", F.upper(F.trim(F.col("store_and_fwd_flag"))))
    df = (
        df.withColumn("trip_duration_minutes",
                      (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0)
        .withColumn("pickup_date", F.date_format("pickup_ts", "yyyy-MM-dd"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        # ISO weekday: Monday=1 ... Sunday=7 (Spark dayofweek: Sunday=1)
        .withColumn("pickup_dow", ((F.dayofweek("pickup_ts") + 5) % 7) + 1)
        .withColumn("source_row_id", F.col("source_row_id").cast("long"))
        .withColumn("silver_run_id", F.lit(run_id))
    )
    key = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in RECORD_KEY_COLUMNS])
    return df.withColumn("record_id", F.sha2(key, 256))
