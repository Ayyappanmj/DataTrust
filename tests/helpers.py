"""Test helpers that need only the standard library (SQLite stands in for the SQL engine)."""
from __future__ import annotations

import sqlite3

from src.sample.generator import parse_float, parse_int, parse_ts

TYPED_COLUMNS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance", "rate_code_id",
    "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "trip_duration_minutes",
    "pickup_date", "pickup_month", "pickup_hour", "pickup_dow", "corrupt_record", "record_id",
]


def typed_rows(lines: list[str]) -> list[dict]:
    """Mimic the Spark cast step in plain Python: unparseable values become NULL."""
    header = lines[0].split(",")
    out = []
    for line in lines[1:]:
        parts = line.split(",")
        parts = (parts + [""] * len(header))[: len(header)]
        raw = dict(zip(header, parts, strict=True))
        pickup, dropoff = parse_ts(raw["tpep_pickup_datetime"]), parse_ts(raw["tpep_dropoff_datetime"])
        duration = (dropoff - pickup).total_seconds() / 60.0 if pickup and dropoff else None
        row = {
            "vendor_id": parse_int(raw["VendorID"]),
            "pickup_ts": pickup.strftime("%Y-%m-%d %H:%M:%S") if pickup else None,
            "dropoff_ts": dropoff.strftime("%Y-%m-%d %H:%M:%S") if dropoff else None,
            "passenger_count": parse_int(raw["passenger_count"]),
            "trip_distance": parse_float(raw["trip_distance"]),
            "rate_code_id": parse_int(raw["RatecodeID"]),
            "store_and_fwd_flag": raw["store_and_fwd_flag"] or None,
            "pu_location_id": parse_int(raw["PULocationID"]),
            "do_location_id": parse_int(raw["DOLocationID"]),
            "payment_type": parse_int(raw["payment_type"]),
            "cbd_congestion_fee": None,
            "trip_duration_minutes": duration,
            "pickup_date": pickup.strftime("%Y-%m-%d") if pickup else None,
            "pickup_month": pickup.strftime("%Y-%m") if pickup else None,
            "pickup_hour": pickup.hour if pickup else None,
            "pickup_dow": pickup.isoweekday() if pickup else None,
            "corrupt_record": None,
        }
        for col in ("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"):
            row[col] = parse_float(raw[col])
        row["record_id"] = "|".join(str(row[c]) for c in (
            "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
            "passenger_count", "trip_distance", "fare_amount", "total_amount"))
        out.append(row)
    return out


def load_typed_table(conn: sqlite3.Connection, table: str, rows: list[dict]) -> None:
    cols = ", ".join(TYPED_COLUMNS)
    conn.execute(f"CREATE TABLE {table} ({cols})")
    marks = ", ".join("?" for _ in TYPED_COLUMNS)
    conn.executemany(
        f"INSERT INTO {table} VALUES ({marks})",
        [tuple(r.get(c) for c in TYPED_COLUMNS) for r in rows],
    )
