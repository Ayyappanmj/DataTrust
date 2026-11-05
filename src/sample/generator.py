"""Deterministic synthetic NYC-taxi-style data plus an independent Python 'oracle'.

The oracle re-implements every quality rule in plain Python so tests can compare the
SQL/Spark rule engines against separately computed expected values.
"""
from __future__ import annotations

import json
import random
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]
TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p")
LOCATIONS = [4, 12, 13, 24, 41, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88, 90, 100, 107, 113, 114,
             125, 132, 137, 138, 140, 141, 142, 143, 144, 148, 151, 161, 162, 163, 164, 170,
             186, 209, 230, 231, 233, 234, 236, 237, 238, 239, 246, 249]
INJECTIONS = (
    "neg_fare", "dropoff_before_pickup", "zero_passengers", "missing_pickup", "bad_timestamp",
    "long_trip", "neg_distance", "missing_pu_location", "duplicate", "unknown_payment",
    "total_mismatch", "malformed_short_row",
)


# ----------------------------- reference parsing ---------------------------------
def parse_ts(value: str | None) -> datetime | None:
    if value is None or not value.strip():
        return None
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_float(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        return float(value.strip())
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    number = parse_float(value)
    return None if number is None else int(number)


@dataclass
class RefRow:
    """A source row parsed independently of Spark/SQL, with the rules it violates."""

    fields: dict
    pickup_ts: datetime | None = None
    dropoff_ts: datetime | None = None
    duration_min: float | None = None
    failures: set = field(default_factory=set)

    @property
    def rejected(self) -> bool:
        return bool(self.failures & REJECT_RULES)

    @property
    def record_key(self) -> tuple:
        """Typed business key; must match `record_id` in processing/transformations.py."""
        f = self.fields
        return (parse_int(f.get("VendorID")), self.pickup_ts, self.dropoff_ts,
                parse_int(f.get("PULocationID")), parse_int(f.get("DOLocationID")),
                parse_int(f.get("passenger_count")), parse_float(f.get("trip_distance")),
                parse_float(f.get("fare_amount")), parse_float(f.get("total_amount")))


REJECT_RULES = {
    "pickup_ts_valid", "dropoff_ts_valid", "pickup_ts_in_range", "pickup_before_dropoff",
    "trip_duration_valid", "pu_location_present", "do_location_present", "fare_amount_present",
    "total_amount_present", "fare_amount_non_negative", "tip_amount_non_negative",
    "tolls_amount_non_negative", "total_amount_non_negative", "passenger_count_valid",
    "trip_distance_valid",
}


def reference_row(fields: dict) -> RefRow:
    """Evaluate the quality rules in plain Python (independent of the SQL expressions)."""
    row = RefRow(fields=fields)
    row.pickup_ts = parse_ts(fields.get("tpep_pickup_datetime"))
    row.dropoff_ts = parse_ts(fields.get("tpep_dropoff_datetime"))
    num = {k: parse_float(fields.get(k)) for k in COLUMNS if k not in
           ("tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag")}
    fail = row.failures
    if row.pickup_ts is None:
        fail.add("pickup_ts_valid")
    if row.dropoff_ts is None:
        fail.add("dropoff_ts_valid")
    if row.pickup_ts is not None and not (datetime(2009, 1, 1) <= row.pickup_ts < datetime(2100, 1, 1)):
        fail.add("pickup_ts_in_range")
    if row.pickup_ts is not None and row.dropoff_ts is not None:
        row.duration_min = (row.dropoff_ts - row.pickup_ts).total_seconds() / 60.0
        if row.pickup_ts > row.dropoff_ts:
            fail.add("pickup_before_dropoff")
        if not (0 < row.duration_min <= 1440):
            fail.add("trip_duration_valid")
    for rule, col in (("pu_location_present", "PULocationID"), ("do_location_present", "DOLocationID"),
                      ("fare_amount_present", "fare_amount"), ("total_amount_present", "total_amount")):
        if num[col] is None:
            fail.add(rule)
    for rule, col in (("fare_amount_non_negative", "fare_amount"), ("tip_amount_non_negative", "tip_amount"),
                      ("tolls_amount_non_negative", "tolls_amount"), ("total_amount_non_negative", "total_amount")):
        if num[col] is not None and num[col] < 0:
            fail.add(rule)
    passengers = parse_int(fields.get("passenger_count"))
    if passengers is not None and not (1 <= passengers <= 9):
        fail.add("passenger_count_valid")
    if num["trip_distance"] is not None and not (0 <= num["trip_distance"] <= 1000):
        fail.add("trip_distance_valid")
    payment = parse_int(fields.get("payment_type"))
    if payment is not None and payment not in range(0, 7):
        fail.add("payment_type_known")
    if num["total_amount"] is not None and num["fare_amount"] is not None:
        parts = [num[c] or 0.0 for c in ("extra", "mta_tax", "tip_amount", "tolls_amount",
                                         "improvement_surcharge", "congestion_surcharge", "airport_fee")]
        if abs(num["total_amount"] - (num["fare_amount"] + sum(parts))) > 0.05:
            fail.add("total_reconciles")
    return row


# ----------------------------- generation ----------------------------------------
def _fmt(x: float) -> str:
    return f"{x:.2f}"


def _valid_row(rng: random.Random, year: int, month: int) -> dict:
    last_day = monthrange(year, month)[1]
    start = datetime(year, month, 1) + timedelta(
        days=rng.randrange(last_day), hours=rng.randrange(24), minutes=rng.randrange(60),
        seconds=rng.randrange(60))
    duration = rng.randint(3, 75)
    end = start + timedelta(minutes=duration, seconds=rng.randrange(60))
    dist = round(duration * rng.uniform(0.15, 0.45), 2)
    fare = round(3.0 + 2.6 * dist + 0.4 * duration * 0.3, 2)
    extra = rng.choice([0.0, 0.5, 1.0, 2.5])
    mta = 0.5
    payment = rng.choices([1, 2, 3, 4], weights=[70, 27, 2, 1])[0]
    tip = round(fare * rng.choice([0.10, 0.15, 0.20, 0.25]), 2) if payment == 1 else 0.0
    tolls = rng.choice([0.0, 0.0, 0.0, 6.55])
    improvement = 1.0
    congestion = 2.5
    pu, do = rng.choice(LOCATIONS), rng.choice(LOCATIONS)
    airport = 1.25 if 132 in (pu, do) or 138 in (pu, do) else 0.0
    total = round(fare + extra + mta + tip + tolls + improvement + congestion + airport, 2)
    passengers = rng.randint(1, 4)
    fmt_ts = "%Y-%m-%d %H:%M:%S"
    return {
        "VendorID": str(rng.choice([1, 2])),
        "tpep_pickup_datetime": start.strftime(fmt_ts),
        "tpep_dropoff_datetime": end.strftime(fmt_ts),
        "passenger_count": f"{passengers}.0" if rng.random() < 0.1 else str(passengers),
        "trip_distance": _fmt(dist), "RatecodeID": "1", "store_and_fwd_flag": "N",
        "PULocationID": str(pu), "DOLocationID": str(do), "payment_type": str(payment),
        "fare_amount": _fmt(fare), "extra": _fmt(extra), "mta_tax": _fmt(mta),
        "tip_amount": _fmt(tip), "tolls_amount": _fmt(tolls),
        "improvement_surcharge": _fmt(improvement), "total_amount": _fmt(total),
        "congestion_surcharge": _fmt(congestion), "airport_fee": _fmt(airport),
    }


def _inject(kind: str, row: dict, rng: random.Random) -> dict:
    row = dict(row)
    if kind == "neg_fare":
        row["fare_amount"] = _fmt(-abs(float(row["fare_amount"])))
    elif kind == "dropoff_before_pickup":
        row["tpep_pickup_datetime"], row["tpep_dropoff_datetime"] = (
            row["tpep_dropoff_datetime"], row["tpep_pickup_datetime"])
    elif kind == "zero_passengers":
        row["passenger_count"] = "0"
    elif kind == "missing_pickup":
        row["tpep_pickup_datetime"] = ""
    elif kind == "bad_timestamp":
        row["tpep_pickup_datetime"] = "not-a-date"
    elif kind == "long_trip":
        end = parse_ts(row["tpep_pickup_datetime"]) + timedelta(days=2, minutes=rng.randint(1, 50))
        row["tpep_dropoff_datetime"] = end.strftime("%Y-%m-%d %H:%M:%S")
    elif kind == "neg_distance":
        row["trip_distance"] = _fmt(-1.0 - rng.random())
    elif kind == "missing_pu_location":
        row["PULocationID"] = ""
    elif kind == "unknown_payment":
        row["payment_type"] = "9"
    elif kind == "total_mismatch":
        row["total_amount"] = _fmt(float(row["total_amount"]) + 3.0)
    return row


def generate_month(month: str, valid_rows: int = 1000, seed: int | None = None,
                   per_injection: int = 10, malformed_rows: int = 2) -> tuple[list[str], dict]:
    """Return (csv_lines, expected_summary) for a month such as '2023-01'.

    ``seed`` defaults to a value derived from the month so every month is stable and
    independent: re-generating never changes bytes of previously generated months.
    """
    year, mon = (int(x) for x in month.split("-"))
    rng = random.Random(seed if seed is not None else year * 100 + mon)
    base = [_valid_row(rng, year, mon) for _ in range(valid_rows)]
    rows: list[dict] = list(base)
    raw_lines: list[str] = []
    row_injection: list[str] = ["valid"] * len(base)
    simple = [k for k in INJECTIONS if k not in ("duplicate", "malformed_short_row")]
    for kind in simple:
        for _ in range(per_injection):
            rows.append(_inject(kind, rng.choice(base), rng))
            row_injection.append(kind)
    for _ in range(per_injection):
        rows.append(dict(rng.choice(base)))
        row_injection.append("duplicate")
    order = list(range(len(rows)))
    rng.shuffle(order)
    lines = [",".join(COLUMNS)]
    ordered_rows = []
    for idx in order:
        lines.append(",".join(rows[idx][c] for c in COLUMNS))
        ordered_rows.append(rows[idx])
    for _ in range(malformed_rows):
        raw_lines.append("1,2023-01-05 10:00:00,2023-01-05")  # truncated line
    for raw in raw_lines:
        lines.insert(rng.randrange(1, len(lines)), raw)
    return lines, summarize(lines)


def summarize(lines: list[str]) -> dict:
    """Independent expected results for a CSV (list of lines incl. header)."""
    header = lines[0].split(",")
    parsed: list[RefRow] = []
    malformed = 0
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != len(header):
            malformed += 1
            parts = (parts + [""] * len(header))[: len(header)]
        parsed.append(reference_row(dict(zip(header, parts, strict=True))))
    rule_failures: dict[str, int] = defaultdict(int)
    for row in parsed:
        for rule in row.failures:
            rule_failures[rule] += 1
    seen: set = set()
    accepted: list[RefRow] = []
    duplicates = 0
    for row in parsed:
        if row.rejected:
            continue
        if row.record_key in seen:
            duplicates += 1
            continue
        seen.add(row.record_key)
        accepted.append(row)
    daily: dict[str, dict] = defaultdict(lambda: {"trips": 0, "fare_revenue": 0.0, "tip_amount": 0.0})
    for row in accepted:
        day = row.pickup_ts.strftime("%Y-%m-%d")
        daily[day]["trips"] += 1
        daily[day]["fare_revenue"] += float(row.fields["fare_amount"]) + float(row.fields["extra"] or 0)
        daily[day]["tip_amount"] += float(row.fields["tip_amount"] or 0)
    return {
        "rows_read": len(parsed),
        "malformed_lines": malformed,
        "rejected_validity": sum(1 for r in parsed if r.rejected),
        "duplicates": duplicates,
        "accepted": len(accepted),
        "rule_failures": dict(rule_failures),
        "daily": {d: {k: round(v, 2) if isinstance(v, float) else v for k, v in m.items()}
                  for d, m in sorted(daily.items())},
    }


def write_month(out_dir: Path, month: str, valid_rows: int = 1000, **kwargs) -> Path:
    """Write ``yellow_tripdata_<month>_sample.csv`` and its ground-truth JSON."""
    lines, expected = generate_month(month, valid_rows=valid_rows, **kwargs)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"yellow_tripdata_{month}_sample.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    expected_dir = out_dir / "_expected"
    expected_dir.mkdir(exist_ok=True)
    (expected_dir / f"{month}.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    return path


def generate_samples(out_dir: Path, months: list[str], valid_rows: int = 1000,
                     as_parquet: bool = False, **kwargs) -> list[Path]:
    """Generate one sample file per month (CSV, or Parquet when ``as_parquet``)."""
    written = []
    for month in months:
        csv_path = write_month(out_dir, month, valid_rows=valid_rows, **kwargs)
        if as_parquet:
            import pandas as pd

            frame = pd.read_csv(csv_path, dtype=str, on_bad_lines="skip")
            parquet_path = csv_path.with_suffix(".parquet")
            frame.to_parquet(parquet_path, index=False)
            csv_path.unlink()
            csv_path = parquet_path
        written.append(csv_path)
    return written
