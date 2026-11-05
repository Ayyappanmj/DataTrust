"""Partitioning benchmark (DuckDB). Measures real timings; nothing is hard-coded.

It generates a synthetic trips table, writes it to Parquet under several layouts and times
three query shapes plus the number of Parquet files each query actually touches.
Results are machine- and run-specific: they show *relative* behaviour on this laptop only.
"""
from __future__ import annotations

import json
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from src.common.config import Settings
from src.common.exceptions import WarehouseError

LAYOUTS = {
    "unpartitioned": None,
    "by_month": "pickup_month",
    "by_day": "pickup_date",
    "by_day_and_hour": ("pickup_date", "pickup_hour"),
}


def _timed(fn, repeats: int) -> tuple[float, float]:
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples), min(samples)


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*.parquet"))


def run_benchmark(settings: Settings, rows: int = 200_000, days: int = 60, repeats: int = 3) -> dict:
    try:
        import duckdb
    except ImportError as exc:
        raise WarehouseError("duckdb is required for the benchmark") from exc
    out_dir = settings.reports_dir / "benchmarks"
    work = out_dir / "_work"
    if work.exists():
        import shutil

        shutil.rmtree(work)
    work.mkdir(parents=True)
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE TABLE trips AS
        SELECT *,
               strftime(pickup_ts, '%Y-%m-%d') AS pickup_date,
               strftime(pickup_ts, '%Y-%m') AS pickup_month,
               CAST(hour(pickup_ts) AS INTEGER) AS pickup_hour
        FROM (
            SELECT
                i AS trip_id,
                TIMESTAMP '2023-01-01 00:00:00'
                    + INTERVAL 1 SECOND * CAST((i * 7919) % ({days} * 86400) AS BIGINT) AS pickup_ts,
                CAST(1 + (i * 31) % 260 AS INTEGER) AS pu_location_id,
                ROUND(3 + ((i * 13) % 5000) / 100.0, 2) AS fare_amount
            FROM range({rows}) t(i)
        )""")
    probe_day = conn.execute("SELECT strftime(TIMESTAMP '2023-01-01' + INTERVAL 20 DAY, '%Y-%m-%d')").fetchone()[0]
    probe_month = probe_day[:7]
    queries = {
        "one_day_filter": (f"SELECT COUNT(*), SUM(fare_amount) FROM {{src}} WHERE pickup_date = '{probe_day}'",
                           f"pickup_date = '{probe_day}'"),
        "one_month_filter": (f"SELECT pu_location_id, COUNT(*) FROM {{src}} WHERE pickup_month = '{probe_month}' "
                             "GROUP BY 1", f"pickup_month = '{probe_month}'"),
        "full_scan_aggregate": ("SELECT pickup_date, COUNT(*), AVG(fare_amount) FROM {src} GROUP BY 1", None),
    }
    results = []
    for layout, cols in LAYOUTS.items():
        target = work / layout
        start = time.perf_counter()
        if cols is None:
            target.mkdir()
            conn.execute(f"COPY trips TO '{(target / 'data.parquet').as_posix()}' (FORMAT PARQUET)")
        else:
            col_list = ", ".join([cols] if isinstance(cols, str) else cols)
            conn.execute(f"COPY trips TO '{target.as_posix()}' (FORMAT PARQUET, PARTITION_BY ({col_list}))")
        write_seconds = time.perf_counter() - start
        files = sorted(target.rglob("*.parquet"))
        src = f"read_parquet('{(target / '**' / '*.parquet').as_posix()}', hive_partitioning = true)"
        for qname, (sql, predicate) in queries.items():
            median_s, best_s = _timed(lambda s=sql, qsrc=src: conn.execute(s.format(src=qsrc)).fetchall(), repeats)
            if predicate:
                touched = conn.execute(
                    f"SELECT COUNT(DISTINCT filename) FROM read_parquet('{(target / '**' / '*.parquet').as_posix()}', "
                    f"hive_partitioning = true, filename = true) WHERE {predicate}").fetchone()[0]
            else:
                touched = len(files)
            results.append({
                "layout": layout, "query": qname, "median_seconds": round(median_s, 5),
                "best_seconds": round(best_s, 5), "files_total": len(files),
                "files_touched": touched, "write_seconds": round(write_seconds, 4),
                "output_bytes": _dir_size(target),
            })
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": rows, "days": days, "repeats": repeats,
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "duckdb": duckdb.__version__, "processor": platform.processor()},
        "note": "Single-machine, synthetic data, DuckDB only. Not representative of production.",
    }
    payload = {"summary": summary, "results": results}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    (out_dir / f"benchmark_{stamp}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    header = list(results[0])
    csv_lines = [",".join(header)] + [",".join(str(r[h]) for h in header) for r in results]
    (out_dir / f"benchmark_{stamp}.csv").write_text("\n".join(csv_lines) + "\n", encoding="utf-8")
    import shutil

    shutil.rmtree(work, ignore_errors=True)
    return payload
