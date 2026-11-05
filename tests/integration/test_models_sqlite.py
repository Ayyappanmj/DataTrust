"""Runs the exact dbt model SQL (rendered) on SQLite and compares with the independent oracle.

This verifies aggregation logic, revenue definitions and the dbt singular tests without needing
DuckDB. The DuckDB/dbt variants of the same checks live in test_pipeline_end_to_end.py.
"""
import sqlite3
from pathlib import Path

import pytest

from src.sample.generator import generate_month, reference_row
from src.warehouse.warehouse_loader import (
    build_models_sql,
    execution_order,
    model_files,
    render_model,
)
from tests.helpers import typed_rows

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def warehouse():
    lines, expected = generate_month("2023-01", valid_rows=300, per_injection=4, malformed_rows=0)
    header = lines[0].split(",")
    seen, accepted = set(), []
    for line, row in zip(lines[1:], typed_rows(lines), strict=True):
        if reference_row(dict(zip(header, line.split(","), strict=True))).rejected or row["record_id"] in seen:
            continue
        seen.add(row["record_id"])
        accepted.append(row)
    conn = sqlite3.connect(":memory:")
    for schema in ("staging", "intermediate", "gold"):
        conn.execute(f"ATTACH ':memory:' AS {schema}")
    ddl = "\n".join(x for x in (ROOT / "sql" / "warehouse_schema.sql").read_text().splitlines()
                    if not x.startswith("--"))
    conn.executescript(ddl)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(silver_trips)")]
    for row in accepted:
        row = {**row, "source_file": "f.csv", "source_checksum": "abc", "source_row_id": 1,
               "ingested_at": None, "silver_run_id": "r"}
        conn.execute(f"INSERT INTO silver_trips ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                     [row.get(c) for c in cols])
    conn.execute("INSERT INTO quality_results VALUES ('r1','silver','silver.trips','x','d','ERROR','reject',"
                 "10,9,1,10.0,5.0,'FAILED','2026-01-01T00:00:00')")
    conn.execute("INSERT INTO quality_results VALUES ('r2','silver','silver.trips','x','d','ERROR','reject',"
                 "10,10,0,0.0,5.0,'PASSED','2026-01-02T00:00:00')")
    conn.execute("INSERT INTO pipeline_runs VALUES ('r1','datatrust','SUCCEEDED','2026-01-01T00:00:00',"
                 "NULL,1.5,10,9,1,9,NULL)")
    built = build_models_sql(conn, staging_as="table", create_schemas=False)
    return conn, expected, built


def test_all_required_models_are_built_in_dependency_order(warehouse):
    _, _, built = warehouse
    for name in ("stg_trips", "clean_trips", "daily_trip_metrics", "hourly_trip_metrics", "location_metrics",
                 "payment_type_metrics", "data_quality_summary", "pipeline_run_summary"):
        assert name in built
    assert built.index("stg_trips") < built.index("clean_trips") < built.index("daily_trip_metrics")


def test_daily_aggregations_match_independent_expected_values(warehouse):
    conn, expected, _ = warehouse
    got = {r[0]: r[1:] for r in conn.execute(
        "SELECT pickup_date, trips, ROUND(fare_revenue, 2), ROUND(tip_amount, 2) FROM gold.daily_trip_metrics")}
    assert set(got) == set(expected["daily"])
    for day, exp in expected["daily"].items():
        assert got[day] == (exp["trips"], round(exp["fare_revenue"], 2), round(exp["tip_amount"], 2))


def test_revenue_excludes_tolls_taxes_and_tips(warehouse):
    conn, _, _ = warehouse
    bad = conn.execute("SELECT COUNT(*) FROM intermediate.clean_trips "
                       "WHERE ABS(fare_revenue - (fare_amount + COALESCE(extra, 0))) > 0.0001").fetchone()[0]
    assert bad == 0
    total, fare_revenue, tolls, tips = conn.execute(
        "SELECT SUM(total_amount), SUM(fare_revenue), SUM(tolls_amount), SUM(tip_amount) "
        "FROM gold.daily_trip_metrics").fetchone()
    assert fare_revenue < total and tolls > 0 and tips > 0


def test_window_columns_in_daily_metrics(warehouse):
    conn, _, _ = warehouse
    rows = conn.execute("SELECT trips, trips_7d_avg, trips_change_vs_prev_day FROM gold.daily_trip_metrics "
                        "ORDER BY pickup_date LIMIT 2").fetchall()
    assert rows[0][2] is None and rows[1][2] == rows[1][0] - rows[0][0]
    assert rows[0][1] == rows[0][0] and abs(rows[1][1] - (rows[0][0] + rows[1][0]) / 2) < 1e-9


def test_distribution_models_sum_to_totals(warehouse):
    conn, _, _ = warehouse
    total = conn.execute("SELECT COUNT(*) FROM intermediate.clean_trips").fetchone()[0]
    assert conn.execute("SELECT SUM(trips) FROM gold.payment_type_metrics").fetchone()[0] == total
    assert conn.execute("SELECT SUM(trips) FROM gold.location_metrics").fetchone()[0] == total
    assert conn.execute("SELECT SUM(trips) FROM gold.hourly_trip_metrics").fetchone()[0] == total
    assert abs(conn.execute("SELECT SUM(share_pct) FROM gold.payment_type_metrics").fetchone()[0] - 100) < 1e-6


def test_tip_percentage_is_defined_for_card_trips_only(warehouse):
    conn, _, _ = warehouse
    assert conn.execute("SELECT COUNT(*) FROM intermediate.clean_trips WHERE payment_type <> 1 "
                        "AND tip_pct IS NOT NULL").fetchone()[0] == 0
    assert conn.execute("SELECT tip_pct FROM gold.payment_type_metrics WHERE payment_type_id = 2").fetchone()[0] is None


def test_quality_summary_keeps_only_latest_result_per_rule(warehouse):
    conn, _, _ = warehouse
    row = conn.execute("SELECT latest_status, runs_evaluated, failed_runs, max_failure_pct "
                       "FROM gold.data_quality_summary").fetchall()
    assert row == [("PASSED", 2, 1, 10.0)]


def test_run_summary_computes_rejection_pct(warehouse):
    conn, _, _ = warehouse
    assert conn.execute("SELECT rejection_pct, run_date FROM gold.pipeline_run_summary").fetchone() == (10.0, "2026-01-01")


def test_dbt_singular_tests_return_no_failing_rows(warehouse):
    conn, _, _ = warehouse
    models = model_files()
    tests = sorted((ROOT / "dbt_project" / "tests").glob("*.sql"))
    assert len(tests) >= 3
    for test in tests:
        assert conn.execute(render_model(test.read_text(), models)).fetchall() == [], test.name


def test_models_reference_only_known_models_and_have_no_cycles():
    models = model_files()
    assert len(execution_order(models)) == len(models) == 10
