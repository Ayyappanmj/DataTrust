"""End-to-end acceptance tests. Need PySpark + Java + DuckDB (they skip otherwise)."""
import json
import os
import shutil
from pathlib import Path

import pytest

from src.common.exceptions import InjectedFault
from src.metadata.models import DATASET_SILVER
from src.orchestration.pipeline_runner import PipelineRunner
from src.storage.partition_manager import list_partitions

pytest.importorskip("pyspark")
duckdb = pytest.importorskip("duckdb")


def make_runner(settings, spark, repo=None):
    runner = PipelineRunner(settings, repo)
    runner._spark = spark  # share the session-scoped Spark session
    return runner


def totals(sample_dir):
    expected = [json.loads(p.read_text()) for p in sorted((Path(sample_dir) / "_expected").glob("*.json"))]
    out = {k: sum(e[k] for e in expected) for k in ("rows_read", "accepted", "rejected_validity", "duplicates")}
    out["rule_failures"] = {}
    out["daily"] = {}
    for e in expected:
        for rule, n in e["rule_failures"].items():
            out["rule_failures"][rule] = out["rule_failures"].get(rule, 0) + n
        out["daily"].update(e["daily"])
    return out


def sql(settings, query, params=None):
    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return conn.execute(query, params or []).fetchall()
    finally:
        conn.close()


def quarantine_rows(settings):
    conn = duckdb.connect(":memory:")
    glob = (settings.quarantine_dir / "**" / "*.parquet").as_posix()
    return conn.execute("SELECT rejection_reason, source_checksum FROM read_parquet(?, hive_partitioning=true)",
                        [glob]).fetchall()


@pytest.fixture
def runner(settings, spark_session):
    r = make_runner(settings, spark_session)
    yield r
    r.repo.close()


def run_ok(runner, sample_dir, **kw):
    ctx = runner.run(sample_dir, **kw)
    assert runner.repo.get_run(ctx.run_id)["status"] == "SUCCEEDED", runner.repo.get_run(ctx.run_id)
    return ctx


# 1-6: core behaviour ---------------------------------------------------------------------------
def test_full_run_matches_independent_expected_values(runner, settings, sample_dir):
    exp = totals(sample_dir)
    ctx = run_ok(runner, sample_dir)
    assert ctx.counts["records_read"] == exp["rows_read"]
    assert ctx.counts["records_accepted"] == exp["accepted"]
    assert ctx.counts["records_rejected"] == exp["rejected_validity"] + exp["duplicates"]
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0] == exp["accepted"]
    daily = {str(r[0]): (r[1], round(r[2], 2)) for r in sql(
        settings, "SELECT pickup_date, trips, fare_revenue FROM gold.daily_trip_metrics")}
    assert daily == {d: (v["trips"], round(v["fare_revenue"], 2)) for d, v in exp["daily"].items()}


def test_quality_report_failed_counts_are_correct(runner, settings, sample_dir):
    exp = totals(sample_dir)
    ctx = run_ok(runner, sample_dir)
    results = {r["rule_name"]: r for r in runner.repo.list_quality_results(ctx.run_id) if r["stage"] == "silver"}
    for rule, failed in exp["rule_failures"].items():
        assert results[rule]["records_failed"] == failed, rule
    report = json.loads(next((settings.reports_dir / "quality").glob(f"{ctx.run_id}__silver.json")).read_text())
    assert {r["rule_name"] for r in report["rules"]} >= set(exp["rule_failures"])


def test_invalid_rows_are_quarantined_with_a_reason(runner, settings, sample_dir):
    exp = totals(sample_dir)
    run_ok(runner, sample_dir)
    rows = quarantine_rows(settings)
    assert len(rows) == exp["rejected_validity"] + exp["duplicates"]
    assert all(reason for reason, _ in rows)
    reasons = {part for reason, _ in rows for part in reason.split(";")}
    assert {"fare_amount_non_negative", "passenger_count_valid", "duplicate_record"} <= reasons


def test_bronze_preserves_all_raw_records_and_original_files(runner, settings, sample_dir):
    before = {p.name: p.read_bytes() for p in Path(sample_dir).glob("*.csv")}
    exp = totals(sample_dir)
    run_ok(runner, sample_dir)
    conn = duckdb.connect(":memory:")
    n = conn.execute("SELECT COUNT(*) FROM read_parquet(?)",
                     [(settings.bronze_dir / "**" / "*.parquet").as_posix()]).fetchone()[0]
    assert n == exp["rows_read"]
    assert {p.name: p.read_bytes() for p in Path(sample_dir).glob("*.csv")} == before


def test_reprocessing_unchanged_files_does_not_duplicate_results(runner, settings, sample_dir):
    run_ok(runner, sample_dir)
    silver_before = sql(settings, "SELECT COUNT(*), SUM(fare_amount) FROM silver_trips")
    gold_before = sql(settings, "SELECT SUM(trips) FROM gold.daily_trip_metrics")
    ctx = run_ok(runner, sample_dir)
    assert {e.action for e in ctx.entries()} == {"UNCHANGED"}
    assert sql(settings, "SELECT COUNT(*), SUM(fare_amount) FROM silver_trips") == silver_before
    assert sql(settings, "SELECT SUM(trips) FROM gold.daily_trip_metrics") == gold_before
    assert len(runner.repo.list_runs()) == 2


def test_forced_reprocessing_is_idempotent(runner, settings, sample_dir):
    exp = totals(sample_dir)
    run_ok(runner, sample_dir)
    ctx = run_ok(runner, sample_dir, force=True)
    assert {e.action for e in ctx.entries()} == {"FORCE"}
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0] == exp["accepted"]
    assert len(quarantine_rows(settings)) == exp["rejected_validity"] + exp["duplicates"]


def test_new_file_is_detected_and_only_it_is_processed(runner, settings, sample_dir):
    run_ok(runner, sample_dir)
    from src.sample.generator import generate_samples

    generate_samples(sample_dir, ["2023-03"], valid_rows=300, per_injection=4, malformed_rows=0)
    ctx = run_ok(runner, sample_dir)
    actions = {Path(e.path).name: e.action for e in ctx.entries()}
    assert actions["yellow_tripdata_2023-03_sample.csv"] == "NEW"
    assert {a for n, a in actions.items() if "03" not in n} == {"UNCHANGED"}
    assert ctx.counts["records_read"] == totals(sample_dir)["rows_read"] - sum(
        json.loads(p.read_text())["rows_read"] for p in (Path(sample_dir) / "_expected").glob("2023-0[12].json"))


def test_changed_file_replaces_its_previous_rows(runner, settings, sample_dir):
    run_ok(runner, sample_dir)
    jan = Path(sample_dir) / "yellow_tripdata_2023-01_sample.csv"
    lines = jan.read_text().splitlines()
    jan.write_text("\n".join(lines[:101]) + "\n")  # keep header + first 100 rows
    ctx = run_ok(runner, sample_dir)
    assert "CHANGED" in {e.action for e in ctx.entries()}
    jan_rows = sql(settings, "SELECT COUNT(*) FROM silver_trips WHERE pickup_month = '2023-01'")[0][0]
    assert 0 < jan_rows <= 100
    active = [m for m in runner.repo.list_manifest() if m["file_name"] == jan.name and m["active"]]
    assert len(active) == 1


def test_duplicate_source_file_is_skipped(runner, settings, sample_dir):
    run_ok(runner, sample_dir)
    before = sql(settings, "SELECT COUNT(*) FROM silver_trips")
    shutil.copy(Path(sample_dir) / "yellow_tripdata_2023-01_sample.csv", Path(sample_dir) / "copy.csv")
    ctx = run_ok(runner, sample_dir)
    assert "DUPLICATE_CONTENT" in {e.action for e in ctx.entries()}
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips") == before


# 3: interruption and recovery --------------------------------------------------------------------
@pytest.mark.parametrize("fault", ["quality_rules", "commit_mid", "load_duckdb", "after_models"])
def test_interrupted_run_is_failed_then_safely_rerun(settings, spark_session, sample_dir, fault):
    exp = totals(sample_dir)
    broken = make_runner(settings.with_overrides(fault_injection=f"fail_at={fault}"), spark_session)
    with pytest.raises(InjectedFault):
        broken.run(sample_dir)
    run = broken.repo.list_runs()[0]
    assert run["status"] == "FAILED" and "Injected fault" in run["error_summary"]
    assert broken.repo.list_errors(run["run_id"])
    manifest = broken.repo.list_manifest()
    if fault in ("quality_rules", "commit_mid"):
        assert all(m["process_status"] != "SUCCEEDED" for m in manifest)  # never falsely successful
    broken.repo.close()

    healthy = make_runner(settings, spark_session)
    ctx = healthy.run(sample_dir)
    assert healthy.repo.get_run(ctx.run_id)["status"] == "SUCCEEDED"
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0] == exp["accepted"]
    assert len(quarantine_rows(settings)) == exp["rejected_validity"] + exp["duplicates"]
    healthy.repo.close()


def test_failed_gate_leaves_silver_untouched(settings, spark_session, sample_dir, tmp_path):
    strict = tmp_path / "strict.yaml"
    strict.write_text("rules:\n  rejection_rate_acceptable:\n    max_failure_pct: 1\n")
    runner = make_runner(settings.with_overrides(quality_config=strict, max_rejection_rate_pct=1.0), spark_session)
    from src.common.exceptions import QualityGateError

    with pytest.raises(QualityGateError):
        runner.run(sample_dir)
    assert not list_partitions(settings.silver_dir, "pickup_date")
    assert all(m["process_status"] == "FAILED" for m in runner.repo.list_manifest())
    assert runner.repo.get_run(runner.repo.list_runs()[0]["run_id"])["status"] == "FAILED"
    runner.repo.close()


# 4: backfill -------------------------------------------------------------------------------------
def test_backfill_rebuilds_only_the_requested_range(runner, settings, sample_dir):
    exp = totals(sample_dir)
    run_ok(runner, sample_dir)
    parts_before = list_partitions(settings.silver_dir, "pickup_date")
    snapshot = {v: sorted(f.name for f in p.path.iterdir()) for v, p in parts_before.items()}
    ctx = runner.backfill("2023-01-10", "2023-01-12")
    assert runner.repo.get_run(ctx.run_id)["status"] == "SUCCEEDED"
    parts_after = list_partitions(settings.silver_dir, "pickup_date")
    assert set(parts_after) == set(parts_before)
    for value, part in parts_after.items():
        names = sorted(f.name for f in part.path.iterdir())
        if "2023-01-10" <= value <= "2023-01-12":
            assert names != snapshot[value]  # rewritten
        else:
            assert names == snapshot[value]  # untouched
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0] == exp["accepted"]
    daily = {str(r[0]): r[1] for r in sql(settings, "SELECT pickup_date, trips FROM gold.daily_trip_metrics")}
    assert daily == {d: v["trips"] for d, v in exp["daily"].items()}
    assert runner.repo.list_backfills()[0]["status"] == "SUCCEEDED"


def test_backfill_with_nothing_processed_fails_clearly(runner):
    from src.common.exceptions import ProcessingError

    with pytest.raises(ProcessingError, match="Nothing to backfill"):
        runner.backfill("2023-01-01", "2023-01-02")


# 7 and failure scenarios -------------------------------------------------------------------------
def _append_column(path: Path, name: str) -> None:
    lines = path.read_text().splitlines()
    path.write_text("\n".join([lines[0] + f",{name}"] + [ln + ",x" for ln in lines[1:]]) + "\n")


def test_unexpected_column_warns_by_default(runner, settings, sample_dir):
    _append_column(Path(sample_dir) / "yellow_tripdata_2023-02_sample.csv", "surprise_col")
    ctx = run_ok(runner, sample_dir)
    res = {r["rule_name"]: r for r in runner.repo.list_quality_results(ctx.run_id) if r["stage"] == "ingestion"}
    assert res["unexpected_columns"]["records_failed"] == 1 and res["schema_drift"]["records_failed"] >= 1


def test_unexpected_column_can_fail_the_file(settings, spark_session, sample_dir):
    _append_column(Path(sample_dir) / "yellow_tripdata_2023-02_sample.csv", "surprise_col")
    runner = make_runner(settings.with_overrides(unexpected_columns="fail"), spark_session)
    ctx = runner.run(sample_dir)
    assert runner.repo.get_run(ctx.run_id)["status"] == "PARTIAL_FAILURE"
    assert len(ctx.failed) == 1
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips WHERE pickup_month = '2023-02'")[0][0] == 0
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips WHERE pickup_month = '2023-01'")[0][0] > 0
    runner.repo.close()


def test_missing_required_column_fails_that_file_only(runner, settings, sample_dir):
    path = Path(sample_dir) / "yellow_tripdata_2023-02_sample.csv"
    path.write_text(path.read_text().replace("fare_amount", "fare_amt", 1))
    ctx = runner.run(sample_dir)
    assert runner.repo.get_run(ctx.run_id)["status"] == "PARTIAL_FAILURE"
    assert any("fare_amount" in msg for msg in ctx.failed.values())


def test_corrupted_file_is_recorded_and_good_files_still_load(runner, settings, sample_dir):
    (Path(sample_dir) / "broken.csv").write_bytes(bytes(range(128, 256)) * 20)
    ctx = runner.run(sample_dir)
    run = runner.repo.get_run(ctx.run_id)
    assert run["status"] == "PARTIAL_FAILURE" and "broken.csv" in run["error_summary"]
    assert runner.repo.list_errors(ctx.run_id)
    assert sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0] == totals(sample_dir)["accepted"]


def test_lineage_and_metadata_are_recorded(runner, settings, sample_dir):
    ctx = run_ok(runner, sample_dir)
    files = runner.repo.contributing_files("gold.daily_trip_metrics")
    assert sorted(Path(f["file"]).name for f in files) == sorted(p.name for p in Path(sample_dir).glob("*.csv"))
    assert all(ctx.run_id in f["run_ids"] for f in files)
    assert runner.repo.get_run(ctx.run_id)["records_written"] == totals(sample_dir)["accepted"]
    assert {p["partition_value"] for p in runner.repo.list_partitions(DATASET_SILVER)} == \
        set(list_partitions(settings.silver_dir, "pickup_date"))


def test_monthly_partitioning_scheme(settings, spark_session, sample_dir):
    runner = make_runner(settings.with_overrides(partition_scheme="monthly"), spark_session)
    ctx = runner.run(sample_dir)
    assert runner.repo.get_run(ctx.run_id)["status"] == "SUCCEEDED"
    assert sorted(list_partitions(settings.silver_dir, "pickup_month")) == ["2023-01", "2023-02"]
    runner.repo.close()


def test_parquet_output_is_readable_by_duckdb_and_spark(runner, settings, sample_dir, spark_session):
    run_ok(runner, sample_dir)
    df = spark_session.read.option("basePath", settings.silver_dir.resolve().as_uri()).parquet(
        settings.silver_dir.resolve().as_uri())
    assert df.count() == sql(settings, "SELECT COUNT(*) FROM silver_trips")[0][0]


# dbt / postgres / dashboard / benchmark -----------------------------------------------------------
@pytest.mark.dbt
def test_dbt_models_compile_run_and_pass_tests(runner, settings, sample_dir):
    pytest.importorskip("dbt.cli.main")
    run_ok(runner, sample_dir)
    from src.warehouse.warehouse_loader import run_dbt

    run_dbt(settings, "compile")
    run_dbt(settings, "build")  # models + schema tests + singular tests; raises on any failure
    assert sql(settings, "SELECT COUNT(*) FROM gold.daily_trip_metrics")[0][0] == len(totals(sample_dir)["daily"])


@pytest.mark.postgres
def test_postgres_metadata_store(pg_settings):
    pytest.importorskip("psycopg2")
    from src.metadata.repository import open_repository

    repo = open_repository(pg_settings)
    repo.reset()
    repo.start_run("pg-run-1", "datatrust_batch", "test")
    repo.finish_run("pg-run-1", "SUCCEEDED", None, records_read=5)
    repo.add_lineage_edge("pg-run-1", "file:/a.csv", "ingest", "bronze.trips", "c" * 64, 5)
    assert repo.get_run("pg-run-1")["records_read"] == 5
    assert repo.lineage_upstream("bronze.trips")[0]["source_dataset"] == "file:/a.csv"
    repo.reset()
    repo.close()


@pytest.mark.dashboard
def test_dashboard_pages_launch_with_and_without_data(runner, settings, sample_dir, monkeypatch):
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    root = Path(__file__).resolve().parents[2]
    pages = [root / "app" / "dashboard.py", *sorted((root / "app" / "pages").glob("*.py"))]
    monkeypatch.setenv("DATATRUST_DATA_ROOT", str(settings.data_root))
    for page in pages:  # empty state first
        at = AppTest.from_file(str(page), default_timeout=60).run()
        assert not at.exception, (page.name, at.exception)
    run_ok(runner, sample_dir)
    for page in pages:
        at = AppTest.from_file(str(page), default_timeout=60).run()
        assert not at.exception, (page.name, at.exception)
    overview = AppTest.from_file(str(pages[0]), default_timeout=60).run()
    assert any("Records" in m.label for m in overview.metric)


def test_benchmark_records_real_measurements_and_shows_pruning(settings):
    from benchmarks.partition_benchmark import run_benchmark

    result = run_benchmark(settings, rows=3000, days=6, repeats=1)
    rows = [r for r in result["results"] if r["query"] == "one_day_filter"]
    by_day = next(r for r in rows if r["layout"] == "by_day")
    flat = next(r for r in rows if r["layout"] == "unpartitioned")
    assert by_day["files_touched"] == 1 < by_day["files_total"]
    assert flat["files_touched"] == flat["files_total"] == 1
    assert all(r["median_seconds"] > 0 for r in result["results"])
    assert list((settings.reports_dir / "benchmarks").glob("benchmark_*.json"))
    assert os.path.getsize(next((settings.reports_dir / "benchmarks").glob("benchmark_*.csv"))) > 0
