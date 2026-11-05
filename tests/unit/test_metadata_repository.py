import pytest

from src.common.exceptions import ConfigError, MetadataConnectionError, MetadataError
from src.metadata import lineage
from src.metadata.repository import PostgresRepository, SqliteRepository, open_repository
from src.orchestration.pipeline_status import summarize_runs

C1, C2 = "a" * 64, "b" * 64


def test_run_lifecycle_records_duration_and_counts(repo):
    repo.start_run("r1", "datatrust", "cli", {"x": 1})
    assert repo.get_run("r1")["status"] == "RUNNING"
    repo.finish_run("r1", "SUCCEEDED", None, records_read=10, records_accepted=8,
                    records_rejected=2, records_written=8)
    run = repo.get_run("r1")
    assert run["status"] == "SUCCEEDED" and run["records_rejected"] == 2
    assert run["duration_seconds"] is not None and run["ended_at"]


def test_unknown_columns_are_rejected(repo):
    repo.start_run("r1", "p")
    with pytest.raises(MetadataError):
        repo.update_run("r1", bogus=1)
    with pytest.raises(MetadataError):
        repo.upsert_manifest(C1, bogus=1)


def test_partial_manifest_updates_do_not_lose_other_fields(repo):
    repo.upsert_manifest(C1, file_path="/a.csv", file_name="a.csv", file_format="csv", size_bytes=3)
    repo.upsert_manifest(C1, ingest_status="SUCCEEDED", record_count=7)
    row = repo.get_manifest(C1)
    assert row["file_name"] == "a.csv" and row["ingest_status"] == "SUCCEEDED" and row["record_count"] == 7


def test_stale_running_runs_are_failed_but_current_one_is_kept(repo):
    repo.start_run("old", "p")
    repo.start_run("current", "p")
    assert repo.fail_stale_runs("current", timeout_minutes=-1) == ["old"]
    assert repo.get_run("old")["status"] == "FAILED"
    assert "interrupted" in repo.get_run("old")["error_summary"]
    assert repo.get_run("current")["status"] == "RUNNING"


def test_quality_results_errors_and_backfills_roundtrip(repo):
    repo.insert_quality_results([{
        "run_id": "r1", "stage": "silver", "dataset_name": "d", "rule_name": "x", "rule_description": "",
        "severity": "ERROR", "action": "reject", "records_evaluated": 10, "records_passed": 9,
        "records_failed": 1, "failure_pct": 10.0, "threshold_pct": 5.0, "status": "FAILED",
        "executed_at": "2026-01-01T00:00:00+00:00"}])
    assert repo.list_quality_results("r1")[0]["records_failed"] == 1
    repo.record_error("r1", "step", ValueError("bad"), {"k": "v"}, "/f.csv")
    assert repo.list_errors("r1")[0]["error_type"] == "ValueError"
    repo.start_backfill("b1", "r1", "2023-01-01", "2023-01-02", ["2023-01-01", "2023-01-02"])
    repo.finish_backfill("b1", "SUCCEEDED", 5)
    assert repo.list_backfills()[0]["status"] == "SUCCEEDED"


def test_lineage_answers_which_files_and_runs_contributed(repo):
    for checksum, name in ((C1, "jan.csv"), (C2, "feb.csv")):
        repo.upsert_manifest(checksum, file_path=f"/in/{name}", file_name=name, file_format="csv",
                             size_bytes=1, active=1)
    lineage.record_ingest(repo, "r1", "/in/jan.csv", C1, 100)
    lineage.record_silver(repo, "r1", C1, 90)
    lineage.record_ingest(repo, "r2", "/in/feb.csv", C2, 50)
    lineage.record_silver(repo, "r2", C2, 45)
    lineage.record_gold(repo, "r2", {"daily_trip_metrics": 59})
    files = lineage.contributing_sources(repo, "gold.daily_trip_metrics")
    assert sorted(f["file"] for f in files) == ["/in/feb.csv", "/in/jan.csv"]
    assert {r for f in files for r in f["run_ids"]} == {"r1", "r2"}
    depths = {e["output_dataset"]: e["depth"] for e in repo.lineage_upstream("gold.daily_trip_metrics")}
    assert depths["gold.daily_trip_metrics"] == 0 and depths["silver.trips"] == 1 and depths["bronze.trips"] == 2


def test_superseded_files_drop_out_of_lineage(repo):
    repo.upsert_manifest(C1, file_path="/in/jan.csv", file_name="jan.csv", file_format="csv",
                         size_bytes=1, active=1)
    lineage.record_ingest(repo, "r1", "/in/jan.csv", C1, 100)
    lineage.record_silver(repo, "r1", C1, 90)
    assert lineage.contributing_sources(repo, "silver.trips")
    repo.set_active(C1, False)
    assert lineage.contributing_sources(repo, "silver.trips") == []


def test_failed_edges_are_not_followed(repo):
    repo.upsert_manifest(C1, file_path="/in/a.csv", file_name="a.csv", file_format="csv", size_bytes=1, active=1)
    lineage.record_ingest(repo, "r1", "/in/a.csv", C1, 5, status="FAILED")
    lineage.record_silver(repo, "r1", C1, 5)
    assert lineage.contributing_sources(repo, "silver.trips") == []


def test_partition_metadata_checkpoint_for_warehouse_loading(repo):
    repo.upsert_partition("silver.trips", "2023-01-01", 10, 100, 1, "r1")
    repo.upsert_partition("silver.trips", "2023-01-02", 0, 0, 0, "r1")
    assert [p["partition_value"] for p in repo.partitions_pending_load("silver.trips")] == ["2023-01-01", "2023-01-02"]
    repo.mark_partitions_loaded("silver.trips", ["2023-01-01"])
    assert [p["partition_value"] for p in repo.partitions_pending_load("silver.trips")] == ["2023-01-02"]
    repo.upsert_partition("silver.trips", "2023-01-01", 12, 120, 1, "r2")  # rewritten -> pending again
    assert len(repo.partitions_pending_load("silver.trips")) == 2


def test_reset_clears_all_tables(repo):
    repo.start_run("r1", "p")
    repo.upsert_dataset("d", "silver", "/x", None, "desc")
    repo.reset()
    assert repo.list_runs() == [] and repo.list_datasets() == []


def test_run_summary_success_rate_and_freshness():
    runs = [{"run_id": "a", "status": "SUCCEEDED", "started_at": "2026-01-01T00:00:00+00:00",
             "ended_at": "2026-01-01T00:01:00+00:00"},
            {"run_id": "b", "status": "FAILED", "started_at": "2026-01-02T00:00:00+00:00", "ended_at": None},
            {"run_id": "c", "status": "RUNNING", "started_at": "2026-01-03T00:00:00+00:00", "ended_at": None}]
    s = summarize_runs(runs)
    assert s["success_rate_pct"] == 50.0 and s["latest_successful_run"] == "a" and s["freshness_hours"] > 0
    assert summarize_runs([])["success_rate_pct"] is None


def test_postgres_backend_is_explicit_and_never_falls_back(settings):
    pytest.importorskip("psycopg2")
    cfg = settings.with_overrides(metadata_backend="postgres", pg_password="x", pg_host="127.0.0.1", pg_port=1)
    repo = open_repository(cfg, init=False)
    assert isinstance(repo, PostgresRepository)
    with pytest.raises(MetadataConnectionError, match="no fallback"):
        repo.init_schema()
    with pytest.raises(ConfigError):
        open_repository(settings.with_overrides(metadata_backend="postgres", pg_password=""))


def test_default_backend_is_sqlite(settings):
    repo = open_repository(settings)
    assert isinstance(repo, SqliteRepository)
    repo.close()
