"""Recovery behaviour that does not need Spark: interrupted commits and stale runs."""
import json

import pytest

from src.storage.parquet_storage import StagingArea, recover_incomplete_commits
from src.storage.partition_manager import list_partitions


def _make(root, value, name):
    d = root / f"pickup_date={value}"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(b"x")


def test_crash_between_silver_and_quarantine_swap_is_rolled_forward(settings):
    _make(settings.silver_dir, "2023-01-01", "old.parquet")
    area = StagingArea(settings, "run-crash")
    area.reset()
    _make(area.silver_root, "2023-01-01", "new.parquet")
    q = area.quarantine_root / "source_checksum=abc"
    q.mkdir(parents=True)
    (q / "q.parquet").write_bytes(b"x")
    area.write_plan({"partition_column": "pickup_date", "affected_values": ["2023-01-01"],
                     "quarantine_values": ["abc"]})

    def crash(point):
        raise RuntimeError("killed")

    with pytest.raises(RuntimeError):
        area.commit(fault_hook=crash)  # silver swapped, quarantine not yet
    assert not (settings.quarantine_dir / "source_checksum=abc").exists()
    assert recover_incomplete_commits(settings) == ["run-crash"]
    assert (settings.quarantine_dir / "source_checksum=abc" / "q.parquet").exists()
    parts = list_partitions(settings.silver_dir, "pickup_date")
    assert [f.name for f in parts["2023-01-01"].path.iterdir()] == ["new.parquet"]
    assert json.loads(area.journal_path.read_text())["state"] == "COMMITTED"


def test_unrelated_partitions_survive_an_interrupted_commit(settings):
    for v in ("2023-01-01", "2023-01-02"):
        _make(settings.silver_dir, v, "keep.parquet")
    area = StagingArea(settings, "run-x")
    area.reset()
    _make(area.silver_root, "2023-01-03", "new.parquet")
    area.write_plan({"partition_column": "pickup_date", "affected_values": ["2023-01-03"],
                     "quarantine_values": []})
    with pytest.raises(RuntimeError):
        area.commit(fault_hook=lambda p: (_ for _ in ()).throw(RuntimeError("boom")))
    recover_incomplete_commits(settings)
    parts = list_partitions(settings.silver_dir, "pickup_date")
    assert sorted(parts) == ["2023-01-01", "2023-01-02", "2023-01-03"]


def test_interrupted_run_left_running_is_failed_by_next_run(repo):
    repo.start_run("dead-run", "datatrust_batch")
    repo.start_run("new-run", "datatrust_batch")
    repo.fail_stale_runs("new-run", timeout_minutes=-1)
    assert repo.get_run("dead-run")["status"] == "FAILED"
