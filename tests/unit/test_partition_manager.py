import json
from datetime import date

import pytest

from src.common.config import Settings
from src.common.exceptions import ConfigError, ProcessingError
from src.storage.parquet_storage import StagingArea, partition_stats, recover_incomplete_commits
from src.storage.partition_manager import (
    apply_partition_actions,
    list_partitions,
    month_bounds,
    parse_date,
    partition_dir,
    partitions_for_range,
    select_partitions,
)


def make(root, col, value, name="part.parquet", size=10):
    d = root / f"{col}={value}"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(b"x" * size)


def test_daily_and_monthly_ranges():
    assert partitions_for_range(date(2023, 1, 30), date(2023, 2, 2), "daily") == [
        "2023-01-30", "2023-01-31", "2023-02-01", "2023-02-02"]
    assert partitions_for_range(date(2023, 1, 30), date(2023, 3, 2), "monthly") == ["2023-01", "2023-02", "2023-03"]
    assert partitions_for_range(date(2022, 12, 31), date(2023, 1, 1), "monthly") == ["2022-12", "2023-01"]


def test_invalid_ranges_and_dates():
    with pytest.raises(ProcessingError):
        partitions_for_range(date(2023, 2, 1), date(2023, 1, 1), "daily")
    with pytest.raises(ProcessingError):
        parse_date("2023-13-45")
    with pytest.raises(ProcessingError):
        parse_date("01/02/2023")
    assert month_bounds("2024-02")[1] == date(2024, 2, 29)


def test_listing_selection_and_stats(tmp_path):
    make(tmp_path, "pickup_date", "2023-01-01", size=5)
    make(tmp_path, "pickup_date", "2023-01-02", size=7)
    (tmp_path / "pickup_date=2023-01-03").mkdir()  # empty -> ignored
    assert sorted(list_partitions(tmp_path, "pickup_date")) == ["2023-01-01", "2023-01-02"]
    paths = select_partitions(tmp_path, "pickup_date", ["2023-01-02", "2023-01-09"])
    assert [p.name for p in paths] == ["pickup_date=2023-01-02"]
    assert partition_stats(tmp_path, "pickup_date", ["2023-01-02"]) == {"2023-01-02": (1, 7)}


def test_unsafe_partition_values_are_rejected(tmp_path):
    with pytest.raises(ConfigError):
        partition_dir(tmp_path, "pickup_date", "../../etc")


def test_replace_and_delete_leave_other_partitions_untouched(tmp_path):
    target, staging = tmp_path / "t", tmp_path / "s"
    for v in ("2023-01-01", "2023-01-02", "2023-01-03"):
        make(target, "pickup_date", v, "old.parquet")
    make(staging, "pickup_date", "2023-01-02", "new.parquet")
    apply_partition_actions(staging, target, "pickup_date",
                            {"2023-01-02": "replace", "2023-01-03": "delete"})
    parts = list_partitions(target, "pickup_date")
    assert sorted(parts) == ["2023-01-01", "2023-01-02"]
    assert [f.name for f in parts["2023-01-02"].path.iterdir()] == ["new.parquet"]
    assert [f.name for f in parts["2023-01-01"].path.iterdir()] == ["old.parquet"]


def test_applying_actions_twice_is_idempotent(tmp_path):
    target, staging = tmp_path / "t", tmp_path / "s"
    make(target, "pickup_date", "2023-01-01", "old.parquet")
    make(staging, "pickup_date", "2023-01-01", "new.parquet")
    actions = {"2023-01-01": "replace"}
    apply_partition_actions(staging, target, "pickup_date", actions)
    apply_partition_actions(staging, target, "pickup_date", actions)
    assert [f.name for f in (target / "pickup_date=2023-01-01").iterdir()] == ["new.parquet"]


def test_replace_without_staged_or_existing_data_fails_loudly(tmp_path):
    with pytest.raises(ProcessingError):
        apply_partition_actions(tmp_path / "s", tmp_path / "t", "pickup_date", {"2023-01-01": "replace"})


def test_commit_and_crash_recovery(tmp_path):
    cfg = Settings(data_root=tmp_path)
    make(cfg.silver_dir, "pickup_date", "2023-01-01", "old.parquet")
    area = StagingArea(cfg, "r1")
    area.reset()
    make(area.silver_root, "pickup_date", "2023-01-01", "new.parquet")
    make(area.silver_root, "pickup_date", "2023-01-05", "new.parquet")
    area.write_plan({"partition_column": "pickup_date", "affected_values": ["2023-01-01", "2023-01-05"],
                     "quarantine_values": []})

    def crash(point):
        raise RuntimeError("power cut")

    with pytest.raises(RuntimeError):
        area.commit(fault_hook=crash)
    # journal says COMMITTING; recovery rolls the swap forward and is itself idempotent
    assert json.loads(area.journal_path.read_text())["state"] == "COMMITTING"
    assert recover_incomplete_commits(cfg) == ["r1"]
    assert recover_incomplete_commits(cfg) == []
    parts = list_partitions(cfg.silver_dir, "pickup_date")
    assert sorted(parts) == ["2023-01-01", "2023-01-05"]
    assert [f.name for f in parts["2023-01-01"].path.iterdir()] == ["new.parquet"]
