import sqlite3

import pytest

from src.common.config import Settings
from src.common.exceptions import ConfigError, QualityGateError
from src.quality.quality_report import build_report, render_text, write_report
from src.quality.quality_runner import blocking_failures, build_results, enforce_gate, run_sql_quality
from src.quality.rules import applicable, load_rules, status_for
from src.quality.validators import sql_rule_stats, table_columns
from src.sample.generator import generate_month
from tests.helpers import load_typed_table, typed_rows


@pytest.fixture
def month():
    lines, expected = generate_month("2023-01", valid_rows=400, per_injection=5, malformed_rows=0)
    conn = sqlite3.connect(":memory:")
    load_typed_table(conn, "t", typed_rows(lines))
    return conn, expected


def test_sql_rule_failures_match_independent_python_oracle(month):
    conn, expected = month
    rules = [r for r in applicable(load_rules(), table_columns(conn, "t")) if r.kind == "expression"]
    total, stats = sql_rule_stats(conn, "t", rules)
    assert total == expected["rows_read"]
    sql_failures = {name: failed for name, (_, failed) in stats.items() if failed}
    assert sql_failures == expected["rule_failures"]


def test_each_injected_defect_is_counted_by_its_own_rule(month):
    conn, expected = month
    failures = expected["rule_failures"]
    for rule in ("fare_amount_non_negative", "passenger_count_valid", "trip_distance_valid",
                 "pickup_before_dropoff", "pu_location_present", "payment_type_known"):
        assert failures[rule] == 5
    assert failures["pickup_ts_valid"] == 10  # 5 missing + 5 unparseable
    assert failures["trip_duration_valid"] == 10  # 5 negative (swapped) + 5 over 24h


def test_null_means_not_applicable_so_defects_are_not_double_counted(month):
    conn, expected = month
    assert "pickup_ts_in_range" not in expected["rule_failures"]


def test_duplicates_detected_with_sql_on_clean_table():
    conn = sqlite3.connect(":memory:")
    lines, _ = generate_month("2023-02", valid_rows=50, per_injection=0, malformed_rows=0)
    rows = typed_rows(lines)
    load_typed_table(conn, "t", rows + [rows[0], rows[1]])
    rules = [r for r in load_rules() if r.name == "duplicate_record"]
    _, stats = sql_rule_stats(conn, "t", rules)
    assert stats["duplicate_record"] == (52, 2)


def test_status_thresholds():
    assert status_for(0, 100, 5) == "PASSED"
    assert status_for(5, 100, 5) == "WARN"
    assert status_for(6, 100, 5) == "FAILED"
    assert status_for(1, 0, 5) == "FAILED"


def test_results_contain_required_fields_and_correct_counts(month):
    conn, expected = month
    results = run_sql_quality(conn, "t", "demo", "silver", "run-1", rules=load_rules())
    by_name = {r.rule_name: r for r in results}
    r = by_name["fare_amount_non_negative"]
    assert (r.records_evaluated, r.records_failed, r.records_passed) == (expected["rows_read"], 5, expected["rows_read"] - 5)
    d = r.to_dict()
    for key in ("dataset_name", "rule_name", "rule_description", "records_evaluated", "records_passed",
                "records_failed", "failure_pct", "executed_at", "severity", "status"):
        assert key in d
    assert d["failure_pct"] == round(100 * 5 / expected["rows_read"], 4)


def test_gate_blocks_only_at_or_above_configured_severity():
    rules = [r for r in load_rules() if r.name in {"fare_amount_non_negative", "rejection_rate_acceptable"}]
    stats = {"fare_amount_non_negative": (100, 50), "rejection_rate_acceptable": (100, 60)}
    results = build_results("d", "silver", "r", rules, stats)
    assert [r.rule_name for r in blocking_failures(results, "CRITICAL")] == ["rejection_rate_acceptable"]
    assert len(blocking_failures(results, "ERROR")) == 2
    with pytest.raises(QualityGateError, match="rejection_rate_acceptable"):
        enforce_gate(results, Settings(fail_on_severity="CRITICAL"))
    enforce_gate(results[:1], Settings(fail_on_severity="CRITICAL"))  # ERROR alone does not block


def test_report_json_and_text(tmp_path, month):
    conn, _ = month
    results = run_sql_quality(conn, "t", "demo", "silver", "run-9", rules=load_rules())
    report = build_report("run-9", "silver", results)
    assert sum(report["rule_status_counts"].values()) == len(results)
    assert report["overall_status"] in {"PASSED", "WARN", "FAILED"}
    json_path, txt_path = write_report(report, tmp_path)
    assert json_path.exists() and "fare_amount_non_negative" in txt_path.read_text()
    assert "Overall" in render_text(report)


def test_yaml_overrides(tmp_path):
    cfg = tmp_path / "q.yaml"
    cfg.write_text("rules:\n  total_reconciles:\n    severity: ERROR\n    max_failure_pct: 1\n"
                   "  passenger_count_valid:\n    enabled: false\n    params: {max_passengers: 6}\n")
    rules = {r.name: r for r in load_rules(config_path=cfg)}
    assert rules["total_reconciles"].severity == "ERROR" and rules["total_reconciles"].max_failure_pct == 1
    assert not rules["passenger_count_valid"].enabled
    assert "BETWEEN 1 AND 6" in rules["passenger_count_valid"].sql()


@pytest.mark.parametrize("body", [
    "rules:\n  no_such_rule: {severity: ERROR}\n",
    "rules:\n  total_reconciles: {severity: LOUD}\n",
    "rules:\n  total_reconciles: {max_failure_pct: 500}\n",
    "rules:\n  total_reconciles: {surprise: 1}\n",
    "rules:\n  passenger_count_valid: {params: {nope: 1}}\n",
])
def test_invalid_quality_config_is_rejected(tmp_path, body):
    cfg = tmp_path / "q.yaml"
    cfg.write_text(body)
    with pytest.raises(ConfigError):
        load_rules(config_path=cfg)


def test_default_quality_config_file_is_valid():
    assert load_rules(Settings())
