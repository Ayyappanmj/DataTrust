import pytest

from src.common.exceptions import SchemaValidationError
from src.processing.schema_validation import (
    check_schema,
    normalize_column_name,
    normalize_columns,
    schema_fingerprint,
)
from src.sample.generator import COLUMNS


@pytest.mark.parametrize("raw,expected", [
    ("PULocationID", "pu_location_id"), ("VendorID", "vendor_id"),
    ("tpep_pickup_datetime", "pickup_datetime"), ("lpep_dropoff_datetime", "dropoff_datetime"),
    ("Airport_fee", "airport_fee"), ("_source_file", "source_file"), (" Fare Amount ", "fare_amount"),
])
def test_normalize_column_name(raw, expected):
    assert normalize_column_name(raw) == expected


def test_collision_raises():
    with pytest.raises(SchemaValidationError):
        normalize_columns(["Airport_fee", "airport_fee"])


def test_known_schema_passes():
    assert check_schema(COLUMNS).status in {"PASSED", "WARN"}
    assert check_schema(COLUMNS).missing_required == []


def test_missing_required_column_fails():
    result = check_schema([c for c in COLUMNS if c != "fare_amount"])
    assert result.status == "FAILED" and result.missing_required == ["fare_amount"]


@pytest.mark.parametrize("policy,status", [("warn", "WARN"), ("fail", "FAILED"), ("ignore", "PASSED")])
def test_unexpected_column_policy(policy, status):
    result = check_schema([*COLUMNS, "surprise_col"], unexpected_policy=policy)
    assert result.unexpected == ["surprise_col"]
    # optional columns missing never change the status; only the unexpected one does
    assert result.status == status


def test_schema_drift_is_reported_as_warning():
    previous = schema_fingerprint(COLUMNS)
    result = check_schema([*COLUMNS, "extra_one"], "ignore", previous)
    assert result.drift_from_previous and result.status == "WARN"


def test_fingerprint_ignores_order_and_case():
    assert schema_fingerprint(["B", "a"]) == schema_fingerprint(["a", "b"])
