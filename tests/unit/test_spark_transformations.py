"""Spark-level unit tests of typing, rule evaluation and de-duplication (skip without PySpark)."""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import LongType, StringType, StructField, StructType  # noqa: E402
from src.common.config import Settings  # noqa: E402
from src.processing.deduplication import split_against_existing, split_in_batch_duplicates  # noqa: E402
from src.processing.transformations import SILVER_COLUMNS, to_typed  # noqa: E402
from src.quality.rules import applicable, load_rules  # noqa: E402
from src.quality.validators import spark_reject_reasons, spark_rule_stats  # noqa: E402

COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID", "payment_type", "fare_amount", "total_amount"]


def bronze(spark, rows):
    meta = [("f.csv", "abc", "2026-01-01 00:00:00", i, None) for i in range(len(rows))]
    data = [tuple(r) + m for r, m in zip(rows, meta, strict=True)]
    cols = COLS + ["_source_file", "_source_checksum", "_ingested_at", "_source_row_id", "_corrupt_record"]
    schema = StructType([
        StructField(c, LongType() if c == "_source_row_id" else StringType(), True)
        for c in cols
    ])
    return spark.createDataFrame(data, schema)


GOOD = ("1", "2023-01-02 10:00:00", "2023-01-02 10:30:00", "2.0", "3.5", "4", "5", "1", "15.0", "20.0")


def typed(spark, rows):
    return to_typed(bronze(spark, rows), Settings(), "run-1")


def test_casts_and_derived_columns(spark_session):
    row = typed(spark_session, [GOOD]).collect()[0].asDict()
    assert row["passenger_count"] == 2 and row["pu_location_id"] == 4 and row["fare_amount"] == 15.0
    assert row["trip_duration_minutes"] == 30.0 and row["pickup_date"] == "2023-01-02"
    assert row["pickup_month"] == "2023-01" and row["pickup_hour"] == 10
    assert row["pickup_dow"] == 1  # 2023-01-02 is a Monday (ISO 1)
    assert set(SILVER_COLUMNS) <= set(row)


@pytest.mark.parametrize("day,iso", [("2023-01-01", 7), ("2023-01-07", 6), ("2023-01-08", 7), ("2023-01-09", 1)])
def test_iso_weekday(spark_session, day, iso):
    r = list(GOOD)
    r[1], r[2] = f"{day} 10:00:00", f"{day} 10:10:00"
    assert typed(spark_session, [tuple(r)]).collect()[0]["pickup_dow"] == iso


def test_multiple_timestamp_formats_and_bad_values(spark_session):
    us = list(GOOD)
    us[1], us[2] = "01/02/2023 10:00:00 AM", "01/02/2023 10:30:00 AM"
    bad = list(GOOD)
    bad[1], bad[8] = "not-a-date", "abc"
    rows = {r["pickup_ts"] is None: r for r in typed(spark_session, [tuple(us), tuple(bad)]).collect()}
    assert rows[False]["trip_duration_minutes"] == 30.0
    assert rows[True]["fare_amount"] is None


def test_rules_reject_bad_rows_with_reasons_and_keep_good_ones(spark_session):
    neg = list(GOOD)
    neg[8] = "-5.0"
    zero_pax = list(GOOD)
    zero_pax[3] = "0"
    zero_pax[8] = "16.0"
    df = typed(spark_session, [GOOD, tuple(neg), tuple(zero_pax)])
    rules = applicable(load_rules(), set(df.columns))
    total, stats = spark_rule_stats(df, rules)
    assert total == 3 and stats["fare_amount_non_negative"][1] == 1 and stats["passenger_count_valid"][1] == 1
    reasons = {r["fare_amount"]: list(r["reasons"]) for r in
               df.withColumn("reasons", spark_reject_reasons(rules)).select("fare_amount", "reasons").collect()}
    assert reasons[15.0] == [] and reasons[-5.0] == ["fare_amount_non_negative"] and reasons[16.0] == ["passenger_count_valid"]


def test_record_id_is_deterministic_and_dedupe_keeps_first(spark_session):
    df = typed(spark_session, [GOOD, GOOD, GOOD])
    assert df.select("record_id").distinct().count() == 1
    kept, dups = split_in_batch_duplicates(df)
    assert kept.count() == 1 and dups.count() == 2
    assert kept.collect()[0]["source_row_id"] == 0


def test_dedupe_against_existing_rows(spark_session):
    df = typed(spark_session, [GOOD])
    new, dup = split_against_existing(df, df)
    assert new.count() == 0 and dup.count() == 1
    new2, dup2 = split_against_existing(df, None)
    assert new2.count() == 1 and dup2 is None
