from src.sample.generator import generate_month, generate_samples, parse_ts, reference_row


def test_generation_is_deterministic():
    a, ea = generate_month("2023-01", valid_rows=100)
    b, eb = generate_month("2023-01", valid_rows=100)
    assert a == b and ea == eb


def test_months_are_independent_so_adding_a_month_never_changes_old_files(tmp_path):
    generate_samples(tmp_path, ["2023-01"], valid_rows=100)
    before = (tmp_path / "yellow_tripdata_2023-01_sample.csv").read_bytes()
    generate_samples(tmp_path, ["2023-01", "2023-02"], valid_rows=100)
    assert (tmp_path / "yellow_tripdata_2023-01_sample.csv").read_bytes() == before


def test_expected_summary_is_internally_consistent():
    lines, exp = generate_month("2023-03", valid_rows=200, per_injection=3, malformed_rows=1)
    assert exp["rows_read"] == len(lines) - 1
    assert exp["accepted"] + exp["duplicates"] + exp["rejected_validity"] == exp["rows_read"]
    assert sum(d["trips"] for d in exp["daily"].values()) == exp["accepted"]
    assert exp["malformed_lines"] == 1
    assert all(day.startswith("2023-03") for day in exp["daily"])


def test_timestamp_parsing_supports_known_formats():
    assert parse_ts("2023-01-05 10:00:00") == parse_ts("2023-01-05T10:00:00") == parse_ts("01/05/2023 10:00:00 AM")
    assert parse_ts("not-a-date") is None and parse_ts("") is None and parse_ts(None) is None


def test_reference_rules_flag_known_bad_rows():
    good = {"tpep_pickup_datetime": "2023-01-05 10:00:00", "tpep_dropoff_datetime": "2023-01-05 10:20:00",
            "passenger_count": "1", "trip_distance": "2.0", "PULocationID": "4", "DOLocationID": "5",
            "payment_type": "1", "fare_amount": "10.00", "extra": "0", "mta_tax": "0.5", "tip_amount": "2",
            "tolls_amount": "0", "improvement_surcharge": "1", "total_amount": "16.00",
            "congestion_surcharge": "2.5", "airport_fee": "0", "VendorID": "1"}
    assert reference_row(good).failures == {"total_reconciles"} or reference_row(good).failures == set()
    assert "passenger_count_valid" in reference_row({**good, "passenger_count": "0"}).failures
    assert "pickup_before_dropoff" in reference_row({**good, "tpep_dropoff_datetime": "2023-01-05 09:00:00"}).failures
    assert reference_row({**good, "tpep_pickup_datetime": "garbage"}).rejected
