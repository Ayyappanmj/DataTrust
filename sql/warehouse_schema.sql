-- DuckDB source tables loaded by src/warehouse/warehouse_loader.py (single source of truth).
-- Models in dbt_project/models read these via source('datatrust', ...).
CREATE TABLE IF NOT EXISTS silver_trips (
    record_id VARCHAR,
    vendor_id INTEGER,
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE,
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    trip_duration_minutes DOUBLE,
    pickup_hour INTEGER,
    pickup_dow INTEGER,
    pickup_month VARCHAR,
    pickup_date DATE,
    source_file VARCHAR,
    source_checksum VARCHAR,
    source_row_id BIGINT,
    ingested_at TIMESTAMP,
    silver_run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS quality_results (
    run_id VARCHAR, stage VARCHAR, dataset_name VARCHAR, rule_name VARCHAR,
    rule_description VARCHAR, severity VARCHAR, action VARCHAR,
    records_evaluated BIGINT, records_passed BIGINT, records_failed BIGINT,
    failure_pct DOUBLE, threshold_pct DOUBLE, status VARCHAR, executed_at VARCHAR
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, pipeline_name VARCHAR, status VARCHAR, started_at VARCHAR, ended_at VARCHAR,
    duration_seconds DOUBLE, records_read BIGINT, records_accepted BIGINT,
    records_rejected BIGINT, records_written BIGINT, error_summary VARCHAR
);
