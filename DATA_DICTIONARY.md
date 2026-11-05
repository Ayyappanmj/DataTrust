# Data Dictionary

Source: NYC TLC yellow/green taxi trip records. Column names below are the canonical (Silver) names; source aliases are normalised
(`PULocationID` -> `pu_location_id`, `tpep_pickup_datetime`/`lpep_pickup_datetime` -> `pickup_datetime`).

## Monetary definitions (explicit on purpose)

| Concept | Definition | Counted as revenue? |
|---|---|---|
| `fare_revenue` | `fare_amount + extra` | **Yes** |
| `tip_amount` | card tips recorded by the meter (cash tips are not recorded) | No (reported separately) |
| `tolls_amount` | tolls | No (pass-through) |
| `taxes_and_fees` | `mta_tax + improvement_surcharge + congestion_surcharge + airport_fee + cbd_congestion_fee` | No (pass-through) |
| `total_amount` | everything the passenger paid | No (used for reconciliation only) |
| `tip_pct` | `100 * tip_amount / fare_amount`, **card trips only** (`payment_type = 1`) | - |

`total_reconciles` (WARNING) checks `total_amount` against the sum of its components (tolerance 0.05).

## Timestamp assumptions

TLC timestamps are local wall-clock times without a time zone. DataTrust parses them as naive values (Spark session zone UTC) and **never shifts them**.
`pickup_date` is the calendar date of the pickup wall-clock time. Backfill ranges are inclusive dates in the same sense.

## Silver: `silver.trips` (DuckDB `main.silver_trips`)

| Column | Type | Description |
|---|---|---|
| record_id | string | SHA-256 of the business key (duplicate detection) |
| vendor_id, rate_code_id, payment_type | int | TLC codes (payment: 0 Flex, 1 Card, 2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided) |
| pickup_ts, dropoff_ts | timestamp | parsed from the source text |
| passenger_count | int | driver-entered; NULL allowed, 0 rejected |
| trip_distance | double | miles |
| store_and_fwd_flag | string | upper-cased Y/N |
| pu_location_id, do_location_id | int | TLC taxi zone ids |
| fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee | double | dollars; optional columns may be NULL |
| trip_duration_minutes | double | `(dropoff - pickup)` in minutes |
| pickup_hour | int | 0-23 |
| pickup_dow | int | ISO weekday: Monday=1 ... Sunday=7 |
| pickup_month / pickup_date | string / date | `YYYY-MM`; `YYYY-MM-DD` (partition columns) |
| source_file, source_checksum, source_row_id, ingested_at, silver_run_id | - | provenance (checksum is the first 20 hex chars of the SHA-256) |

## Bronze: `bronze.trips`
All original columns as text with original names + `_source_file`, `_source_checksum`, `_ingested_at`, `_source_row_id`, `_corrupt_record`.

## Quarantine: `quarantine.trips`
`source_file, source_checksum, source_row_id, raw_record (JSON of the original fields), rejection_reason (rule names joined by ';'), run_id, rejected_at, pickup_date`.

## Gold models (DuckDB schema `gold`; `intermediate.clean_trips`)

| Model | Grain | Key columns |
|---|---|---|
| clean_trips (intermediate) | trip | Silver + `fare_revenue`, `taxes_and_fees`, `is_card_trip`, `tip_pct` |
| daily_trip_metrics | pickup_date | trips, fare_revenue, tip_amount, tolls_amount, taxes_and_fees, total_amount, avg_fare, avg_trip_minutes, avg_trip_distance, avg_tip_pct, trips_7d_avg, trips_change_vs_prev_day |
| hourly_trip_metrics | pickup_dow x pickup_hour | trips, fare_revenue, avg_fare, avg_trip_minutes |
| location_metrics | pu_location_id | trips, fare_revenue, avg_fare, avg_trip_distance, pct_of_trips |
| payment_type_metrics | payment_type_id | name, trips, share_pct, fare_revenue, tip_amount, tip_pct |
| data_quality_summary | dataset x rule x stage | latest status/failure %, runs evaluated, avg/max failure %, failed runs |
| pipeline_run_summary | run | status, duration, record counts, rejection_pct |
| stg_trips / stg_quality_results / stg_pipeline_runs (staging views) | - | standardised source views |

## Metadata tables
`pipeline_runs, ingestion_manifest, datasets, quality_results, processing_errors, lineage_edges, backfill_history, partition_metadata` - see `sql/metadata_schema.sql`.
