# Data Quality Rules

Rules are defined in `src/quality/rules.py` and can be tuned in `config/quality_rules.yaml` (`severity`, `action`, `max_failure_pct`, `enabled`, `params`).

## How evaluation works

* A row **fails** a rule only when its expression is FALSE. NULL means *not applicable*, so one defect is counted by one rule (an unparseable pickup time is counted by `pickup_ts_valid`, not again by range or duration rules).
* `reject` rules send failing rows to quarantine; `flag` rules keep the row and only count it.
* Status per rule: **PASSED** (0 failures), **WARN** (failures within `max_failure_pct`), **FAILED** (above it).
* The **quality gate** blocks the run (before Silver is changed) when a FAILED rule has severity >= `DATATRUST_FAIL_ON_SEVERITY` (default `CRITICAL`).
* The same expression text is evaluated by Spark (`F.expr`) and by plain SQL (DuckDB / SQLite), so the Silver check and the warehouse check cannot drift apart.
* Thresholds are deliberately lenient where real TLC data is known to be messy (e.g. `total_reconciles`, `payment_type_known`) and strict where a defect makes a record unusable.

## Row-level and dataset rules (Silver stage)

| Rule | Description | Expression (SQL) | Severity | Action | Max failure |
|---|---|---|---|---|---|
| `malformed_record` | Source line could not be parsed by the CSV reader. | `corrupt_record IS NULL` | ERROR | reject | 2.0% |
| `pickup_ts_valid` | Pickup timestamp is present and parseable. | `pickup_ts IS NOT NULL` | ERROR | reject | 2.0% |
| `dropoff_ts_valid` | Drop-off timestamp is present and parseable. | `dropoff_ts IS NOT NULL` | ERROR | reject | 2.0% |
| `pickup_ts_in_range` | Pickup falls between 2009-01-01 00:00:00 and 2100-01-01 00:00:00. | `pickup_ts >= '2009-01-01 00:00:00' AND pickup_ts < '2100-01-01 00:00:00'` | ERROR | reject | 5.0% |
| `pickup_before_dropoff` | Pickup is not after drop-off. | `pickup_ts <= dropoff_ts` | ERROR | reject | 5.0% |
| `trip_duration_valid` | Trip lasts more than 0 and at most 1440 minutes. | `trip_duration_minutes > 0 AND trip_duration_minutes <= 1440` | ERROR | reject | 5.0% |
| `pu_location_present` | Pickup location id is present. | `pu_location_id IS NOT NULL` | ERROR | reject | 2.0% |
| `do_location_present` | Drop-off location id is present. | `do_location_id IS NOT NULL` | ERROR | reject | 2.0% |
| `fare_amount_present` | Fare amount is present and numeric. | `fare_amount IS NOT NULL` | ERROR | reject | 2.0% |
| `total_amount_present` | Total amount is present and numeric. | `total_amount IS NOT NULL` | ERROR | reject | 2.0% |
| `fare_amount_non_negative` | Fare amount is not negative. | `fare_amount >= 0` | ERROR | reject | 5.0% |
| `tip_amount_non_negative` | Tip amount is not negative. | `tip_amount >= 0` | ERROR | reject | 5.0% |
| `tolls_amount_non_negative` | Tolls amount is not negative. | `tolls_amount >= 0` | ERROR | reject | 5.0% |
| `total_amount_non_negative` | Total amount is not negative. | `total_amount >= 0` | ERROR | reject | 5.0% |
| `passenger_count_valid` | Passenger count (when present) is between 1 and 9. | `passenger_count BETWEEN 1 AND 9` | ERROR | reject | 5.0% |
| `trip_distance_valid` | Trip distance is between 0 and 1000 miles. | `trip_distance >= 0 AND trip_distance <= 1000` | ERROR | reject | 5.0% |
| `coordinates_valid_range` | Pickup coordinates (only when the columns exist) are valid lat/lon values. | `pickup_latitude BETWEEN -90 AND 90 AND pickup_longitude BETWEEN -180 AND 180` | ERROR | reject | 5.0% |
| `coordinates_plausible_nyc` | Pickup coordinates (only when the columns exist) lie in the NYC area; (0,0) placeholders in old TLC files are flagged. | `pickup_latitude BETWEEN 40.4 AND 41.1 AND pickup_longitude BETWEEN -74.5 AND -73.5` | WARNING | flag | 10.0% |
| `payment_type_known` | Payment type is one of the documented TLC codes 0-6. | `payment_type IN (0, 1, 2, 3, 4, 5, 6)` | WARNING | flag | 5.0% |
| `total_reconciles` | Total amount equals the sum of its components within 0.05. | `ABS(total_amount - (fare_amount + COALESCE(extra, 0) + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0) + COALESCE(tolls_amount, 0) + COALESCE(improvement_surcharge, 0) + COALESCE(congestion_surcharge, 0) + COALESCE(airport_fee, 0) + COALESCE(cbd_congestion_fee, 0))) <= 0.05` | WARNING | flag | 10.0% |
| `duplicate_record` | Record is not a duplicate of another accepted record. | `(computed in Silver job)` | ERROR | reject | 5.0% |
| `rejection_rate_acceptable` | Share of rejected records stays below the dataset-level limit. | `(computed by pipeline)` | CRITICAL | gate | 30.0% |

## File-level rules (ingestion stage)

| Rule | Description | Severity | Max failure |
|---|---|---|---|
| `source_file_readable` | Source file exists, is non-empty and is not corrupted. | CRITICAL | 0.0% |
| `required_columns_present` | Every required column exists in the source file. | CRITICAL | 0.0% |
| `unexpected_columns` | Source file has no columns outside the known schema. | WARNING | 0.0% |
| `schema_drift` | Column set matches the previously ingested file. | WARNING | 0.0% |
| `malformed_lines` | CSV lines have the same number of fields as the header. | WARNING | 1.0% |

File-level rules count files (or CSV rows for `malformed_lines`). A failed file is marked FAILED in the manifest; the other files continue (`DATATRUST_ON_FILE_ERROR=continue`) and the run ends `PARTIAL_FAILURE`.

## Other checks

* **Schema policy** (`DATATRUST_UNEXPECTED_COLUMNS` = warn | fail | ignore): missing required columns always fail the file; unexpected columns follow the policy; a column set different from the previously ingested file is reported as `schema_drift` (warning).
* **Gold contract** (Pandera): `gold.daily_trip_metrics` must have unique dates, trips >= 1, non-negative revenue/fare, positive average duration.
* **Coordinates**: `coordinates_*` rules apply only if `pickup_latitude`/`pickup_longitude` columns exist (modern TLC files have none).
* **Duplicates**: key = `vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, passenger_count, trip_distance, fare_amount, total_amount`. The first row (lowest checksum, row id) is kept.

## Reports

`data/reports/quality/<run_id>__<stage>.json` (machine-readable) and `.txt` (summary table); results are also stored in the metadata `quality_results` table and shown in the dashboard.

## Synthetic invalid data (sample files)

Each generated month contains: negative fares, drop-off before pickup, zero passengers, missing pickup time, unparseable pickup time, >24 h trips, negative distances, missing pickup location, exact duplicates, unknown payment types (flag only), total mismatches (flag only) and 2 truncated lines. The generator also writes `_expected/<month>.json`, computed by an independent pure-Python implementation, which the tests compare against.
