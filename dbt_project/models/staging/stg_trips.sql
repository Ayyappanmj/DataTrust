{{ config(materialized='view') }}
-- Standardised view over Silver trips (one row per accepted trip).
select
    record_id, vendor_id, pickup_ts, dropoff_ts, passenger_count, trip_distance,
    pu_location_id, do_location_id, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
    congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount,
    trip_duration_minutes, pickup_date, pickup_hour, pickup_dow,
    source_file, source_checksum
from {{ source('datatrust', 'silver_trips') }}
