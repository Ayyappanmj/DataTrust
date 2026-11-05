{{ config(materialized='table') }}
-- One row per pickup date. Not incremental on purpose: it is a small aggregate over a
-- table that is replaced partition-by-partition, so a full rebuild is cheap and always correct.
with daily as (
    select
        pickup_date,
        count(*) as trips,
        sum(fare_revenue) as fare_revenue,
        sum(tip_amount) as tip_amount,
        sum(tolls_amount) as tolls_amount,
        sum(taxes_and_fees) as taxes_and_fees,
        sum(total_amount) as total_amount,
        avg(fare_amount) as avg_fare,
        avg(trip_duration_minutes) as avg_trip_minutes,
        avg(trip_distance) as avg_trip_distance,
        sum(case when is_card_trip = 1 then tip_amount else 0 end) as card_tip_amount,
        sum(case when is_card_trip = 1 then fare_amount else 0 end) as card_fare_amount
    from {{ ref('clean_trips') }}
    group by pickup_date
)
select
    pickup_date, trips, fare_revenue, tip_amount, tolls_amount, taxes_and_fees, total_amount,
    avg_fare, avg_trip_minutes, avg_trip_distance,
    case when card_fare_amount > 0 then 100.0 * card_tip_amount / card_fare_amount end as avg_tip_pct,
    avg(trips) over (order by pickup_date rows between 6 preceding and current row) as trips_7d_avg,
    trips - lag(trips) over (order by pickup_date) as trips_change_vs_prev_day
from daily
