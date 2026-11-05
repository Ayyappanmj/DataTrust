{{ config(materialized='table') }}
-- pickup_dow: ISO weekday, Monday = 1 ... Sunday = 7.
select
    pickup_dow, pickup_hour,
    count(*) as trips,
    sum(fare_revenue) as fare_revenue,
    avg(fare_amount) as avg_fare,
    avg(trip_duration_minutes) as avg_trip_minutes
from {{ ref('clean_trips') }}
group by pickup_dow, pickup_hour
