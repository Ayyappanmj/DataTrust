{{ config(materialized='table') }}
select
    pu_location_id,
    count(*) as trips,
    sum(fare_revenue) as fare_revenue,
    avg(fare_amount) as avg_fare,
    avg(trip_distance) as avg_trip_distance,
    100.0 * count(*) / sum(count(*)) over () as pct_of_trips
from {{ ref('clean_trips') }}
group by pu_location_id
