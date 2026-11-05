-- Fails (returns a row) if the daily aggregate loses or invents trips.
select d.total_trips, c.total_clean
from (select coalesce(sum(trips), 0) as total_trips from {{ ref('daily_trip_metrics') }}) d
cross join (select count(*) as total_clean from {{ ref('clean_trips') }}) c
where d.total_trips <> c.total_clean
