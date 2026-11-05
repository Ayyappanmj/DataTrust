{{ config(materialized='table') }}
-- Payment codes follow the TLC data dictionary.
select
    payment_type as payment_type_id,
    case payment_type
        when 0 then 'Flex Fare' when 1 then 'Credit card' when 2 then 'Cash'
        when 3 then 'No charge' when 4 then 'Dispute' when 5 then 'Unknown'
        when 6 then 'Voided trip' else 'Other' end as payment_type_name,
    count(*) as trips,
    100.0 * count(*) / sum(count(*)) over () as share_pct,
    sum(fare_revenue) as fare_revenue,
    sum(tip_amount) as tip_amount,
    case when sum(fare_amount) > 0 and payment_type = 1
         then 100.0 * sum(tip_amount) / sum(fare_amount) end as tip_pct
from {{ ref('clean_trips') }}
group by payment_type
