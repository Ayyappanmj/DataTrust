{{ config(materialized='table') }}
-- Business definitions live here:
--   fare_revenue   = fare_amount + extra               (metered fare and extras)
--   taxes_and_fees = mta_tax + improvement_surcharge + congestion_surcharge + airport_fee
--                    + cbd_congestion_fee               (pass-through, NOT revenue)
--   tolls_amount and tip_amount are reported separately and never added to revenue.
--   tip_pct is only defined for card trips (payment_type = 1): cash tips are not recorded.
select
    t.*,
    t.fare_amount + coalesce(t.extra, 0) as fare_revenue,
    coalesce(t.mta_tax, 0) + coalesce(t.improvement_surcharge, 0)
        + coalesce(t.congestion_surcharge, 0) + coalesce(t.airport_fee, 0)
        + coalesce(t.cbd_congestion_fee, 0) as taxes_and_fees,
    case when t.payment_type = 1 then 1 else 0 end as is_card_trip,
    case when t.payment_type = 1 and t.fare_amount > 0
         then 100.0 * t.tip_amount / t.fare_amount end as tip_pct
from {{ ref('stg_trips') }} as t
