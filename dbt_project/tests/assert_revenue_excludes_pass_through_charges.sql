-- fare_revenue must equal fare_amount + extra; tolls/taxes/tips must not leak into it.
select record_id
from {{ ref('clean_trips') }}
where abs(fare_revenue - (fare_amount + coalesce(extra, 0))) > 0.0001
