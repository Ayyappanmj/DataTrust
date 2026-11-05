select sum(share_pct) as total_share
from {{ ref('payment_type_metrics') }}
having count(*) > 0 and abs(sum(share_pct) - 100.0) > 0.01
