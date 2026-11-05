{{ config(materialized='table') }}
select
    run_id, pipeline_name, status, started_at, substr(started_at, 1, 10) as run_date,
    ended_at, duration_seconds, records_read, records_accepted, records_rejected,
    records_written,
    case when records_read > 0 then 100.0 * records_rejected / records_read end as rejection_pct,
    error_summary
from {{ ref('stg_pipeline_runs') }}
