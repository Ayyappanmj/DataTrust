{{ config(materialized='view') }}
select
    run_id, pipeline_name, status, started_at, ended_at, duration_seconds,
    records_read, records_accepted, records_rejected, records_written, error_summary
from {{ source('datatrust', 'pipeline_runs') }}
