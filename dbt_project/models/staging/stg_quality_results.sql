{{ config(materialized='view') }}
select
    run_id, stage, dataset_name, rule_name, severity, action,
    records_evaluated, records_passed, records_failed, failure_pct, threshold_pct,
    status, executed_at
from {{ source('datatrust', 'quality_results') }}
