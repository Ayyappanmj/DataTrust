{{ config(materialized='table') }}
-- Latest result per rule plus its history across runs.
with ranked as (
    select *,
        row_number() over (
            partition by dataset_name, rule_name, stage order by executed_at desc, run_id desc
        ) as rn
    from {{ ref('stg_quality_results') }}
),
history as (
    select dataset_name, rule_name, stage,
        count(distinct run_id) as runs_evaluated,
        avg(failure_pct) as avg_failure_pct,
        max(failure_pct) as max_failure_pct,
        sum(case when status = 'FAILED' then 1 else 0 end) as failed_runs
    from {{ ref('stg_quality_results') }}
    group by dataset_name, rule_name, stage
)
select
    l.dataset_name, l.rule_name, l.stage, l.severity,
    l.records_evaluated as latest_evaluated, l.records_failed as latest_failed,
    l.failure_pct as latest_failure_pct, l.status as latest_status,
    l.executed_at as latest_executed_at,
    h.runs_evaluated, h.avg_failure_pct, h.max_failure_pct, h.failed_runs
from ranked l
join history h
  on l.dataset_name = h.dataset_name and l.rule_name = h.rule_name and l.stage = h.stage
where l.rn = 1
