"""DataTrust Airflow DAG.

Every task shells out to ``python -m src.cli step ...`` inside the project's own virtualenv
(DATATRUST_PYTHON), so Airflow's dependencies never clash with PySpark/dbt/DuckDB. Tasks pass
only a deterministic run id; all data and state live in Parquet, DuckDB and the metadata store
(nothing large goes through XCom).

Config (Airflow Variables or environment):
  DATATRUST_SCHEDULE   cron expression or preset, default "@daily"
  DATATRUST_INPUT_DIR  input directory (default /opt/datatrust/data/sample)
  DATATRUST_PYTHON     interpreter used for tasks (default "python")
Backfill: ``airflow dags backfill -s 2023-01-01 -e 2023-01-31 datatrust_pipeline`` (each logical
date is its own run id; processing is idempotent so repeated runs never duplicate data), or the
CLI ``python -m src.cli backfill --start ... --end ...`` for pickup-date backfills.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("DATATRUST_PROJECT_DIR", "/opt/datatrust")
PYTHON = os.environ.get("DATATRUST_PYTHON", "python")
SCHEDULE = Variable.get("DATATRUST_SCHEDULE", default_var=os.environ.get("DATATRUST_SCHEDULE", "@daily"))
INPUT_DIR = Variable.get("DATATRUST_INPUT_DIR",
                         default_var=os.environ.get("DATATRUST_INPUT_DIR", f"{PROJECT_DIR}/data/sample"))
RUN_ID = "af-{{ run_id | replace(':', '-') | replace('+', '-') | replace(' ', '-') }}"


def _on_failure(context: dict) -> None:
    """Mark the DataTrust run FAILED so a dead task never leaves a RUNNING run behind."""
    import subprocess

    run_id = "af-" + "".join(
        ch if ch.isalnum() or ch in "-_." else "-" for ch in context["run_id"])
    subprocess.run(
        [PYTHON, "-m", "src.cli", "finalize-failed", "--run-id", run_id,
         "--message", f"Airflow task {context['task_instance'].task_id} failed"],
        cwd=PROJECT_DIR, check=False)


default_args = {
    "owner": "datatrust",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="datatrust_pipeline",
    description="Ingest -> Bronze -> Silver -> quality gate -> DuckDB -> dbt -> final checks",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["datatrust", "data-engineering"],
) as dag:

    def step(task_id: str, cli_step: str, extra_env: dict | None = None) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            cwd=PROJECT_DIR,
            bash_command=(f'{PYTHON} -m src.cli step --step {cli_step} --run-id "{RUN_ID}" '
                          f'--input "{INPUT_DIR}"'),
            env={**os.environ, **(extra_env or {})},
            append_env=True,
        )

    discover = step("discover_input_files", "discover_files")
    validate = step("validate_source_files", "validate_files")
    ingest = step("ingest_source_data", "ingest_bronze")
    schema = step("run_schema_checks", "schema_checks")
    silver = step("bronze_to_silver", "bronze_to_silver")
    quality = step("execute_quality_rules", "quality_rules")
    load = step("load_duckdb", "load_duckdb")
    dbt = step("run_dbt_models", "dbt_models", {"DATATRUST_MODELING_ENGINE": "dbt"})
    final = step("final_quality_checks", "final_quality_checks")
    lineage = step("record_lineage", "record_lineage")
    summary = step("generate_run_summary", "run_summary")

    (discover >> validate >> ingest >> schema >> silver >> quality >> load >> dbt
     >> final >> lineage >> summary)
