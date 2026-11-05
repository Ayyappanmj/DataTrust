# Troubleshooting

## Spark / Java
- **`JAVA_HOME is not set` / `java not found`** - install Java 17 (Temurin) and reopen PowerShell. Check `java -version`.
- **Windows: `UnsatisfiedLinkError ... NativeIO$Windows.access0` or `HADOOP_HOME and hadoop.home.dir are unset`** - Spark's Hadoop layer wants
  `winutils.exe`. Either install a winutils build matching Hadoop 3.3 and set `HADOOP_HOME` (folder containing `bin\winutils.exe`), or run in Docker:
  `.\scripts\docker.ps1 -Action pipeline`. WSL2 also works.
- **`Python worker failed to connect back`** - the code sets `PYSPARK_PYTHON` to the active interpreter; make sure you run from the project's `.venv`.
- **Out of memory** - raise `DATATRUST_SPARK_DRIVER_MEMORY` (e.g. `4g`) and lower `DATATRUST_SHUFFLE_PARTITIONS` for small data.

## Installation
- **pip dependency conflict** - the pinned set was not resolved on a real machine by the author. Install `requirements.txt` first, then `requirements-dashboard.txt`; if `dbt-core` conflicts
  with another package, install dbt in a separate venv (`dbt` is only needed for `--engine dbt`, `dbt` command and the dbt test) or relax the pin.
- **PowerShell blocks scripts** - `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

## Pipeline behaviour
- **Run ended `PARTIAL_FAILURE`** - at least one input file failed (corrupted, missing required column, unexpected column under policy `fail`). `python -m src.cli status` and the dashboard *Pipeline Runs* page show the reason; fix the file and rerun - unchanged good files are skipped.
- **`QualityGateError`** - a CRITICAL rule exceeded its threshold (commonly `rejection_rate_acceptable`: > 30 % rows rejected, often a wrong file/format). Read `data/reports/quality/<run>__silver.txt`. Silver was not changed. Adjust `config/quality_rules.yaml` only if the data is legitimately that dirty.
- **`PipelineLockedError`** - another run is active or crashed. Wait, or `python -m src.cli unlock` if you are sure nothing is running (locks older than 120 min are ignored automatically).
- **Run stuck at `RUNNING`** - an earlier process was killed; the next run marks it FAILED. Interrupted partition swaps are rolled forward automatically.
- **Reprocess a file on purpose** - `run --force`. **Start over** - `.\scripts\reset_demo_data.ps1` (asks for confirmation; keeps `data\sample` unless `-IncludeSample`).
- **Simulate failures** - `$env:DATATRUST_FAULT_INJECTION="fail_at=quality_rules"` (also `commit_mid`, `load_duckdb`, `after_models`, any step name), run, then unset it and rerun.

## DuckDB / dbt / dashboard
- **`Cannot open DuckDB file ... lock`** - only one process may write; stop dbt/pipeline before opening the warehouse for writing. The dashboard opens it read-only, but a running write blocks it - refresh after the run.
- **dbt cannot find the profile** - use `python -m src.cli dbt` (sets `DATATRUST_DUCKDB_PATH` and uses `dbt_project/profiles.yml`).
- **Dashboard shows "No pipeline data yet"** - that is the intended empty state; run the pipeline.
- **PostgreSQL: `MetadataConnectionError`** - the backend is explicit. Start Postgres (`.\scripts\docker.ps1 -Action postgres`), check `DATATRUST_PG_*`, or switch `DATATRUST_METADATA_BACKEND=sqlite`.

## Docker
- Give Docker Desktop >= 4 GB (Spark) / 6 GB (Airflow). Commands in `docker compose run ...` execute inside Linux containers; PowerShell scripts run on the Windows host.
- Airflow compose requires `AIRFLOW_FERNET_KEY`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`, `POSTGRES_PASSWORD` in `.env` (placeholders only in `.env.example`).
