"""Load Silver partitions into DuckDB and build the analytical models.

Two interchangeable ways to build models from the *same* SQL files in dbt_project/models:
  * ``build_models_sql``  - renders ref()/source() and runs the SQL directly (lightweight mode).
  * ``run_dbt``           - runs dbt-core (dbt-duckdb) for tests, docs and lineage.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT, Settings
from src.common.exceptions import ModelingError, WarehouseError
from src.common.logging_config import get_logger
from src.metadata.models import DATASET_SILVER
from src.storage.partition_manager import data_files, partition_dir
from src.warehouse.duckdb_client import connect, execute, init_schema

log = get_logger(__name__)
MODELS_DIR = PROJECT_ROOT / "dbt_project" / "models"
LAYER_SCHEMA = {"staging": "staging", "intermediate": "intermediate", "marts": "gold"}
_REF = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
_SOURCE = re.compile(r"\{\{\s*source\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)\s*\}\}")
_CONFIG = re.compile(r"\{\{\s*config\([^}]*\)\s*\}\}")


# ------------------------------------------------------------------ silver loading
def load_silver_partitions(conn: Any, settings: Settings, values: list[str]) -> int:
    """Replace the given partitions of ``silver_trips`` with the current Silver Parquet files.

    DELETE + INSERT per partition inside one transaction => re-running never duplicates rows.
    A partition with no Parquet files on disk (deleted by a backfill/replacement) is just removed.
    """
    pcol = settings.partition_column
    loaded = 0
    execute(conn, "BEGIN TRANSACTION")
    try:
        for value in values:
            execute(conn, f"DELETE FROM silver_trips WHERE CAST({pcol} AS VARCHAR) = ?", [value])
            pdir = partition_dir(settings.silver_dir, pcol, value)
            files = data_files(pdir) if pdir.is_dir() else []
            if files:
                glob = (pdir / "*.parquet").as_posix()
                execute(conn,
                        "INSERT INTO silver_trips BY NAME "
                        "SELECT * FROM read_parquet(?, hive_partitioning = true)", [glob])
                loaded += 1
        execute(conn, "COMMIT")
    except Exception:
        execute(conn, "ROLLBACK")
        raise
    return loaded


def sync_metadata_tables(conn: Any, repo: Any) -> None:
    """Copy quality results and run history into DuckDB so SQL models can use them."""
    execute(conn, "DELETE FROM quality_results")
    for r in repo.list_quality_results(limit=100000):
        execute(conn, "INSERT INTO quality_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            r["run_id"], r["stage"], r["dataset_name"], r["rule_name"], r["rule_description"],
            r["severity"], r["action"], r["records_evaluated"], r["records_passed"],
            r["records_failed"], r["failure_pct"], r["threshold_pct"], r["status"],
            str(r["executed_at"])])
    execute(conn, "DELETE FROM pipeline_runs")
    for r in repo.list_runs(limit=100000):
        execute(conn, "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
            r["run_id"], r["pipeline_name"], r["status"], str(r["started_at"]),
            str(r["ended_at"]) if r["ended_at"] else None, r["duration_seconds"],
            r["records_read"], r["records_accepted"], r["records_rejected"],
            r["records_written"], r["error_summary"]])


def sync_warehouse(settings: Settings, repo: Any) -> dict:
    """Load every pending Silver partition and refresh metadata tables."""
    pending = repo.partitions_pending_load(DATASET_SILVER)
    values = [p["partition_value"] for p in pending]
    with connect(settings.warehouse_path) as conn:
        init_schema(conn)
        loaded = load_silver_partitions(conn, settings, values) if values else 0
        sync_metadata_tables(conn, repo)
        rows = execute(conn, "SELECT COUNT(*) FROM silver_trips").fetchone()[0]
    if values:
        repo.mark_partitions_loaded(DATASET_SILVER, values)
    log.info("Warehouse synced", extra={"partitions": len(values), "silver_rows": rows})
    return {"partitions_synced": len(values), "partitions_loaded": loaded, "silver_rows": rows}


# ------------------------------------------------------------------ model rendering
def model_files(models_dir: Path = MODELS_DIR) -> dict[str, tuple[str, Path]]:
    """{model_name: (layer, path)} for every .sql model."""
    found = {}
    for layer in LAYER_SCHEMA:
        for path in sorted((models_dir / layer).glob("*.sql")):
            found[path.stem] = (layer, path)
    return found


def render_model(sql: str, models: dict[str, tuple[str, Path]]) -> str:
    """Replace config()/ref()/source() with plain SQL (the only Jinja the models use)."""
    def ref(match: re.Match) -> str:
        name = match.group(1)
        if name not in models:
            raise ModelingError(f"ref('{name}') points to an unknown model")
        return f"{LAYER_SCHEMA[models[name][0]]}.{name}"

    sql = _CONFIG.sub("", sql)
    sql = _REF.sub(ref, sql)
    return _SOURCE.sub(lambda m: f"main.{m.group(1)}", sql).strip()


def execution_order(models: dict[str, tuple[str, Path]]) -> list[str]:
    deps = {n: set(_REF.findall(p.read_text(encoding="utf-8"))) for n, (_, p) in models.items()}
    order: list[str] = []
    while deps:
        ready = sorted(n for n, d in deps.items() if d <= set(order))
        if not ready:
            raise ModelingError(f"Circular model dependencies: {sorted(deps)}")
        for name in ready:
            order.append(name)
            del deps[name]
    return order


def build_models_sql(conn: Any, staging_as: str = "view", models_dir: Path = MODELS_DIR,
                     create_schemas: bool = True) -> list[str]:
    """Create every model in dependency order. Returns the model names built."""
    models = model_files(models_dir)
    if create_schemas:
        for schema in sorted(set(LAYER_SCHEMA.values())):
            execute(conn, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    built = []
    for name in execution_order(models):
        layer, path = models[name]
        select = render_model(path.read_text(encoding="utf-8"), models)
        kind = staging_as if layer == "staging" else "table"
        target = f"{LAYER_SCHEMA[layer]}.{name}"
        for drop_kind in ("VIEW", "TABLE"):
            try:
                conn.execute(f"DROP {drop_kind} IF EXISTS {target}")
            except Exception:  # noqa: BLE001, S110 - wrong object kind; the other DROP handles it
                pass
        execute(conn, f"CREATE {kind.upper()} {target} AS {select}")
        built.append(name)
    return built


def build_models(settings: Settings) -> list[str]:
    """Lightweight mode: build all models in DuckDB, then export gold tables to Parquet."""
    with connect(settings.warehouse_path) as conn:
        init_schema(conn)
        built = build_models_sql(conn)
        settings.gold_dir.mkdir(parents=True, exist_ok=True)
        for name, (layer, _) in model_files().items():
            if layer == "marts":
                out = (settings.gold_dir / f"{name}.parquet").as_posix()
                execute(conn, f"COPY gold.{name} TO '{out}' (FORMAT PARQUET)")
    return built


# ------------------------------------------------------------------ dbt
def ensure_profiles(project_dir: Path) -> None:
    profiles = project_dir / "profiles.yml"
    if not profiles.exists():
        profiles.write_text((project_dir / "profiles.yml.example").read_text(encoding="utf-8"),
                            encoding="utf-8")


def run_dbt(settings: Settings, command: str = "build") -> None:
    """Run ``dbt <command>`` through dbt's programmatic API. Raises ModelingError on failure."""
    try:
        from dbt.cli.main import dbtRunner
    except ImportError as exc:
        raise ModelingError("dbt-core/dbt-duckdb are not installed (pip install -r requirements.txt)") from exc
    project = settings.dbt_project_dir
    ensure_profiles(project)
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["DATATRUST_DUCKDB_PATH"] = str(settings.warehouse_path.resolve())
    args = [command, "--project-dir", str(project), "--profiles-dir", str(project)]
    result = dbtRunner().invoke(args)
    if result.exception is not None:
        raise ModelingError(f"dbt {command} raised: {result.exception}")
    if not result.success:
        failed = []
        for item in getattr(result.result, "results", []) or []:
            if str(item.status) in {"error", "fail"}:
                failed.append(f"{item.node.name}: {item.message}")
        raise ModelingError(f"dbt {command} failed: " + "; ".join(failed or ["see dbt logs"]))
    if command in {"build", "run"}:
        with connect(settings.warehouse_path) as conn:
            settings.gold_dir.mkdir(parents=True, exist_ok=True)
            for name, (layer, _) in model_files().items():
                if layer == "marts":
                    out = (settings.gold_dir / f"{name}.parquet").as_posix()
                    execute(conn, f"COPY gold.{name} TO '{out}' (FORMAT PARQUET)")


def ensure_warehouse_ready(settings: Settings) -> None:
    if not settings.warehouse_path.exists():
        raise WarehouseError(f"Warehouse not found at {settings.warehouse_path}; run the pipeline first")


