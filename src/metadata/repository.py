"""Metadata repository with two explicit backends: SQLite (lightweight) and PostgreSQL.

The backend is chosen by ``Settings.metadata_backend``. There is **no silent fallback**:
if PostgreSQL is selected and unreachable, ``MetadataConnectionError`` is raised.
All queries are parameterised and every write happens inside a transaction.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT, Settings
from src.common.exceptions import MetadataConnectionError, MetadataError

_MANIFEST_COLUMNS = {
    "file_path", "file_name", "file_format", "size_bytes", "record_count", "columns_json",
    "schema_fingerprint", "ingest_status", "process_status", "active", "first_seen_at",
    "ingested_at", "processed_at", "last_run_id", "bronze_path", "error_message",
}
_RUN_COLUMNS = {
    "status", "ended_at", "duration_seconds", "input_files", "records_read",
    "records_accepted", "records_rejected", "records_written", "error_summary", "params",
}
_TABLES = (
    "pipeline_runs", "ingestion_manifest", "datasets", "quality_results", "processing_errors",
    "lineage_edges", "backfill_history", "partition_metadata",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


class MetadataRepository:
    """Shared implementation; subclasses only provide the connection and the DDL file."""

    backend = "abstract"
    placeholder_style = "%s"

    def __init__(self) -> None:
        self._conn: Any = None

    # ---- connection plumbing (implemented by subclasses) ----------------
    def _connect(self) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def _ddl_path(self) -> Path:  # pragma: no cover - abstract
        raise NotImplementedError

    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> MetadataRepository:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _sql(self, sql: str) -> str:
        return sql

    @contextmanager
    def _tx(self) -> Iterator[Any]:
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def _execute(self, sql: str, params: tuple = ()) -> None:
        with self._tx() as cur:
            cur.execute(self._sql(sql), params)

    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        cur = self.conn.cursor()
        try:
            cur.execute(self._sql(sql), params)
            names = [d[0] for d in cur.description]
            rows = [{n: _jsonable(v) for n, v in zip(names, r, strict=True)} for r in cur.fetchall()]
            if self.backend == "postgres":
                self.conn.rollback()  # end the implicit read transaction
            return rows
        finally:
            cur.close()

    def _upsert(self, table: str, key: dict, values: dict) -> None:
        cols = {**key, **values}
        names = ", ".join(cols)
        marks = ", ".join(["%s"] * len(cols))
        keycols = ", ".join(key)
        if values:
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in values)
            conflict = f"ON CONFLICT ({keycols}) DO UPDATE SET {updates}"
        else:
            conflict = f"ON CONFLICT ({keycols}) DO NOTHING"
        self._execute(
            f"INSERT INTO {table} ({names}) VALUES ({marks}) {conflict}", tuple(cols.values())
        )

    # ---- schema ---------------------------------------------------------
    def init_schema(self) -> None:
        raw = self._ddl_path().read_text(encoding="utf-8")
        ddl = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--"))
        statements = [s.strip() for s in ddl.split(";") if s.strip()]
        with self._tx() as cur:
            for statement in statements:
                cur.execute(statement)

    def ping(self) -> bool:
        self._query("SELECT 1 AS ok")
        return True

    def reset(self) -> None:
        """Delete every row (used by `reset --yes`)."""
        with self._tx() as cur:
            for table in _TABLES:
                cur.execute(f"DELETE FROM {table}")

    # ---- pipeline runs --------------------------------------------------
    def start_run(self, run_id: str, pipeline_name: str, trigger: str = "cli",
                  params: dict | None = None) -> None:
        self._upsert(
            "pipeline_runs", {"run_id": run_id},
            {"pipeline_name": pipeline_name, "trigger_type": trigger, "status": "RUNNING",
             "started_at": utc_now(), "params": json.dumps(params or {}, default=str),
             "ended_at": None, "duration_seconds": None, "error_summary": None},
        )

    def update_run(self, run_id: str, **fields: Any) -> None:
        bad = set(fields) - _RUN_COLUMNS
        if bad:
            raise MetadataError(f"Unknown run columns: {sorted(bad)}")
        if not fields:
            return
        sets = ", ".join(f"{c} = %s" for c in fields)
        self._execute(f"UPDATE pipeline_runs SET {sets} WHERE run_id = %s",
                      (*fields.values(), run_id))

    def finish_run(self, run_id: str, status: str, error_summary: str | None = None,
                   **counts: int) -> None:
        run = self.get_run(run_id)
        ended = datetime.now(timezone.utc)
        duration = None
        if run and run.get("started_at"):
            started = datetime.fromisoformat(str(run["started_at"]))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            duration = round((ended - started).total_seconds(), 3)
        self.update_run(run_id, status=status, ended_at=ended.isoformat(timespec="seconds"),
                        duration_seconds=duration, error_summary=error_summary, **counts)

    def get_run(self, run_id: str) -> dict | None:
        rows = self._query("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,))
        return rows[0] if rows else None

    def list_runs(self, limit: int = 200) -> list[dict]:
        return self._query("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT %s", (limit,))

    def fail_stale_runs(self, exclude_run_id: str, timeout_minutes: int) -> list[str]:
        """Mark RUNNING runs older than the timeout as FAILED (they were interrupted)."""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)).isoformat()
        stale = self._query(
            "SELECT run_id FROM pipeline_runs WHERE status = 'RUNNING' AND run_id <> %s "
            "AND started_at < %s", (exclude_run_id, cutoff))
        for row in stale:
            self.finish_run(row["run_id"], "FAILED",
                            "Run was interrupted: still RUNNING after the lock timeout")
        return [r["run_id"] for r in stale]

    # ---- ingestion manifest ----------------------------------------------
    def get_manifest(self, checksum: str) -> dict | None:
        rows = self._query("SELECT * FROM ingestion_manifest WHERE checksum = %s", (checksum,))
        return rows[0] if rows else None

    def get_active_manifest_for_path(self, file_path: str) -> dict | None:
        rows = self._query(
            "SELECT * FROM ingestion_manifest WHERE file_path = %s AND active = 1 "
            "ORDER BY first_seen_at DESC", (file_path,))
        return rows[0] if rows else None

    def upsert_manifest(self, checksum: str, **fields: Any) -> None:
        """Insert a manifest row, or update only the given fields of an existing one."""
        bad = set(fields) - _MANIFEST_COLUMNS
        if bad:
            raise MetadataError(f"Unknown manifest columns: {sorted(bad)}")
        if self.get_manifest(checksum) is None:
            fields.setdefault("first_seen_at", utc_now())
            names = ", ".join(["checksum", *fields])
            marks = ", ".join(["%s"] * (len(fields) + 1))
            self._execute(f"INSERT INTO ingestion_manifest ({names}) VALUES ({marks})",
                          (checksum, *fields.values()))
        elif fields:
            sets = ", ".join(f"{c} = %s" for c in fields)
            self._execute(f"UPDATE ingestion_manifest SET {sets} WHERE checksum = %s",
                          (*fields.values(), checksum))

    def list_manifest(self, active_only: bool = False) -> list[dict]:
        where = "WHERE active = 1" if active_only else ""
        return self._query(f"SELECT * FROM ingestion_manifest {where} ORDER BY first_seen_at")

    def set_active(self, checksum: str, active: bool) -> None:
        self._execute("UPDATE ingestion_manifest SET active = %s WHERE checksum = %s",
                      (1 if active else 0, checksum))

    # ---- datasets ----------------------------------------------------------
    def upsert_dataset(self, name: str, layer: str, location: str, partition_column: str | None,
                       description: str) -> None:
        self._upsert("datasets", {"dataset_name": name},
                     {"layer": layer, "location": location, "partition_column": partition_column,
                      "description": description, "updated_at": utc_now()})

    def list_datasets(self) -> list[dict]:
        return self._query("SELECT * FROM datasets ORDER BY dataset_name")

    # ---- quality -----------------------------------------------------------
    def insert_quality_results(self, results: list[dict]) -> None:
        if not results:
            return
        sql = ("INSERT INTO quality_results (run_id, stage, dataset_name, rule_name, "
               "rule_description, severity, action, records_evaluated, records_passed, "
               "records_failed, failure_pct, threshold_pct, status, executed_at) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        with self._tx() as cur:
            for r in results:
                cur.execute(self._sql(sql), (
                    r["run_id"], r["stage"], r["dataset_name"], r["rule_name"],
                    r.get("rule_description"), r["severity"], r.get("action"),
                    r["records_evaluated"], r["records_passed"], r["records_failed"],
                    r["failure_pct"], r.get("threshold_pct"), r["status"], r["executed_at"]))

    def list_quality_results(self, run_id: str | None = None, limit: int = 5000) -> list[dict]:
        if run_id:
            return self._query(
                "SELECT * FROM quality_results WHERE run_id = %s ORDER BY id", (run_id,))
        return self._query("SELECT * FROM quality_results ORDER BY id DESC LIMIT %s", (limit,))

    # ---- errors --------------------------------------------------------------
    def record_error(self, run_id: str, step: str, error: BaseException | str,
                     details: dict | None = None, file_path: str | None = None) -> None:
        error_type = type(error).__name__ if isinstance(error, BaseException) else "Error"
        self._execute(
            "INSERT INTO processing_errors (run_id, step, error_type, message, details, "
            "file_path, occurred_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (run_id, step, error_type, str(error)[:4000],
             json.dumps(details or {}, default=str), file_path, utc_now()))

    def list_errors(self, run_id: str | None = None, limit: int = 500) -> list[dict]:
        if run_id:
            return self._query(
                "SELECT * FROM processing_errors WHERE run_id = %s ORDER BY id", (run_id,))
        return self._query("SELECT * FROM processing_errors ORDER BY id DESC LIMIT %s", (limit,))

    # ---- lineage ---------------------------------------------------------------
    def add_lineage_edge(self, run_id: str, source_dataset: str, transformation: str,
                         output_dataset: str, source_checksum: str | None = None,
                         output_record_count: int | None = None, status: str = "SUCCEEDED") -> None:
        self._execute(
            "INSERT INTO lineage_edges (run_id, source_dataset, source_checksum, transformation, "
            "output_dataset, output_record_count, processed_at, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_id, source_dataset, source_checksum, transformation, output_dataset,
             output_record_count, utc_now(), status))

    def list_lineage(self, limit: int = 2000) -> list[dict]:
        return self._query("SELECT * FROM lineage_edges ORDER BY id DESC LIMIT %s", (limit,))

    def lineage_upstream(self, output_dataset: str) -> list[dict]:
        """All SUCCEEDED edges upstream of ``output_dataset`` (dataset-level lineage).

        Edges whose source file/checksum has been superseded (manifest.active = 0) are excluded.
        """
        return self._query(
            """
            WITH RECURSIVE up(dataset, depth) AS (
                SELECT CAST(%s AS TEXT), 0
                UNION
                SELECT e.source_dataset, up.depth + 1
                FROM lineage_edges e JOIN up ON e.output_dataset = up.dataset
                WHERE e.status = 'SUCCEEDED'
            )
            SELECT e.run_id, e.source_dataset, e.source_checksum, e.transformation,
                   e.output_dataset, e.output_record_count, e.processed_at, MIN(up.depth) AS depth
            FROM lineage_edges e
            JOIN up ON e.output_dataset = up.dataset
            LEFT JOIN ingestion_manifest m ON m.checksum = e.source_checksum
            WHERE e.status = 'SUCCEEDED' AND (e.source_checksum IS NULL OR m.active IS NULL OR m.active = 1)
            GROUP BY e.run_id, e.source_dataset, e.source_checksum, e.transformation,
                     e.output_dataset, e.output_record_count, e.processed_at
            ORDER BY MIN(up.depth), e.processed_at
            """, (output_dataset,))

    def contributing_files(self, output_dataset: str) -> list[dict]:
        """Which source files and runs contributed to ``output_dataset``?"""
        edges = self.lineage_upstream(output_dataset)
        files: dict[str, dict] = {}
        for edge in edges:
            if str(edge["source_dataset"]).startswith("file:"):
                entry = files.setdefault(edge["source_checksum"], {
                    "file": str(edge["source_dataset"])[5:], "checksum": edge["source_checksum"],
                    "run_ids": set(), "records": edge["output_record_count"]})
                entry["run_ids"].add(edge["run_id"])
        return [{**f, "run_ids": sorted(f["run_ids"])} for f in files.values()]

    # ---- backfills ---------------------------------------------------------------
    def start_backfill(self, backfill_id: str, run_id: str, start: str, end: str,
                       partitions: list[str]) -> None:
        self._upsert("backfill_history", {"backfill_id": backfill_id},
                     {"run_id": run_id, "start_date": start, "end_date": end,
                      "partitions": json.dumps(partitions), "status": "RUNNING",
                      "started_at": utc_now()})

    def finish_backfill(self, backfill_id: str, status: str, records_written: int | None = None,
                        error: str | None = None) -> None:
        self._execute(
            "UPDATE backfill_history SET status = %s, ended_at = %s, records_written = %s, "
            "error_message = %s WHERE backfill_id = %s",
            (status, utc_now(), records_written, error, backfill_id))

    def list_backfills(self) -> list[dict]:
        return self._query("SELECT * FROM backfill_history ORDER BY started_at DESC")

    # ---- partition metadata ---------------------------------------------------------
    def upsert_partition(self, dataset: str, value: str, row_count: int, size_bytes: int,
                         file_count: int, run_id: str) -> None:
        self._upsert("partition_metadata", {"dataset_name": dataset, "partition_value": value},
                     {"row_count": row_count, "size_bytes": size_bytes, "file_count": file_count,
                      "source_run_id": run_id, "updated_at": utc_now(),
                      "warehouse_loaded_at": None})

    def list_partitions(self, dataset: str) -> list[dict]:
        return self._query(
            "SELECT * FROM partition_metadata WHERE dataset_name = %s ORDER BY partition_value",
            (dataset,))

    def partitions_pending_load(self, dataset: str) -> list[dict]:
        return self._query(
            "SELECT * FROM partition_metadata WHERE dataset_name = %s "
            "AND warehouse_loaded_at IS NULL ORDER BY partition_value", (dataset,))

    def mark_partitions_loaded(self, dataset: str, values: list[str]) -> None:
        now = utc_now()
        with self._tx() as cur:
            for value in values:
                cur.execute(self._sql(
                    "UPDATE partition_metadata SET warehouse_loaded_at = %s "
                    "WHERE dataset_name = %s AND partition_value = %s"), (now, dataset, value))


class SqliteRepository(MetadataRepository):
    backend = "sqlite"

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=15, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _ddl_path(self) -> Path:
        return PROJECT_ROOT / "sql" / "metadata_schema_sqlite.sql"

    def _sql(self, sql: str) -> str:
        return sql.replace("%s", "?")


class PostgresRepository(MetadataRepository):
    backend = "postgres"

    def __init__(self, dsn: str) -> None:
        super().__init__()
        self._dsn = dsn

    def _connect(self) -> Any:
        try:
            import psycopg2
        except ImportError as exc:
            raise MetadataConnectionError(
                "psycopg2 is not installed; run: pip install psycopg2-binary") from exc
        try:
            return psycopg2.connect(self._dsn, connect_timeout=5)
        except Exception as exc:  # psycopg2.OperationalError and friends
            raise MetadataConnectionError(
                f"Cannot connect to PostgreSQL ({type(exc).__name__}): {exc}. "
                "DATATRUST_METADATA_BACKEND=postgres is explicit, so there is no fallback to "
                "SQLite. Start the database or switch the backend to sqlite."
            ) from exc

    def _ddl_path(self) -> Path:
        return PROJECT_ROOT / "sql" / "metadata_schema.sql"


def open_repository(settings: Settings, init: bool = True) -> MetadataRepository:
    """Create the repository selected in settings (explicit choice, no fallback)."""
    repo: MetadataRepository
    if settings.metadata_backend == "postgres":
        repo = PostgresRepository(settings.pg_dsn())
    else:
        repo = SqliteRepository(settings.sqlite_path)
    if init:
        repo.init_schema()
    return repo
