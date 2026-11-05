"""Small DuckDB helper: connection context manager and safe, parameterised execution."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT
from src.common.exceptions import WarehouseError


@contextmanager
def connect(path: Path, read_only: bool = False) -> Iterator[Any]:
    """Open the warehouse file (created on first use) and always close it."""
    try:
        import duckdb
    except ImportError as exc:
        raise WarehouseError("duckdb is not installed; run: pip install -r requirements.txt") from exc
    path = Path(path)
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = duckdb.connect(str(path), read_only=read_only)
    except Exception as exc:  # noqa: BLE001
        raise WarehouseError(
            f"Cannot open DuckDB file {path}: {exc}. Another process (dashboard, dbt) may be "
            "holding a write lock."
        ) from exc
    try:
        yield conn
    finally:
        conn.close()


def execute(conn: Any, sql: str, params: list | tuple | None = None) -> Any:
    """Run one statement with bound parameters; wrap failures with the SQL for diagnosis."""
    try:
        return conn.execute(sql, params or [])
    except Exception as exc:  # noqa: BLE001
        raise WarehouseError(f"SQL failed: {exc}\n--- SQL ---\n{sql[:600]}") from exc


def init_schema(conn: Any) -> None:
    """Create the source tables (idempotent) from sql/warehouse_schema.sql."""
    ddl = (PROJECT_ROOT / "sql" / "warehouse_schema.sql").read_text(encoding="utf-8")
    cleaned = "\n".join(line for line in ddl.splitlines() if not line.strip().startswith("--"))
    for statement in (s.strip() for s in cleaned.split(";")):
        if statement:
            execute(conn, statement)


def table_exists(conn: Any, schema: str, table: str) -> bool:
    row = execute(
        conn,
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return bool(row and row[0])
