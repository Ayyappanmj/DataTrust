"""Rule evaluation engines.

* ``spark_rule_stats``  - evaluates row-level rules on a Spark DataFrame in one pass.
* ``sql_rule_stats``    - evaluates the same rules on any DB-API connection (DuckDB, SQLite).
* ``validate_gold_contract`` - Pandera schema check of the daily metrics table.

Both stats functions return ``{rule_name: (records_evaluated, records_failed)}``.
"""
from __future__ import annotations

from typing import Any

from src.quality.rules import Rule


def _expression_rules(rules: list[Rule]) -> list[Rule]:
    return [r for r in rules if r.kind == "expression"]


def spark_rule_stats(df: Any, rules: list[Rule]) -> tuple[int, dict[str, tuple[int, int]]]:
    """Return (total_rows, stats) for expression rules, using one Spark aggregation."""
    from pyspark.sql import functions as F

    expr_rules = _expression_rules(rules)
    aggs = [F.count(F.lit(1)).alias("__total")]
    for idx, rule in enumerate(expr_rules):
        # NOT(NULL) is NULL -> F.when treats it as not-true -> counted as pass.
        aggs.append(F.sum(F.when(~F.expr(rule.sql()), 1).otherwise(0)).alias(f"f_{idx}"))
    row = df.agg(*aggs).collect()[0]
    total = int(row["__total"] or 0)
    stats = {r.name: (total, int(row[f"f_{i}"] or 0)) for i, r in enumerate(expr_rules)}
    return total, stats


def spark_reject_reasons(rules: list[Rule]) -> Any:
    """Column holding an array of the names of every 'reject' rule the row violates."""
    from pyspark.sql import functions as F

    parts = [
        F.when(~F.expr(r.sql()), F.lit(r.name))
        for r in _expression_rules(rules)
        if r.action == "reject"
    ]
    return F.array_compact(F.array(*parts))


def table_columns(conn: Any, table: str) -> set[str]:
    """Column names of a table. Works for DuckDB and SQLite connections."""
    try:
        rows = conn.execute(f"SELECT * FROM {table} LIMIT 0")
        return {d[0] for d in rows.description}
    except Exception as exc:  # noqa: BLE001 - re-raised with context
        raise RuntimeError(f"Cannot inspect table {table!r}: {exc}") from exc


def sql_rule_stats(conn: Any, table: str, rules: list[Rule]) -> tuple[int, dict[str, tuple[int, int]]]:
    """Evaluate expression rules (and duplicate detection) with plain SQL."""
    expr_rules = _expression_rules(rules)
    select = ["COUNT(*)"]
    for rule in expr_rules:
        select.append(f"SUM(CASE WHEN NOT ({rule.sql()}) THEN 1 ELSE 0 END)")
    row = conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchone()
    total = int(row[0] or 0)
    stats = {r.name: (total, int(row[i + 1] or 0)) for i, r in enumerate(expr_rules)}
    for rule in rules:
        if rule.kind == "duplicate":
            dup = conn.execute(
                f"SELECT COUNT(*) - COUNT(DISTINCT record_id) FROM {table}"
            ).fetchone()[0]
            stats[rule.name] = (total, int(dup or 0))
    return total, stats


def validate_gold_contract(daily_metrics: Any) -> list[str]:
    """Validate the daily metrics table with Pandera. Returns a list of problems (empty = ok)."""
    import pandera as pa

    schema = pa.DataFrameSchema(
        {
            "pickup_date": pa.Column(nullable=False, unique=True),
            "trips": pa.Column(int, pa.Check.ge(1)),
            "fare_revenue": pa.Column(float, pa.Check.ge(0), coerce=True),
            "avg_fare": pa.Column(float, pa.Check.ge(0), coerce=True),
            "avg_trip_minutes": pa.Column(float, pa.Check.gt(0), coerce=True),
            "avg_tip_pct": pa.Column(float, pa.Check.in_range(0, 1000), nullable=True, coerce=True),
        },
        strict=False,
    )
    try:
        schema.validate(daily_metrics, lazy=True)
    except pa.errors.SchemaErrors as exc:
        return [f"{row.column}: {row.check} (failure case: {row.failure_case})"
                for row in exc.failure_cases.itertuples()]
    return []
