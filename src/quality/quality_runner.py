"""Turn raw rule statistics into RuleResults, persist them and enforce the quality gate."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.common.config import SEVERITY_RANK, Settings
from src.common.exceptions import QualityGateError
from src.common.logging_config import get_logger
from src.quality.rules import Rule, RuleResult, applicable, load_rules
from src.quality.validators import sql_rule_stats, table_columns

log = get_logger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_results(
    dataset: str,
    stage: str,
    run_id: str,
    rules: list[Rule],
    stats: dict[str, tuple[int, int]],
    executed_at: str | None = None,
) -> list[RuleResult]:
    """One RuleResult per rule that has statistics."""
    executed_at = executed_at or now_iso()
    results = []
    for rule in rules:
        if rule.name not in stats:
            continue
        evaluated, failed = stats[rule.name]
        results.append(RuleResult(
            dataset_name=dataset, rule_name=rule.name, rule_description=rule.describe(),
            records_evaluated=evaluated, records_failed=failed, severity=rule.severity,
            action=rule.action, max_failure_pct=rule.max_failure_pct, stage=stage,
            run_id=run_id, executed_at=executed_at,
        ))
    return results


def blocking_failures(results: list[RuleResult], fail_on_severity: str) -> list[RuleResult]:
    """Results that are FAILED and whose severity is at or above the configured gate."""
    threshold = SEVERITY_RANK[fail_on_severity]
    return [r for r in results
            if r.status == "FAILED" and SEVERITY_RANK[r.severity] >= threshold]


def enforce_gate(results: list[RuleResult], settings: Settings) -> None:
    """Raise QualityGateError if any blocking rule failed."""
    blockers = blocking_failures(results, settings.fail_on_severity)
    if blockers:
        detail = "; ".join(
            f"{r.rule_name} failed {r.records_failed}/{r.records_evaluated} "
            f"({r.failure_pct}% > {r.max_failure_pct}%)" for r in blockers
        )
        raise QualityGateError(f"Quality gate failed [{settings.fail_on_severity}+]: {detail}")


def run_sql_quality(
    conn: Any, table: str, dataset: str, stage: str, run_id: str,
    settings: Settings | None = None, rules: list[Rule] | None = None,
) -> list[RuleResult]:
    """Evaluate all applicable rules on a SQL table (used for the warehouse check)."""
    rules = rules if rules is not None else load_rules(settings)
    usable = [r for r in applicable(rules, table_columns(conn, table)) if r.kind != "dataset"]
    _, stats = sql_rule_stats(conn, table, usable)
    return build_results(dataset, stage, run_id, usable, stats)


def persist_results(repo: Any, results: list[RuleResult]) -> None:
    repo.insert_quality_results([r.to_dict() for r in results])
