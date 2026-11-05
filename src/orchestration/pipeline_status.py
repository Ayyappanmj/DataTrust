"""Run-level status helpers shared by the CLI and the dashboard."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.metadata.models import RUN_SUCCEEDED


def _parse(ts: Any) -> datetime | None:
    if not ts:
        return None
    parsed = datetime.fromisoformat(str(ts))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def summarize_runs(runs: list[dict]) -> dict:
    """Success rate, latest successful run and data freshness from a list of run rows."""
    finished = [r for r in runs if r["status"] != "RUNNING"]
    ok = [r for r in finished if r["status"] == RUN_SUCCEEDED]
    latest_ok = max(ok, key=lambda r: str(r["started_at"]), default=None)
    freshness_hours = None
    if latest_ok:
        ended = _parse(latest_ok["ended_at"] or latest_ok["started_at"])
        freshness_hours = round((datetime.now(timezone.utc) - ended).total_seconds() / 3600, 2)
    return {
        "runs": len(runs), "finished": len(finished), "succeeded": len(ok),
        "success_rate_pct": round(100.0 * len(ok) / len(finished), 1) if finished else None,
        "latest_successful_run": latest_ok["run_id"] if latest_ok else None,
        "freshness_hours": freshness_hours,
    }
