"""Machine-readable (JSON) and human-readable (text) quality reports."""
from __future__ import annotations

import json
from pathlib import Path

from src.quality.rules import RuleResult


def build_report(run_id: str, stage: str, results: list[RuleResult], extra: dict | None = None) -> dict:
    counts = {"PASSED": 0, "WARN": 0, "FAILED": 0}
    for r in results:
        counts[r.status] += 1
    return {
        "run_id": run_id,
        "stage": stage,
        "overall_status": "FAILED" if counts["FAILED"] else ("WARN" if counts["WARN"] else "PASSED"),
        "rule_status_counts": counts,
        "rules": [r.to_dict() for r in results],
        **(extra or {}),
    }


def render_text(report: dict) -> str:
    lines = [
        f"DataTrust quality report  run={report['run_id']}  stage={report['stage']}",
        f"Overall: {report['overall_status']}  {report['rule_status_counts']}",
        "",
        f"{'rule':<30}{'sev':<10}{'action':<8}{'eval':>9}{'failed':>9}{'fail%':>8}  status",
        "-" * 85,
    ]
    for r in report["rules"]:
        lines.append(
            f"{r['rule_name']:<30}{r['severity']:<10}{r['action']:<8}"
            f"{r['records_evaluated']:>9}{r['records_failed']:>9}{r['failure_pct']:>8.2f}  {r['status']}"
        )
    return "\n".join(lines) + "\n"


def write_report(report: dict, reports_dir: Path) -> tuple[Path, Path]:
    """Write <run>_<stage>.json and .txt under reports_dir/quality and return both paths."""
    out = reports_dir / "quality"
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{report['run_id']}__{report['stage']}"
    json_path, txt_path = out / f"{stem}.json", out / f"{stem}.txt"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    txt_path.write_text(render_text(report), encoding="utf-8")
    return json_path, txt_path
