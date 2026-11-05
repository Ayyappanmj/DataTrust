"""DataTrust command-line interface:  python -m src.cli <command> [options]"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from src.common.config import Settings
from src.common.exceptions import DataTrustError
from src.common.logging_config import get_logger, setup_logging

log = get_logger("datatrust.cli")
INGEST_STEPS = ["discover_files", "validate_files", "ingest_bronze", "schema_checks", "run_summary"]
DEFAULT_MONTHS = ["2023-01", "2023-02"]


def _settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env()
    overrides = {}
    if getattr(args, "engine", None):
        overrides["modeling_engine"] = args.engine
    if getattr(args, "partition_scheme", None):
        overrides["partition_scheme"] = args.partition_scheme
    return settings.with_overrides(**overrides) if overrides else settings


def _exit_code(status: str) -> int:
    return 0 if status == "SUCCEEDED" else 2


def cmd_generate_sample(args: argparse.Namespace) -> int:
    from src.sample.generator import generate_samples

    out = Path(args.out)
    paths = generate_samples(out, args.months or DEFAULT_MONTHS, valid_rows=args.rows,
                             as_parquet=args.parquet)
    for path in paths:
        print(f"wrote {path}")
    return 0


def _run(args: argparse.Namespace, steps: list[str] | None) -> int:
    from src.orchestration.pipeline_runner import PipelineRunner

    settings = _settings(args)
    runner = PipelineRunner(settings)
    try:
        ctx = runner.run(Path(args.input), force=args.force, steps=steps,
                         force_models=getattr(args, "force_models", False))
        run = runner.repo.get_run(ctx.run_id)
        print(f"run {ctx.run_id}: {run['status']}  {ctx.counts}")
        for entry in ctx.entries():
            note = f"  FAILED: {ctx.failed[entry.checksum]}" if entry.checksum in ctx.failed else ""
            print(f"  {Path(entry.path).name:<45} {entry.action:<18}{note}")
        return _exit_code(run["status"])
    finally:
        runner.close()


def cmd_ingest(args: argparse.Namespace) -> int:
    return _run(args, INGEST_STEPS)


def cmd_run(args: argparse.Namespace) -> int:
    return _run(args, None)


def cmd_backfill(args: argparse.Namespace) -> int:
    from src.orchestration.pipeline_runner import PipelineRunner

    runner = PipelineRunner(_settings(args))
    try:
        ctx = runner.backfill(args.start, args.end)
        status = runner.repo.get_run(ctx.run_id)["status"]
        print(f"backfill {ctx.run_id} [{args.start}..{args.end}]: {status}  {ctx.counts}")
        return _exit_code(status)
    finally:
        runner.close()


def cmd_quality(args: argparse.Namespace) -> int:
    from src.metadata.repository import open_repository
    from src.orchestration.pipeline_runner import quality_only

    settings = _settings(args)
    repo = open_repository(settings)
    try:
        for r in quality_only(settings, repo):
            print(f"{r.rule_name:<30}{r.records_failed:>8}/{r.records_evaluated:<8} {r.status}")
        return 0
    finally:
        repo.close()


def cmd_models(args: argparse.Namespace) -> int:
    from src.warehouse import warehouse_loader

    settings = _settings(args)
    built = (warehouse_loader.build_models(settings) if settings.modeling_engine == "duckdb"
             else (warehouse_loader.run_dbt(settings, "build") or list(warehouse_loader.model_files())))
    print("built:", ", ".join(built))
    return 0


def cmd_dbt(args: argparse.Namespace) -> int:
    from src.warehouse import warehouse_loader

    warehouse_loader.run_dbt(_settings(args), args.dbt_command)
    print(f"dbt {args.dbt_command}: OK")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    from src.metadata.repository import open_repository
    from src.orchestration.pipeline_status import summarize_runs

    settings = _settings(args)
    repo = open_repository(settings)
    try:
        runs = repo.list_runs(20)
        print(json.dumps(summarize_runs(repo.list_runs(1000)), indent=2))
        for r in runs[:10]:
            print(f"{r['run_id']:<34}{r['status']:<17}read={r['records_read']} "
                  f"rejected={r['records_rejected']} written={r['records_written']}")
        print("manifest:")
        for m in repo.list_manifest():
            print(f"  {m['file_name']:<40} ingest={m['ingest_status']:<10} "
                  f"process={m['process_status']:<10} active={m['active']}")
        return 0
    finally:
        repo.close()


def cmd_lineage(args: argparse.Namespace) -> int:
    from src.metadata.repository import open_repository

    repo = open_repository(_settings(args))
    try:
        files = repo.contributing_files(args.dataset)
        if not files:
            print(f"No lineage recorded for {args.dataset}")
        for f in files:
            print(f"{f['file']}  checksum={f['checksum'][:12]}  runs={f['run_ids']}")
        return 0
    finally:
        repo.close()


def cmd_reset(args: argparse.Namespace) -> int:
    settings = _settings(args)
    if not args.yes:
        answer = input(f"Delete generated data under {settings.data_root}? Type RESET to confirm: ")
        if answer.strip() != "RESET":
            print("Aborted; nothing deleted.")
            return 1
    targets = [settings.bronze_dir.parent, settings.silver_dir.parent, settings.quarantine_dir.parent,
               settings.gold_dir, settings.reports_dir, settings.staging_dir, settings.state_dir,
               settings.trash_dir, settings.warehouse_path.parent, settings.sqlite_path.parent]
    if args.include_sample:
        targets += [settings.sample_dir]
    root = settings.data_root.resolve()
    for target in targets:
        resolved = target.resolve()
        if root not in resolved.parents:
            print(f"refusing to delete outside data root: {resolved}")
            continue
        if resolved.exists():
            shutil.rmtree(resolved)
            resolved.mkdir(parents=True, exist_ok=True)
            (resolved / ".gitkeep").touch()
            print(f"cleared {resolved}")
    settings.lock_path.unlink(missing_ok=True)
    if settings.metadata_backend == "postgres":
        from src.metadata.repository import open_repository

        repo = open_repository(settings)
        try:
            repo.reset()
        finally:
            repo.close()
    return 0


def cmd_step(args: argparse.Namespace) -> int:
    from src.orchestration.pipeline_runner import PipelineRunner

    runner = PipelineRunner(_settings(args))
    try:
        backfill = (args.start, args.end) if args.start and args.end else None
        runner.run_single_step(args.step, args.run_id, args.input or "", args.force, backfill)
        print(f"step {args.step}: OK")
        return 0
    finally:
        runner.close()


def cmd_finalize_failed(args: argparse.Namespace) -> int:
    from src.orchestration.pipeline_runner import PipelineRunner

    runner = PipelineRunner(_settings(args))
    try:
        runner.fail_run_externally(args.run_id, args.message)
        return 0
    finally:
        runner.close()


def cmd_benchmark(args: argparse.Namespace) -> int:
    from benchmarks.partition_benchmark import run_benchmark

    result = run_benchmark(_settings(args), rows=args.rows, days=args.days, repeats=args.repeats)
    print(json.dumps(result["summary"], indent=2))
    return 0


def cmd_validate_config(args: argparse.Namespace) -> int:
    from src.quality.rules import load_rules

    settings = _settings(args)
    rules = load_rules(settings)
    print(f"settings OK (metadata backend={settings.metadata_backend}, "
          f"partitions={settings.partition_scheme}); {len(rules)} quality rules loaded")
    return 0


def cmd_unlock(args: argparse.Namespace) -> int:
    settings = _settings(args)
    settings.lock_path.unlink(missing_ok=True)
    print("lock removed")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datatrust", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name: str, func, help_: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_)
        p.set_defaults(func=func)
        p.add_argument("--engine", choices=["duckdb", "dbt"], help="modeling engine override")
        p.add_argument("--partition-scheme", choices=["daily", "monthly"], dest="partition_scheme")
        return p

    p = add("generate-sample", cmd_generate_sample, "write deterministic synthetic taxi files")
    p.add_argument("--out", default="data/sample")
    p.add_argument("--months", nargs="*", help="e.g. 2023-01 2023-02 2023-03")
    p.add_argument("--rows", type=int, default=1000, help="valid rows per month (plus injected bad rows)")
    p.add_argument("--parquet", action="store_true", help="write Parquet instead of CSV (needs pyarrow)")
    for name, func, text in (("ingest", cmd_ingest, "discover, validate and write Bronze"),
                             ("run", cmd_run, "run the full pipeline")):
        p = add(name, func, text)
        p.add_argument("--input", default="data/sample")
        p.add_argument("--force", action="store_true", help="reprocess unchanged files (controlled)")
        p.add_argument("--force-models", action="store_true", dest="force_models")
    p = add("backfill", cmd_backfill, "rebuild a pickup-date range from Bronze")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    add("quality", cmd_quality, "re-run quality rules on the warehouse")
    add("models", cmd_models, "(re)build analytical models")
    p = add("dbt", cmd_dbt, "run a dbt command against the warehouse")
    p.add_argument("--dbt-command", default="build", choices=["build", "run", "test", "compile", "seed"])
    add("status", cmd_status, "show runs and manifest")
    p = add("lineage", cmd_lineage, "which files/runs contributed to a dataset")
    p.add_argument("--dataset", default="gold.daily_trip_metrics")
    p = add("reset", cmd_reset, "delete generated data (asks for confirmation)")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--include-sample", action="store_true")
    p = add("step", cmd_step, "run a single pipeline step (used by Airflow)")
    p.add_argument("--step", required=True)
    p.add_argument("--run-id", required=True, dest="run_id")
    p.add_argument("--input", default="")
    p.add_argument("--force", action="store_true")
    p.add_argument("--start")
    p.add_argument("--end")
    p = add("finalize-failed", cmd_finalize_failed, "mark a RUNNING run FAILED (Airflow callback)")
    p.add_argument("--run-id", required=True, dest="run_id")
    p.add_argument("--message", default="task failed")
    p = add("benchmark", cmd_benchmark, "partitioning benchmark")
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--repeats", type=int, default=3)
    add("validate-config", cmd_validate_config, "validate settings and quality config")
    add("unlock", cmd_unlock, "remove a stale pipeline lock")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
        setup_logging(settings.log_level, settings.log_json)
        return args.func(args)
    except DataTrustError as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return 1
    except KeyboardInterrupt:
        log.error("Interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
