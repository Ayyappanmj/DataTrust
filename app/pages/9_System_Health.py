import importlib.metadata as md
import platform
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st  # noqa: E402

from app.components.data_access import frame, get_settings  # noqa: E402
from src.metadata.repository import open_repository  # noqa: E402
from src.orchestration.pipeline_status import summarize_runs  # noqa: E402

st.set_page_config(page_title="System Health", layout="wide")
st.title("System Health")
settings = get_settings()
rows = [("Python", platform.python_version()), ("Metadata backend", settings.metadata_backend),
        ("Java on PATH", "yes" if shutil.which("java") else "NO (needed for Spark)"),
        ("Warehouse file", "present" if settings.warehouse_path.exists() else "missing"),
        ("Free disk (GB)", round(shutil.disk_usage(settings.data_root.anchor or "/").free / 1e9, 1))]
try:
    repo = open_repository(settings)
    repo.ping()
    repo.close()
    rows.append(("Metadata store", "reachable"))
except Exception as exc:  # noqa: BLE001
    rows.append(("Metadata store", f"UNREACHABLE: {exc}"))
for pkg in ("pyspark", "duckdb", "pyarrow", "pandas", "dbt-core", "dbt-duckdb", "pandera", "streamlit"):
    try:
        rows.append((pkg, md.version(pkg)))
    except md.PackageNotFoundError:
        rows.append((pkg, "not installed"))
st.table({"check": [r[0] for r in rows], "value": [str(r[1]) for r in rows]})
runs = frame("list_runs", 500)
if not runs.empty:
    st.json(summarize_runs(runs.to_dict("records")))
    running = runs[runs["status"] == "RUNNING"]
    if not running.empty:
        st.warning(f"{len(running)} run(s) currently marked RUNNING (stale ones are failed on next run).")
