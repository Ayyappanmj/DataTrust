"""Read-only data access for the dashboard. Shows real data or a clear empty state - never fakes."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import Settings  # noqa: E402
from src.metadata.repository import open_repository  # noqa: E402


def get_settings() -> Settings:
    return Settings.from_env()


def metadata(method: str, *args, **kwargs) -> list[dict]:
    """Call a repository read method; surface connection errors instead of hiding them."""
    settings = get_settings()
    try:
        repo = open_repository(settings)
    except Exception as exc:  # noqa: BLE001 - shown to the user
        st.error(f"Metadata store unavailable ({settings.metadata_backend}): {exc}")
        return []
    try:
        return getattr(repo, method)(*args, **kwargs)
    finally:
        repo.close()


def frame(method: str, *args, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(metadata(method, *args, **kwargs))


def warehouse_query(sql: str, params: list | None = None) -> pd.DataFrame | None:
    """Run SQL on the DuckDB warehouse (read-only). None if the warehouse does not exist yet."""
    settings = get_settings()
    if not settings.warehouse_path.exists():
        return None
    import duckdb

    try:
        conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot open warehouse (is a pipeline run writing to it?): {exc}")
        return None
    try:
        return conn.execute(sql, params or []).df()
    except Exception as exc:  # noqa: BLE001 - e.g. table not built yet
        if "does not exist" in str(exc) or "Catalog Error" in str(exc):
            return None
        st.error(f"Query failed: {exc}")
        return None
    finally:
        conn.close()


def empty_state(message: str = "No pipeline data yet.") -> None:
    st.info(f"{message} Generate sample data and run the pipeline:\n\n"
            "`python -m src.cli generate-sample` then `python -m src.cli run --input data/sample`")


def date_range_filter(df: pd.DataFrame, column: str, label: str = "Date range") -> pd.DataFrame:
    """Sidebar date-range filter on a column of dates/timestamps."""
    if df.empty or column not in df:
        return df
    dates = pd.to_datetime(df[column], errors="coerce", utc=True).dt.tz_localize(None)
    lo, hi = dates.min().date(), dates.max().date()
    chosen = st.sidebar.date_input(label, (lo, hi), min_value=lo, max_value=hi, key=f"{label}-{column}")
    if isinstance(chosen, tuple) and len(chosen) == 2:
        mask = (dates.dt.date >= chosen[0]) & (dates.dt.date <= chosen[1])
        return df[mask]
    return df
