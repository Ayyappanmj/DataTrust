import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import date_range_filter, empty_state, frame  # noqa: E402

st.set_page_config(page_title="Pipeline Runs", layout="wide")
st.title("Pipeline Runs")
runs = frame("list_runs", 1000)
if runs.empty:
    empty_state()
    st.stop()
runs = date_range_filter(runs, "started_at")
status = st.sidebar.multiselect("Status", sorted(runs["status"].unique()), default=sorted(runs["status"].unique()))
runs = runs[runs["status"].isin(status)]
st.dataframe(runs.drop(columns=["params", "input_files"], errors="ignore"), use_container_width=True)
if runs["duration_seconds"].notna().any():
    st.plotly_chart(px.scatter(runs, x="started_at", y="duration_seconds", color="status",
                               title="Duration over time"), use_container_width=True)
errors = frame("list_errors", None, 200)
st.subheader("Processing errors")
if errors.empty:
    st.success("No processing errors recorded.")
else:
    st.dataframe(errors, use_container_width=True)
backfills = frame("list_backfills")
if not backfills.empty:
    st.subheader("Backfill history")
    st.dataframe(backfills, use_container_width=True)
