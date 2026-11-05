import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import date_range_filter, empty_state, frame  # noqa: E402

st.set_page_config(page_title="Data Quality", layout="wide")
st.title("Data Quality")
q = frame("list_quality_results", None, 20000)
if q.empty:
    empty_state("No quality results recorded yet.")
    st.stop()
q = date_range_filter(q, "executed_at")
dataset = st.sidebar.selectbox("Dataset", ["(all)"] + sorted(q["dataset_name"].unique()))
if dataset != "(all)":
    q = q[q["dataset_name"] == dataset]
latest_run = q.sort_values("executed_at").iloc[-1]["run_id"] if not q.empty else None
st.subheader(f"Latest results ({latest_run})")
latest = q[q["run_id"] == latest_run]
st.dataframe(latest[["stage", "rule_name", "severity", "action", "records_evaluated", "records_failed",
                     "failure_pct", "threshold_pct", "status"]], use_container_width=True)
st.subheader("Failure rate trend")
trend = q[q["records_failed"] > 0]
if trend.empty:
    st.success("No rule failures in the selected range.")
else:
    st.plotly_chart(px.line(trend.sort_values("executed_at"), x="executed_at", y="failure_pct",
                            color="rule_name", markers=True), use_container_width=True)
