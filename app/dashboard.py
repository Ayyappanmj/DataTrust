"""DataTrust - Overview page (Streamlit entry point). Run: streamlit run app/dashboard.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import empty_state, frame, get_settings, warehouse_query  # noqa: E402
from src.orchestration.pipeline_status import summarize_runs  # noqa: E402

st.set_page_config(page_title="DataTrust", page_icon="🗄️", layout="wide")
st.title("DataTrust - Data Observability")
st.caption("Medallion lakehouse: Bronze -> Silver -> Gold, with quality gates and lineage.")

runs = frame("list_runs", 1000)
manifest = frame("list_manifest", True)
if runs.empty:
    empty_state()
    st.stop()

summary = summarize_runs(runs.to_dict("records"))
ingested = int(manifest["record_count"].fillna(0).sum()) if not manifest.empty else 0
partitions = frame("list_partitions", "silver.trips")
processed = int(partitions["row_count"].sum()) if not partitions.empty else 0
rejected_df = warehouse_query("SELECT COUNT(*) AS n FROM read_parquet(?, hive_partitioning = true)",
                              [(get_settings().quarantine_dir / "**" / "*.parquet").as_posix()])
rejected = int(rejected_df["n"].iloc[0]) if rejected_df is not None else 0

c = st.columns(4)
c[0].metric("Records ingested (active files)", f"{ingested:,}")
c[1].metric("Records in Silver", f"{processed:,}")
c[2].metric("Records quarantined", f"{rejected:,}")
c[3].metric("Pipeline success rate",
            f"{summary['success_rate_pct']}%" if summary["success_rate_pct"] is not None else "n/a")
c = st.columns(3)
c[0].metric("Latest successful run", summary["latest_successful_run"] or "none")
c[1].metric("Data freshness", f"{summary['freshness_hours']} h ago"
            if summary["freshness_hours"] is not None else "n/a")
last = runs.iloc[0]
c[2].metric("Last run duration", f"{last['duration_seconds']} s" if last["duration_seconds"] else "running/n/a")

left, right = st.columns(2)
done = runs[runs["duration_seconds"].notna()].head(30).iloc[::-1]
if not done.empty:
    left.plotly_chart(px.bar(done, x="run_id", y="duration_seconds", color="status",
                             title="Processing duration by run"), use_container_width=True)
quality = frame("list_quality_results", None, 2000)
if not quality.empty:
    latest = quality[quality["run_id"] == quality.iloc[0]["run_id"]]
    right.plotly_chart(px.bar(latest[latest["records_failed"] > 0], x="rule_name", y="records_failed",
                              color="severity", title="Rule failures (latest run with results)"),
                       use_container_width=True)
st.dataframe(runs.head(10)[["run_id", "status", "started_at", "duration_seconds", "records_read",
                            "records_rejected", "records_written"]], use_container_width=True)
