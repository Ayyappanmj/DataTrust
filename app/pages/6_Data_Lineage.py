import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import empty_state, get_settings  # noqa: E402
from src.metadata.models import GOLD_TABLES, gold_dataset  # noqa: E402
from src.metadata.repository import open_repository  # noqa: E402

st.set_page_config(page_title="Data Lineage", layout="wide")
st.title("Data Lineage (dataset level)")
st.caption("Tracks files, runs and datasets. Column-level lineage is not implemented.")
repo = open_repository(get_settings())
try:
    options = ["silver.trips"] + [gold_dataset(t) for t in GOLD_TABLES]
    dataset = st.sidebar.selectbox("Dataset", options, index=2)
    files = repo.contributing_files(dataset)
    edges = repo.lineage_upstream(dataset)
finally:
    repo.close()
if not edges:
    empty_state(f"No lineage recorded for {dataset}.")
    st.stop()
st.subheader("Which source files and runs contributed?")
st.dataframe([{**f, "run_ids": ", ".join(f["run_ids"])} for f in files], use_container_width=True)
labels = sorted({e["source_dataset"] for e in edges} | {e["output_dataset"] for e in edges})
index = {name: i for i, name in enumerate(labels)}
fig = go.Figure(go.Sankey(
    node={"label": labels},
    link={"source": [index[e["source_dataset"]] for e in edges],
          "target": [index[e["output_dataset"]] for e in edges],
          "value": [max(int(e["output_record_count"] or 1), 1) for e in edges]}))
st.plotly_chart(fig, use_container_width=True)
st.dataframe(edges, use_container_width=True)
