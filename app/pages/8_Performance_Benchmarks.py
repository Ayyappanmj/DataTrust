import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import get_settings  # noqa: E402

st.set_page_config(page_title="Performance Benchmarks", layout="wide")
st.title("Performance Benchmarks")
st.caption("Local, single-machine, synthetic data. These numbers do not predict production performance.")
files = sorted((get_settings().reports_dir / "benchmarks").glob("benchmark_*.json"))
if not files:
    st.info("No benchmark results yet. Run `python -m src.cli benchmark`.")
    st.stop()
chosen = st.sidebar.selectbox("Result file", [f.name for f in reversed(files)])
data = json.loads((files[0].parent / chosen).read_text(encoding="utf-8"))
st.json(data["summary"])
df = pd.DataFrame(data["results"])
st.plotly_chart(px.bar(df, x="query", y="median_seconds", color="layout", barmode="group",
                       title="Median query time"), use_container_width=True)
st.plotly_chart(px.bar(df, x="query", y="files_touched", color="layout", barmode="group",
                       title="Parquet files touched"), use_container_width=True)
st.dataframe(df, use_container_width=True)
