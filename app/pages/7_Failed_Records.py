import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import empty_state, get_settings, warehouse_query  # noqa: E402

st.set_page_config(page_title="Failed Records", layout="wide")
st.title("Failed Records (Quarantine)")
glob = (get_settings().quarantine_dir / "**" / "*.parquet").as_posix()
df = warehouse_query(
    "SELECT source_file, source_row_id, rejection_reason, raw_record, run_id, rejected_at "
    "FROM read_parquet(?, hive_partitioning = true) ORDER BY rejected_at DESC LIMIT 5000", [glob])
if df is None or df.empty:
    empty_state("Nothing has been quarantined.")
    st.stop()
by_reason = df.assign(reason=df["rejection_reason"].str.split(";")).explode("reason")
st.plotly_chart(px.bar(by_reason.groupby("reason").size().reset_index(name="records"),
                       x="reason", y="records", title="Rejected records by reason"),
                use_container_width=True)
choice = st.sidebar.multiselect("Reason", sorted(by_reason["reason"].unique()))
if choice:
    df = df[df["rejection_reason"].apply(lambda r: any(c in r.split(";") for c in choice))]
st.dataframe(df, use_container_width=True)
