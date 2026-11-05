import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import empty_state, frame, get_settings  # noqa: E402
from src.storage.partition_manager import list_partitions  # noqa: E402

st.set_page_config(page_title="Partition Explorer", layout="wide")
st.title("Partition Explorer (Silver)")
settings = get_settings()
meta = frame("list_partitions", "silver.trips")
if meta.empty:
    empty_state("No Silver partitions recorded.")
    st.stop()
meta = meta[meta["row_count"] > 0]
st.caption(f"Partition column: `{settings.partition_column}` ({settings.partition_scheme}).")
st.plotly_chart(px.bar(meta, x="partition_value", y="row_count", title="Rows per partition"),
                use_container_width=True)
st.plotly_chart(px.bar(meta, x="partition_value", y="size_bytes", title="Bytes per partition"),
                use_container_width=True)
on_disk = list_partitions(settings.silver_dir, settings.partition_column)
missing = sorted(set(meta["partition_value"]) - set(on_disk))
extra = sorted(set(on_disk) - set(meta["partition_value"]))
if missing or extra:
    st.warning(f"Metadata/disk mismatch. In metadata only: {missing}; on disk only: {extra}")
else:
    st.success("Partition metadata matches the files on disk.")
st.dataframe(meta, use_container_width=True)
