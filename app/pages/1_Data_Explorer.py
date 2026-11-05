import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import date_range_filter, empty_state, warehouse_query  # noqa: E402

st.set_page_config(page_title="Data Explorer", layout="wide")
st.title("Data Explorer")
TABLES = {"gold.daily_trip_metrics": "pickup_date", "gold.hourly_trip_metrics": None,
          "gold.location_metrics": None, "gold.payment_type_metrics": None,
          "intermediate.clean_trips": "pickup_date", "main.silver_trips": "pickup_date"}
dataset = st.sidebar.selectbox("Dataset", list(TABLES))
limit = st.sidebar.slider("Rows", 100, 5000, 1000, step=100)
df = warehouse_query(f"SELECT * FROM {dataset} LIMIT {int(limit)}")
if df is None or df.empty:
    empty_state(f"{dataset} is not available or has no rows.")
    st.stop()
if TABLES[dataset]:
    df = date_range_filter(df, TABLES[dataset])
st.caption(f"{len(df):,} rows shown")
st.dataframe(df, use_container_width=True)
if dataset == "gold.daily_trip_metrics":
    st.plotly_chart(px.line(df, x="pickup_date", y="trips", title="Trips per day"), use_container_width=True)
