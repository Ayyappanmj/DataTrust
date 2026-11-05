import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from app.components.data_access import date_range_filter, empty_state, frame, warehouse_query  # noqa: E402

st.set_page_config(page_title="Historical Trends", layout="wide")
st.title("Historical Trends")
daily = warehouse_query("SELECT * FROM gold.daily_trip_metrics ORDER BY pickup_date")
if daily is None or daily.empty:
    empty_state("The gold.daily_trip_metrics model has not been built.")
    st.stop()
daily = date_range_filter(daily, "pickup_date")
st.plotly_chart(px.line(daily, x="pickup_date", y=["trips", "trips_7d_avg"], title="Daily trips"),
                use_container_width=True)
st.plotly_chart(px.bar(daily, x="pickup_date", y="fare_revenue",
                       title="Fare revenue (fare_amount + extra; excludes tips, tolls, taxes)"),
                use_container_width=True)
st.plotly_chart(px.line(daily, x="pickup_date", y="avg_tip_pct", title="Tip % of fare (card trips)"),
                use_container_width=True)
runs = frame("list_runs", 500)
if not runs.empty:
    st.plotly_chart(px.bar(runs.sort_values("started_at"), x="started_at", y="records_read",
                           color="status", title="Records read per run"), use_container_width=True)
