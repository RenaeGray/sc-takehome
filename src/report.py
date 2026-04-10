import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.express as px
import streamlit as st
from datetime import date
from src.transform import download_parquet
from src.queries import (db,
                        delay_trend,
                        delay_by_carrier,
                        delay_causes,
                        delay_by_carrier_and_cause,
                        delay_by_dimension,
                        )

st.set_page_config(layout="wide")
st.title("✈️ Flight Delay Dashboard")

download_parquet()

DATE_MIN = date(2018, 1, 1)
DATE_MAX = date(2025, 1, 31)

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

YEARS = list(range(DATE_MIN.year, DATE_MAX.year + 1))

QUARTER_OPTIONS = {
    f"Q{q} {y}": (y, q)
    for y in YEARS for q in range(1, 5)
    if (DATE_MIN.year, 1) <= (y, q) <= (DATE_MAX.year, (DATE_MAX.month - 1) // 3 + 1)
}
MONTH_OPTIONS = {
    f"{MONTH_NAMES[m-1]} {y}": (y, m)
    for y in YEARS for m in range(1, 13)
    if (DATE_MIN.year, DATE_MIN.month) <= (y, m) <= (DATE_MAX.year, DATE_MAX.month)
}

left, right = st.columns([1, 3])

with left.container(border=True):
    st.subheader("Filters")

    grain = st.pills("Time filter", ["Month", "Quarter", "Year"], default="Year")
    if grain == "Year":
        user_sel= st.multiselect("Year", YEARS, default=[2024])
        if not user_sel:
            st.warning("Select at least one year.")
            st.stop()
        years_str = ", ".join(str(y) for y in user_sel)
        filters = [f"Year IN ({years_str})"]
        date_label = ", ".join(str(y) for y in sorted(user_sel))

    elif grain == "Quarter":
        user_sel= st.multiselect("Quarter", list(QUARTER_OPTIONS), default=[list(QUARTER_OPTIONS)[-1]])
        if not user_sel:
            st.warning("Select at least one quarter.")
            st.stop()
        pairs = ", ".join(f"({y}, {q})" for s in user_sel for y, q in [QUARTER_OPTIONS[s]])
        filters = [f"(Year, Quarter) IN ({pairs})"]
        date_label = ", ".join(user_sel)

    else:  # Month
        user_sel= st.multiselect("Month", list(MONTH_OPTIONS), default=[list(MONTH_OPTIONS)[-1]])
        if not user_sel:
            st.warning("Select at least one month.")
            st.stop()
        pairs = ", ".join(f"({y}, {m})" for s in user_sel for y, m in [MONTH_OPTIONS[s]])
        filters = [f"(Year, Month) IN ({pairs})"]
        date_label = ", ".join(user_sel)
    # Compare by filter chart 5 & 6
    compare_by = st.selectbox(
        "Compare by (Charts 5 & 6)",
        ["Origin Airport", "Destination Airport", "Route"]
    )

with db() as conn:
    trend_df     = conn.execute(delay_trend(filters)).df()
    carrier_df   = conn.execute(delay_by_carrier(filters)).df()
    causes_row   = conn.execute(delay_causes(filters)).df()
    cause_car_df = conn.execute(delay_by_carrier_and_cause(filters)).df()
    dimension_df      = conn.execute(delay_by_dimension(compare_by, filters)).df()
    dimension_best_df = conn.execute(delay_by_dimension(compare_by, filters, ascending=True)).df()

def render_chart(fig, caption):
    fig.update_layout(yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(caption)

# KPIs

total_flights = int(carrier_df["total_flights"].sum())
overall_rate  = carrier_df["delay_rate"].mean()  # weighted by equal airline representation

with left.container(border=True):
    st.metric("Overall Delay Rate", f"{overall_rate:.1%}")
    st.metric("Total Flights", f"{total_flights:,}")

# Chart 1: Delay rate trendline
with right.container(border=True):
    if grain == "Month" and len(user_sel) == 1:
        st.info(f"Trend not shown for a single month -- see the metrics on the left for {date_label}.")
    else:
        fig1 = px.line(
            trend_df, x="month", y="delay_rate",
            title=f"1. How bad are delays? ({date_label})"
        )
        render_chart(fig1, "What % of flights are delayed (>15 min), and how has that changed over time?")

# Chart 2: Ranked airlines 
fig2 = px.bar(
    carrier_df, x="Reporting_Airline", y="delay_rate",
    title=f"2. Which airlines have the worst delays? ({date_label})"
)
render_chart(fig2, "Delay rate per airline. Rate is used so an airline with 10x more flights can still be compared against a smaller airline.")

# Chart 3: Which airlines are the worst & why?
cause_melted = cause_car_df.melt(
    id_vars="Reporting_Airline",
    value_vars=["airline_issues_rate", "weather_rate", "air_traffic_rate", "security_rate"],
    var_name="cause",
    value_name="rate"
).replace({
    "airline_issues_rate": "Airline Issues",
    "weather_rate":        "Weather",
    "air_traffic_rate":    "Air Traffic",
    "security_rate":       "Security / Other",
})
fig4 = px.bar(
    cause_melted, x="Reporting_Airline", y="rate", color="cause",
    barmode="stack",
    title=f"3. Which airlines have the most delays - and what caused those delays? ({date_label})"
)
render_chart(fig4, "Each bar shows an airline's total delay rate, split by what caused those delays.")

# Chart 3: Why are flights delayed?
causes_melted = causes_row.melt(
    var_name="cause",
    value_name="delay_minutes"
).replace({
    "airline_issues": "Airline Issues",
    "weather":        "Weather",
    "air_traffic":    "Air Traffic",
    "security_other": "Security / Other",
})
causes_melted["share"] = causes_melted["delay_minutes"] / causes_melted["delay_minutes"].sum()
fig3 = px.bar(
    causes_melted, x="cause", y="share",
    title=f"4. What causes the most delays? ({date_label})"
)
render_chart(fig3, "Share of total delay minutes by cause across all airlines in the selected period.")

# Chart 5: Worst Airport/route comparison 
dim_col = "route" if compare_by == "Route" else "airport"
fig5 = px.bar(
    dimension_df, x=dim_col, y="delay_rate",
    title=f"5. Which {compare_by.lower()}s  have the worst delays? Top 20 ({date_label})"
)
render_chart(fig5,
    f"Top 20 {compare_by.lower()}s by delay rate. "
    "Showing rate not volume, as a small airport with few flights can still rank high. "
    "Only includes origins/destinations/routes with at least 100 flights in the selected period. "
    "Low-traffic routes with zero delays are excluded as they aren't a meaningful sample."
)

# Chart 6: Best airports/routes comparision
fig6 = px.bar(
    dimension_best_df, x=dim_col, y="delay_rate",
    title=f"6. Which {compare_by.lower()}s are the most reliable? Top 20 ({date_label})"
)
render_chart(fig6,
    f"Top 20 most on-time {compare_by.lower()}s by delay rate. "
    "Only includes origins/destinations/routes with at least 100 flights. "
    "Low-traffic routes with zero delays are excluded as they aren't a meaningful sample."
)
