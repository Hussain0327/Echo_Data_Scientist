from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Revenue Analysis", page_icon="$", layout="wide")


@st.cache_data
def load_data():
    try:
        df = pd.read_csv("data/samples/revenue_sample.csv")
    except FileNotFoundError:
        np.random.seed(42)
        n_records = 500
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(n_records)]

        df = pd.DataFrame(
            {
                "transaction_date": dates,
                "customer_id": [f"CUST_{np.random.randint(1, 150):04d}" for _ in range(n_records)],
                "amount": np.random.exponential(scale=200, size=n_records),
                "channel": np.random.choice(
                    ["Organic", "Paid", "Email", "Social", "Referral"], n_records
                ),
            }
        )

    date_cols = [col for col in df.columns if "date" in col.lower()]
    if date_cols:
        df["date"] = pd.to_datetime(df[date_cols[0]])

    amount_cols = [
        col for col in df.columns if any(x in col.lower() for x in ["amount", "revenue", "price"])
    ]
    if amount_cols:
        df["amount"] = pd.to_numeric(df[amount_cols[0]], errors="coerce")

    return df


def main():
    st.title("Revenue Analysis")
    st.markdown("Detailed revenue metrics, trends, and growth analysis")

    df = load_data()

    # Sidebar
    st.sidebar.header("Revenue Filters")
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    date_range = st.sidebar.date_input(
        "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )

    if len(date_range) == 2:
        mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])
        df = df[mask]

    # Aggregation level
    agg_level = st.sidebar.selectbox("Aggregation Level", ["Daily", "Weekly", "Monthly"])

    st.divider()

    # Revenue Summary
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Revenue", f"${df['amount'].sum():,.2f}")

    with col2:
        daily_avg = df.groupby(df["date"].dt.date)["amount"].sum().mean()
        st.metric("Daily Average", f"${daily_avg:,.2f}")

    with col3:
        st.metric("Transaction Count", f"{len(df):,}")

    with col4:
        st.metric("Avg Transaction", f"${df['amount'].mean():,.2f}")

    st.divider()

    # Revenue Trend
    st.subheader("Revenue Trend")

    if agg_level == "Daily":
        agg_df = df.groupby(df["date"].dt.date)["amount"].sum().reset_index()
        agg_df.columns = ["date", "revenue"]
    elif agg_level == "Weekly":
        agg_df = df.groupby(df["date"].dt.to_period("W"))["amount"].sum().reset_index()
        agg_df.columns = ["date", "revenue"]
        agg_df["date"] = agg_df["date"].astype(str)
    else:
        agg_df = df.groupby(df["date"].dt.to_period("M"))["amount"].sum().reset_index()
        agg_df.columns = ["date", "revenue"]
        agg_df["date"] = agg_df["date"].astype(str)

    fig = px.line(agg_df, x="date", y="revenue", title=f"{agg_level} Revenue")
    fig.update_traces(line_color="#1f77b4", line_width=2)
    st.plotly_chart(fig, use_container_width=True)

    # Growth Analysis
    st.subheader("Growth Analysis")

    monthly = df.groupby(df["date"].dt.to_period("M"))["amount"].sum().reset_index()
    monthly.columns = ["month", "revenue"]
    monthly["month_str"] = monthly["month"].astype(str)
    monthly["growth_pct"] = monthly["revenue"].pct_change() * 100
    monthly["growth_abs"] = monthly["revenue"].diff()

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        colors = ["#28a745" if x >= 0 else "#dc3545" for x in monthly["growth_pct"].fillna(0)]

        fig.add_trace(
            go.Bar(
                x=monthly["month_str"],
                y=monthly["growth_pct"],
                marker_color=colors,
                text=monthly["growth_pct"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A"),
                textposition="auto",
            )
        )

        fig.update_layout(
            title="Month-over-Month Growth (%)", xaxis_title="Month", yaxis_title="Growth %"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Cumulative Revenue
        monthly["cumulative"] = monthly["revenue"].cumsum()

        fig = px.area(monthly, x="month_str", y="cumulative", title="Cumulative Revenue")
        fig.update_traces(fillcolor="rgba(31, 119, 180, 0.3)", line_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    # Revenue Distribution
    st.subheader("Revenue Distribution")

    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(df, x="amount", nbins=30, title="Transaction Value Distribution")
        fig.update_traces(marker_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.box(df, y="amount", title="Revenue Box Plot")
        fig.update_traces(marker_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    # Summary Statistics
    st.subheader("Summary Statistics")

    stats = df["amount"].describe()
    stats_df = pd.DataFrame(
        {
            "Metric": ["Count", "Mean", "Std Dev", "Min", "25%", "50% (Median)", "75%", "Max"],
            "Value": [
                f"{stats['count']:,.0f}",
                f"${stats['mean']:,.2f}",
                f"${stats['std']:,.2f}",
                f"${stats['min']:,.2f}",
                f"${stats['25%']:,.2f}",
                f"${stats['50%']:,.2f}",
                f"${stats['75%']:,.2f}",
                f"${stats['max']:,.2f}",
            ],
        }
    )

    st.dataframe(stats_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
