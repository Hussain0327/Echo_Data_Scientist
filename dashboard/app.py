import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Page configuration
st.set_page_config(
    page_title="Echo Analytics Dashboard",
    page_icon="E",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .big-metric {
        font-size: 36px;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .positive {
        color: #28a745;
    }
    .negative {
        color: #dc3545;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data
def load_data():
    """Load and prepare sample data."""
    try:
        df = pd.read_csv("data/samples/revenue_sample.csv")
    except FileNotFoundError:
        # Generate synthetic data if file not found
        np.random.seed(42)
        n_records = 500

        # Generate dates
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(n_records)]

        # Generate synthetic data
        df = pd.DataFrame(
            {
                "transaction_date": dates,
                "customer_id": [f"CUST_{np.random.randint(1, 150):04d}" for _ in range(n_records)],
                "amount": np.random.exponential(scale=200, size=n_records),
                "channel": np.random.choice(
                    ["Organic", "Paid", "Email", "Social", "Referral"], n_records
                ),
                "product_category": np.random.choice(
                    ["Electronics", "Clothing", "Home", "Books", "Other"], n_records
                ),
            }
        )

    # Process dates
    date_cols = [col for col in df.columns if "date" in col.lower()]
    if date_cols:
        df["date"] = pd.to_datetime(df[date_cols[0]])
    else:
        df["date"] = pd.to_datetime(df.iloc[:, 0])

    # Find amount column
    amount_cols = [
        col
        for col in df.columns
        if any(x in col.lower() for x in ["amount", "revenue", "price", "total"])
    ]
    if amount_cols:
        df["amount"] = pd.to_numeric(df[amount_cols[0]], errors="coerce")
    elif "amount" not in df.columns:
        df["amount"] = np.random.exponential(scale=200, size=len(df))

    return df


def calculate_metrics(df, date_range=None):
    """Calculate key business metrics."""
    if date_range:
        mask = (df["date"] >= date_range[0]) & (df["date"] <= date_range[1])
        df = df[mask]

    total_revenue = df["amount"].sum()
    total_transactions = len(df)
    avg_order_value = df["amount"].mean() if len(df) > 0 else 0
    unique_customers = df["customer_id"].nunique() if "customer_id" in df.columns else 0

    return {
        "total_revenue": total_revenue,
        "total_transactions": total_transactions,
        "avg_order_value": avg_order_value,
        "unique_customers": unique_customers,
    }


def create_revenue_trend(df):
    """Create revenue trend chart."""
    daily = df.groupby(df["date"].dt.date)["amount"].sum().reset_index()
    daily.columns = ["date", "revenue"]
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date")

    # Add 7-day moving average
    daily["ma_7"] = daily["revenue"].rolling(window=7).mean()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["revenue"],
            mode="lines",
            name="Daily Revenue",
            line=dict(color="#1f77b4", width=1),
            opacity=0.6,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=daily["date"],
            y=daily["ma_7"],
            mode="lines",
            name="7-Day Moving Avg",
            line=dict(color="#ff7f0e", width=2),
        )
    )

    fig.update_layout(
        title="Revenue Trend",
        xaxis_title="Date",
        yaxis_title="Revenue ($)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_channel_performance(df):
    """Create channel performance chart."""
    if "channel" not in df.columns:
        df["channel"] = np.random.choice(
            ["Organic", "Paid", "Email", "Social", "Referral"], len(df)
        )

    channel_data = df.groupby("channel").agg({"amount": ["sum", "count", "mean"]}).round(2)
    channel_data.columns = ["total_revenue", "transactions", "avg_order"]
    channel_data = channel_data.reset_index()
    channel_data = channel_data.sort_values("total_revenue", ascending=True)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=channel_data["channel"],
            x=channel_data["total_revenue"],
            orientation="h",
            marker_color="#1f77b4",
            text=channel_data["total_revenue"].apply(lambda x: f"${x:,.0f}"),
            textposition="auto",
        )
    )

    fig.update_layout(title="Revenue by Channel", xaxis_title="Revenue ($)", yaxis_title="Channel")

    return fig, channel_data


def create_monthly_comparison(df):
    """Create month-over-month comparison."""
    monthly = df.groupby(df["date"].dt.to_period("M"))["amount"].sum().reset_index()
    monthly.columns = ["month", "revenue"]
    monthly["month"] = monthly["month"].astype(str)
    monthly["growth"] = monthly["revenue"].pct_change() * 100

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=monthly["month"],
            y=monthly["revenue"],
            marker_color="#1f77b4",
            text=monthly["revenue"].apply(lambda x: f"${x:,.0f}"),
            textposition="auto",
            name="Revenue",
        )
    )

    fig.update_layout(title="Monthly Revenue", xaxis_title="Month", yaxis_title="Revenue ($)")

    return fig, monthly


def main():
    """Main dashboard application."""

    # Header
    st.title("Echo Analytics Dashboard")
    st.markdown("Real-time business analytics and KPI monitoring")

    # Load data
    df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")

    # Date range filter
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    date_range = st.sidebar.date_input(
        "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )

    # Apply filters
    if len(date_range) == 2:
        mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])
        filtered_df = df[mask]
    else:
        filtered_df = df

    # Calculate metrics
    metrics = calculate_metrics(filtered_df)

    # Previous period for comparison
    days_in_range = (date_range[1] - date_range[0]).days if len(date_range) == 2 else 30
    prev_start = date_range[0] - timedelta(days=days_in_range)
    prev_end = date_range[0] - timedelta(days=1)
    prev_mask = (df["date"].dt.date >= prev_start) & (df["date"].dt.date <= prev_end)
    prev_metrics = calculate_metrics(df[prev_mask])

    # KPI Cards
    st.header("Key Performance Indicators")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        delta = (
            (
                (metrics["total_revenue"] - prev_metrics["total_revenue"])
                / prev_metrics["total_revenue"]
                * 100
            )
            if prev_metrics["total_revenue"] > 0
            else 0
        )
        st.metric(
            label="Total Revenue", value=f"${metrics['total_revenue']:,.2f}", delta=f"{delta:+.1f}%"
        )

    with col2:
        delta = (
            (
                (metrics["total_transactions"] - prev_metrics["total_transactions"])
                / prev_metrics["total_transactions"]
                * 100
            )
            if prev_metrics["total_transactions"] > 0
            else 0
        )
        st.metric(
            label="Transactions", value=f"{metrics['total_transactions']:,}", delta=f"{delta:+.1f}%"
        )

    with col3:
        delta = (
            (
                (metrics["avg_order_value"] - prev_metrics["avg_order_value"])
                / prev_metrics["avg_order_value"]
                * 100
            )
            if prev_metrics["avg_order_value"] > 0
            else 0
        )
        st.metric(
            label="Avg Order Value",
            value=f"${metrics['avg_order_value']:,.2f}",
            delta=f"{delta:+.1f}%",
        )

    with col4:
        delta = (
            (
                (metrics["unique_customers"] - prev_metrics["unique_customers"])
                / prev_metrics["unique_customers"]
                * 100
            )
            if prev_metrics["unique_customers"] > 0
            else 0
        )
        st.metric(
            label="Unique Customers",
            value=f"{metrics['unique_customers']:,}",
            delta=f"{delta:+.1f}%",
        )

    st.divider()

    # Charts Row 1
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue Trend")
        fig = create_revenue_trend(filtered_df)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Monthly Revenue")
        fig, monthly_data = create_monthly_comparison(filtered_df)
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Channel Performance")
        fig, channel_data = create_channel_performance(filtered_df)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Revenue Distribution")
        fig = px.histogram(
            filtered_df,
            x="amount",
            nbins=30,
            title="Order Value Distribution",
            labels={"amount": "Order Value ($)", "count": "Frequency"},
        )
        fig.update_traces(marker_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Data Table
    st.subheader("Recent Transactions")

    display_df = (
        filtered_df[["date", "customer_id", "amount"]].copy()
        if "customer_id" in filtered_df.columns
        else filtered_df[["date", "amount"]].copy()
    )
    display_df = display_df.sort_values("date", ascending=False).head(10)
    display_df["date"] = display_df["date"].dt.strftime("%Y-%m-%d")
    display_df["amount"] = display_df["amount"].apply(lambda x: f"${x:,.2f}")

    st.dataframe(display_df, use_container_width=True)

    # Footer
    st.divider()
    st.markdown(
        """
    <div style='text-align: center; color: #666; font-size: 12px;'>
        Echo Analytics Dashboard | Built with Streamlit | Data updated in real-time
    </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
