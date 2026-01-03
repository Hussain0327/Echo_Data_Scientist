from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Customer Analysis", page_icon="C", layout="wide")


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

    if "customer_id" not in df.columns:
        df["customer_id"] = [f"CUST_{np.random.randint(1, 150):04d}" for _ in range(len(df))]

    return df


def calculate_rfm(df):
    analysis_date = df["date"].max() + timedelta(days=1)

    rfm = df.groupby("customer_id").agg(
        {"date": lambda x: (analysis_date - x.max()).days, "customer_id": "count", "amount": "sum"}
    )

    rfm.columns = ["recency", "frequency", "monetary"]
    rfm = rfm.reset_index()

    # Score
    rfm["r_score"] = pd.qcut(rfm["recency"], q=5, labels=[5, 4, 3, 2, 1], duplicates="drop").astype(
        int
    )
    rfm["f_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5], duplicates="drop"
    ).astype(int)
    rfm["m_score"] = pd.qcut(
        rfm["monetary"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5], duplicates="drop"
    ).astype(int)

    # Segment
    def segment(row):
        r, f = row["r_score"], row["f_score"]
        if r >= 4 and f >= 4:
            return "Champions"
        elif f >= 4:
            return "Loyal"
        elif r >= 4:
            return "New Customers"
        elif r <= 2 and f >= 3:
            return "At Risk"
        elif r <= 2:
            return "Hibernating"
        else:
            return "Need Attention"

    rfm["segment"] = rfm.apply(segment, axis=1)

    return rfm


def main():
    st.title("Customer Analysis")
    st.markdown("Customer segmentation, behavior patterns, and lifetime value analysis")

    df = load_data()

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Customers", f"{df['customer_id'].nunique():,}")

    with col2:
        avg_purchases = len(df) / df["customer_id"].nunique()
        st.metric("Avg Purchases/Customer", f"{avg_purchases:.1f}")

    with col3:
        avg_ltv = df.groupby("customer_id")["amount"].sum().mean()
        st.metric("Avg Customer Value", f"${avg_ltv:,.2f}")

    with col4:
        new_customers = df.groupby("customer_id")["date"].min().reset_index()
        new_customers["month"] = new_customers["date"].dt.to_period("M")
        latest_month_new = new_customers[
            new_customers["month"] == new_customers["month"].max()
        ].shape[0]
        st.metric("New Customers (Latest Month)", f"{latest_month_new}")

    st.divider()

    # RFM Analysis
    st.subheader("Customer Segmentation (RFM)")

    rfm = calculate_rfm(df)

    col1, col2 = st.columns(2)

    with col1:
        # Segment distribution
        segment_counts = rfm["segment"].value_counts()

        fig = px.pie(
            values=segment_counts.values,
            names=segment_counts.index,
            title="Customer Segments",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Segment value
        segment_value = rfm.groupby("segment")["monetary"].sum().sort_values(ascending=True)

        fig = go.Figure(
            go.Bar(
                x=segment_value.values,
                y=segment_value.index,
                orientation="h",
                marker_color="#1f77b4",
                text=[f"${v:,.0f}" for v in segment_value.values],
                textposition="auto",
            )
        )

        fig.update_layout(
            title="Revenue by Segment", xaxis_title="Revenue ($)", yaxis_title="Segment"
        )
        st.plotly_chart(fig, use_container_width=True)

    # RFM Scatter
    st.subheader("RFM Distribution")

    col1, col2 = st.columns(2)

    with col1:
        fig = px.scatter(
            rfm,
            x="frequency",
            y="monetary",
            color="segment",
            size="recency",
            title="Frequency vs Monetary (size = recency)",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Heatmap of RFM scores
        heatmap_data = rfm.groupby(["r_score", "f_score"]).size().reset_index(name="count")
        heatmap_pivot = heatmap_data.pivot(
            index="r_score", columns="f_score", values="count"
        ).fillna(0)

        fig = px.imshow(
            heatmap_pivot,
            labels=dict(x="Frequency Score", y="Recency Score", color="Customers"),
            title="Customer Distribution by RFM Scores",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Customer Acquisition
    st.subheader("Customer Acquisition Over Time")

    first_purchase = df.groupby("customer_id")["date"].min().reset_index()
    first_purchase.columns = ["customer_id", "first_purchase"]
    first_purchase["month"] = first_purchase["first_purchase"].dt.to_period("M")
    acquisition = first_purchase.groupby("month").size().reset_index(name="new_customers")
    acquisition["month"] = acquisition["month"].astype(str)
    acquisition["cumulative"] = acquisition["new_customers"].cumsum()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=acquisition["month"],
            y=acquisition["new_customers"],
            name="New Customers",
            marker_color="#1f77b4",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=acquisition["month"],
            y=acquisition["cumulative"],
            name="Cumulative",
            line=dict(color="#ff7f0e", width=2),
            yaxis="y2",
        )
    )

    fig.update_layout(
        title="Customer Acquisition",
        yaxis=dict(title="New Customers"),
        yaxis2=dict(title="Cumulative", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Customer Lifetime Value Distribution
    st.subheader("Customer Lifetime Value Distribution")

    customer_ltv = df.groupby("customer_id")["amount"].sum().reset_index()
    customer_ltv.columns = ["customer_id", "ltv"]

    col1, col2 = st.columns(2)

    with col1:
        fig = px.histogram(customer_ltv, x="ltv", nbins=30, title="LTV Distribution")
        fig.update_traces(marker_color="#1f77b4")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Top customers
        top_customers = customer_ltv.nlargest(10, "ltv")

        fig = go.Figure(
            go.Bar(
                x=top_customers["ltv"],
                y=top_customers["customer_id"],
                orientation="h",
                marker_color="#28a745",
                text=[f"${v:,.0f}" for v in top_customers["ltv"]],
                textposition="auto",
            )
        )

        fig.update_layout(
            title="Top 10 Customers by LTV", xaxis_title="LTV ($)", yaxis_title="Customer"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Segment Details Table
    st.subheader("Segment Details")

    segment_stats = (
        rfm.groupby("segment")
        .agg(
            {
                "customer_id": "count",
                "recency": "mean",
                "frequency": "mean",
                "monetary": ["mean", "sum"],
            }
        )
        .round(2)
    )

    segment_stats.columns = [
        "Customers",
        "Avg Recency (days)",
        "Avg Frequency",
        "Avg LTV",
        "Total Revenue",
    ]
    segment_stats = segment_stats.sort_values("Total Revenue", ascending=False)

    # Format
    segment_stats["Avg LTV"] = segment_stats["Avg LTV"].apply(lambda x: f"${x:,.2f}")
    segment_stats["Total Revenue"] = segment_stats["Total Revenue"].apply(lambda x: f"${x:,.2f}")

    st.dataframe(segment_stats, use_container_width=True)


if __name__ == "__main__":
    main()
