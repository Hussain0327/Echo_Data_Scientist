import pandas as pd

from app.services.metrics.base import BaseMetric, MetricDefinition, MetricResult


class TotalRevenue(BaseMetric):
    PAID_STATUSES = ["paid", "success", "completed", "active"]

    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="total_revenue",
            display_name="Total Revenue",
            description="Sum of all revenue from paid transactions",
            category="revenue",
            unit="$",
            formula="SUM(amount) WHERE status IN (paid, success, completed)",
            required_columns=["amount"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        if "status" in df.columns:
            df = df[df["status"].str.lower().isin(self.PAID_STATUSES)]

        total = df["amount"].sum()
        count = len(df)
        avg = df["amount"].mean() if count > 0 else 0

        return self._format_result(
            value=float(total), transaction_count=count, average_transaction=round(float(avg), 2)
        )


class RevenueByPeriod(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="revenue_by_period",
            display_name="Revenue by Period",
            description="Revenue grouped by time period",
            category="revenue",
            unit="$",
            formula="SUM(amount) GROUP BY period",
            required_columns=["amount", "date"],
        )

    def calculate(self, period: str = "month", **kwargs) -> MetricResult:
        df = self.df.copy()
        df["date"] = pd.to_datetime(df["date"])

        if "status" in df.columns:
            df = df[df["status"].str.lower().isin(TotalRevenue.PAID_STATUSES)]

        if period == "day":
            df["period"] = df["date"].dt.strftime("%Y-%m-%d")
        elif period == "week":
            df["period"] = df["date"].dt.to_period("W").astype(str)
        elif period == "month":
            df["period"] = df["date"].dt.strftime("%Y-%m")
        elif period == "quarter":
            df["period"] = df["date"].dt.to_period("Q").astype(str)
        elif period == "year":
            df["period"] = df["date"].dt.strftime("%Y")
        else:
            df["period"] = df["date"].dt.strftime("%Y-%m")

        grouped = df.groupby("period")["amount"].sum().to_dict()
        total = sum(grouped.values())

        return self._format_result(
            value=float(total), period=period, breakdown=grouped, period_count=len(grouped)
        )


class RevenueGrowth(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="revenue_growth",
            display_name="Revenue Growth Rate",
            description="Period-over-period revenue growth percentage",
            category="revenue",
            unit="%",
            formula="((current - previous) / previous) * 100",
            required_columns=["amount", "date"],
        )

    def calculate(self, period: str = "month", **kwargs) -> MetricResult:
        df = self.df.copy()
        df["date"] = pd.to_datetime(df["date"])

        if "status" in df.columns:
            df = df[df["status"].str.lower().isin(TotalRevenue.PAID_STATUSES)]

        if period == "month":
            df["period"] = df["date"].dt.to_period("M")
        elif period == "week":
            df["period"] = df["date"].dt.to_period("W")
        elif period == "quarter":
            df["period"] = df["date"].dt.to_period("Q")
        elif period == "year":
            df["period"] = df["date"].dt.to_period("Y")
        else:
            df["period"] = df["date"].dt.to_period("M")

        revenue_by_period = df.groupby("period")["amount"].sum().sort_index()

        if len(revenue_by_period) < 2:
            return self._format_result(
                value=0.0,
                period=period,
                message="Insufficient data for growth calculation",
                periods_available=len(revenue_by_period),
            )

        current = float(revenue_by_period.iloc[-1])
        previous = float(revenue_by_period.iloc[-2])

        if previous == 0:
            growth = 100.0 if current > 0 else 0.0
        else:
            growth = ((current - previous) / previous) * 100

        return self._format_result(
            value=growth,
            period=period,
            current_period=str(revenue_by_period.index[-1]),
            previous_period=str(revenue_by_period.index[-2]),
            current_revenue=current,
            previous_revenue=previous,
        )


class MRR(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="mrr",
            display_name="Monthly Recurring Revenue",
            description="Recurring revenue normalized to monthly amount",
            category="revenue",
            unit="$",
            formula="SUM(amount normalized to monthly)",
            required_columns=["amount"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        if "status" in df.columns:
            active_statuses = ["active", "paid", "current"]
            df = df[df["status"].str.lower().isin(active_statuses)]

        if "billing_period" in df.columns:
            df["monthly_amount"] = df.apply(self._normalize_to_monthly, axis=1)
        else:
            df["monthly_amount"] = df["amount"]

        mrr = df["monthly_amount"].sum()
        subscriber_count = len(df)
        avg_per_sub = mrr / subscriber_count if subscriber_count > 0 else 0

        return self._format_result(
            value=float(mrr),
            subscriber_count=subscriber_count,
            average_per_subscriber=round(float(avg_per_sub), 2),
        )

    def _normalize_to_monthly(self, row) -> float:
        amount = float(row["amount"])
        period = str(row.get("billing_period", "monthly")).lower()

        multipliers = {
            "monthly": 1,
            "month": 1,
            "annual": 1 / 12,
            "yearly": 1 / 12,
            "year": 1 / 12,
            "quarterly": 1 / 3,
            "quarter": 1 / 3,
            "weekly": 4.33,
            "week": 4.33,
        }
        return amount * multipliers.get(period, 1)


class ARR(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="arr",
            display_name="Annual Recurring Revenue",
            description="Monthly recurring revenue annualized (MRR * 12)",
            category="revenue",
            unit="$",
            formula="MRR * 12",
            required_columns=["amount"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        mrr_metric = MRR(self.df)
        mrr_result = mrr_metric.calculate()

        arr = mrr_result.value * 12

        return self._format_result(
            value=float(arr),
            mrr=mrr_result.value,
            subscriber_count=mrr_result.metadata.get("subscriber_count", 0),
        )


class AverageOrderValue(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="average_order_value",
            display_name="Average Order Value",
            description="Average revenue per transaction",
            category="revenue",
            unit="$",
            formula="SUM(amount) / COUNT(transactions)",
            required_columns=["amount"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        if "status" in df.columns:
            df = df[df["status"].str.lower().isin(TotalRevenue.PAID_STATUSES)]

        if len(df) == 0:
            return self._format_result(value=0.0, transaction_count=0)

        avg = df["amount"].mean()
        total = df["amount"].sum()

        return self._format_result(
            value=float(avg),
            transaction_count=len(df),
            total_revenue=round(float(total), 2),
            min_order=round(float(df["amount"].min()), 2),
            max_order=round(float(df["amount"].max()), 2),
        )


class RevenueByProduct(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="revenue_by_product",
            display_name="Revenue by Product",
            description="Revenue breakdown by product or plan",
            category="revenue",
            unit="$",
            formula="SUM(amount) GROUP BY product",
            required_columns=["amount", "product"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        if "status" in df.columns:
            df = df[df["status"].str.lower().isin(TotalRevenue.PAID_STATUSES)]

        breakdown = df.groupby("product").agg({"amount": ["sum", "count", "mean"]}).round(2)

        breakdown.columns = ["revenue", "transactions", "avg_order"]
        breakdown = breakdown.sort_values("revenue", ascending=False)

        total = breakdown["revenue"].sum()
        top_product = breakdown.index[0] if len(breakdown) > 0 else None

        return self._format_result(
            value=float(total),
            breakdown=breakdown.to_dict("index"),
            product_count=len(breakdown),
            top_product=top_product,
        )
