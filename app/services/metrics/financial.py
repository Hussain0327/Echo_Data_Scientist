import pandas as pd

from app.services.metrics.base import BaseMetric, MetricDefinition, MetricResult


class CAC(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="cac",
            display_name="Customer Acquisition Cost",
            description="Average cost to acquire a new customer",
            category="financial",
            unit="$",
            formula="Total Marketing Spend / New Customers Acquired",
            required_columns=["spend", "conversions"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        total_spend = self.df["spend"].sum()
        total_conversions = self.df["conversions"].sum()

        if total_conversions == 0:
            cac = 0.0
        else:
            cac = total_spend / total_conversions

        return self._format_result(
            value=float(cac),
            total_spend=round(float(total_spend), 2),
            total_conversions=int(total_conversions),
        )


class LTV(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="ltv",
            display_name="Customer Lifetime Value",
            description="Predicted total revenue from a customer",
            category="financial",
            unit="$",
            formula="Average Revenue Per Customer * Avg Lifespan (months)",
            required_columns=["amount", "customer_id"],
        )

    def calculate(self, avg_lifespan_months: int = 24, **kwargs) -> MetricResult:
        revenue_per_customer = self.df.groupby("customer_id")["amount"].sum()

        if len(revenue_per_customer) == 0:
            return self._format_result(value=0.0, customer_count=0)

        avg_revenue = revenue_per_customer.mean()
        months_in_data = self._estimate_data_months()

        if months_in_data > 0:
            monthly_value = avg_revenue / months_in_data
            ltv = monthly_value * avg_lifespan_months
        else:
            ltv = avg_revenue

        return self._format_result(
            value=float(ltv),
            avg_customer_revenue=round(float(avg_revenue), 2),
            customer_count=len(revenue_per_customer),
            assumed_lifespan_months=avg_lifespan_months,
            data_months=months_in_data,
        )

    def _estimate_data_months(self) -> int:
        if "date" not in self.df.columns:
            return 1

        dates = pd.to_datetime(self.df["date"])
        if len(dates) == 0:
            return 1

        date_range = (dates.max() - dates.min()).days
        months = max(1, date_range // 30)
        return months


class LTVCACRatio(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="ltv_cac_ratio",
            display_name="LTV:CAC Ratio",
            description="Ratio of customer lifetime value to acquisition cost",
            category="financial",
            unit="ratio",
            formula="LTV / CAC",
            required_columns=["amount", "customer_id", "spend", "conversions"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        ltv_metric = LTV(self.df)
        ltv_result = ltv_metric.calculate(**kwargs)

        cac_metric = CAC(self.df)
        cac_result = cac_metric.calculate()

        if cac_result.value == 0:
            ratio = 0.0
            status = "unknown"
        else:
            ratio = ltv_result.value / cac_result.value
            if ratio >= 3:
                status = "healthy"
            elif ratio >= 1:
                status = "acceptable"
            else:
                status = "concerning"

        return self._format_result(
            value=ratio, ltv=ltv_result.value, cac=cac_result.value, status=status
        )


class GrossMargin(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="gross_margin",
            display_name="Gross Margin",
            description="Revenue minus cost of goods sold as percentage",
            category="financial",
            unit="%",
            formula="((Revenue - COGS) / Revenue) * 100",
            required_columns=["amount", "cost"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        revenue = self.df["amount"].sum()
        cost = self.df["cost"].sum()

        if revenue == 0:
            return self._format_result(value=0.0, revenue=0, cost=0)

        gross_profit = revenue - cost
        margin = (gross_profit / revenue) * 100

        return self._format_result(
            value=float(margin),
            revenue=round(float(revenue), 2),
            cost=round(float(cost), 2),
            gross_profit=round(float(gross_profit), 2),
        )


class BurnRate(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="burn_rate",
            display_name="Burn Rate",
            description="Average monthly cash outflow",
            category="financial",
            unit="$/month",
            formula="Total Expenses / Number of Months",
            required_columns=["expense", "date"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()
        df["date"] = pd.to_datetime(df["date"])

        df["month"] = df["date"].dt.to_period("M")
        monthly_expenses = df.groupby("month")["expense"].sum()

        if len(monthly_expenses) == 0:
            return self._format_result(value=0.0, months=0)

        avg_burn = monthly_expenses.mean()
        total_burn = monthly_expenses.sum()

        return self._format_result(
            value=float(avg_burn),
            total_expenses=round(float(total_burn), 2),
            months=len(monthly_expenses),
            monthly_breakdown={str(k): round(float(v), 2) for k, v in monthly_expenses.items()},
        )


class Runway(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="runway",
            display_name="Runway",
            description="Months of operation remaining at current burn rate",
            category="financial",
            unit="months",
            formula="Cash Balance / Monthly Burn Rate",
            required_columns=["expense", "date"],
        )

    def calculate(self, cash_balance: float = 0, **kwargs) -> MetricResult:
        burn_metric = BurnRate(self.df)
        burn_result = burn_metric.calculate()

        if burn_result.value == 0 or cash_balance == 0:
            return self._format_result(
                value=0.0,
                cash_balance=cash_balance,
                burn_rate=burn_result.value,
                message="Need cash_balance parameter and expense data",
            )

        runway = cash_balance / burn_result.value

        if runway >= 18:
            status = "healthy"
        elif runway >= 6:
            status = "monitor"
        else:
            status = "critical"

        return self._format_result(
            value=runway, cash_balance=cash_balance, burn_rate=burn_result.value, status=status
        )
