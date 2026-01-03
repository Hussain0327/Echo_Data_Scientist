import pandas as pd
import pytest

from app.services.metrics.revenue import (
    ARR,
    MRR,
    AverageOrderValue,
    RevenueByPeriod,
    RevenueByProduct,
    RevenueGrowth,
    TotalRevenue,
)


class TestTotalRevenue:
    def test_basic_sum(self):
        df = pd.DataFrame({"amount": [100, 200, 300]})
        metric = TotalRevenue(df)
        result = metric.calculate()

        assert result.value == 600.0
        assert result.unit == "$"
        assert result.metadata["transaction_count"] == 3
        assert result.metadata["average_transaction"] == 200.0

    def test_filters_unpaid_transactions(self):
        df = pd.DataFrame(
            {"amount": [100, 200, 300, 400], "status": ["paid", "failed", "paid", "refunded"]}
        )
        metric = TotalRevenue(df)
        result = metric.calculate()

        assert result.value == 400.0
        assert result.metadata["transaction_count"] == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame({"amount": []})
        metric = TotalRevenue(df)
        result = metric.calculate()

        assert result.value == 0.0

    def test_all_paid_statuses(self):
        df = pd.DataFrame(
            {"amount": [100, 100, 100, 100], "status": ["paid", "success", "completed", "active"]}
        )
        metric = TotalRevenue(df)
        result = metric.calculate()

        assert result.value == 400.0

    def test_missing_required_column(self):
        df = pd.DataFrame({"not_amount": [100, 200]})
        with pytest.raises(ValueError) as exc:
            TotalRevenue(df)
        assert "Missing columns" in str(exc.value)


class TestRevenueByPeriod:
    def test_monthly_grouping(self):
        df = pd.DataFrame(
            {"date": ["2024-01-15", "2024-01-20", "2024-02-10"], "amount": [100, 200, 300]}
        )
        metric = RevenueByPeriod(df)
        result = metric.calculate(period="month")

        assert result.value == 600.0
        assert "2024-01" in result.metadata["breakdown"]
        assert "2024-02" in result.metadata["breakdown"]
        assert result.metadata["breakdown"]["2024-01"] == 300.0
        assert result.metadata["breakdown"]["2024-02"] == 300.0

    def test_daily_grouping(self):
        df = pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-01", "2024-01-02"], "amount": [100, 200, 300]}
        )
        metric = RevenueByPeriod(df)
        result = metric.calculate(period="day")

        assert result.metadata["breakdown"]["2024-01-01"] == 300.0
        assert result.metadata["breakdown"]["2024-01-02"] == 300.0


class TestRevenueGrowth:
    def test_positive_growth(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "amount": [100, 150]})
        metric = RevenueGrowth(df)
        result = metric.calculate(period="month")

        assert result.value == 50.0
        assert result.metadata["current_revenue"] == 150.0
        assert result.metadata["previous_revenue"] == 100.0

    def test_negative_growth(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "amount": [200, 100]})
        metric = RevenueGrowth(df)
        result = metric.calculate(period="month")

        assert result.value == -50.0

    def test_insufficient_data(self):
        df = pd.DataFrame({"date": ["2024-01-15"], "amount": [100]})
        metric = RevenueGrowth(df)
        result = metric.calculate(period="month")

        assert result.value == 0.0
        assert "Insufficient data" in result.metadata.get("message", "")

    def test_zero_previous_period(self):
        df = pd.DataFrame(
            {"date": ["2024-01-15", "2024-02-15"], "amount": [0, 100], "status": ["paid", "paid"]}
        )
        metric = RevenueGrowth(df)
        result = metric.calculate(period="month")

        assert result.value == 100.0


class TestMRR:
    def test_monthly_subscriptions(self):
        df = pd.DataFrame(
            {
                "amount": [100, 200, 300],
                "billing_period": ["monthly", "monthly", "monthly"],
                "status": ["active", "active", "active"],
            }
        )
        metric = MRR(df)
        result = metric.calculate()

        assert result.value == 600.0
        assert result.metadata["subscriber_count"] == 3

    def test_annual_subscriptions(self):
        df = pd.DataFrame(
            {
                "amount": [1200, 2400],
                "billing_period": ["annual", "yearly"],
                "status": ["active", "active"],
            }
        )
        metric = MRR(df)
        result = metric.calculate()

        assert result.value == 300.0

    def test_quarterly_subscriptions(self):
        df = pd.DataFrame({"amount": [300], "billing_period": ["quarterly"], "status": ["active"]})
        metric = MRR(df)
        result = metric.calculate()

        assert result.value == 100.0

    def test_mixed_billing_periods(self):
        df = pd.DataFrame(
            {
                "amount": [100, 1200, 300],
                "billing_period": ["monthly", "annual", "quarterly"],
                "status": ["active", "active", "active"],
            }
        )
        metric = MRR(df)
        result = metric.calculate()

        assert result.value == 300.0

    def test_without_billing_period(self):
        df = pd.DataFrame({"amount": [100, 200], "status": ["active", "active"]})
        metric = MRR(df)
        result = metric.calculate()

        assert result.value == 300.0


class TestARR:
    def test_arr_from_mrr(self):
        df = pd.DataFrame(
            {
                "amount": [100, 200, 300],
                "billing_period": ["monthly", "monthly", "monthly"],
                "status": ["active", "active", "active"],
            }
        )
        metric = ARR(df)
        result = metric.calculate()

        assert result.value == 7200.0
        assert result.metadata["mrr"] == 600.0


class TestAverageOrderValue:
    def test_basic_average(self):
        df = pd.DataFrame({"amount": [100, 200, 300]})
        metric = AverageOrderValue(df)
        result = metric.calculate()

        assert result.value == 200.0
        assert result.metadata["min_order"] == 100.0
        assert result.metadata["max_order"] == 300.0

    def test_filters_failed_transactions(self):
        df = pd.DataFrame({"amount": [100, 200, 1000], "status": ["paid", "paid", "failed"]})
        metric = AverageOrderValue(df)
        result = metric.calculate()

        assert result.value == 150.0

    def test_empty_after_filter(self):
        df = pd.DataFrame({"amount": [100], "status": ["failed"]})
        metric = AverageOrderValue(df)
        result = metric.calculate()

        assert result.value == 0.0


class TestRevenueByProduct:
    def test_product_breakdown(self):
        df = pd.DataFrame(
            {
                "amount": [100, 200, 150, 250],
                "product": ["Basic", "Pro", "Basic", "Pro"],
                "status": ["paid", "paid", "paid", "paid"],
            }
        )
        metric = RevenueByProduct(df)
        result = metric.calculate()

        assert result.value == 700.0
        assert result.metadata["product_count"] == 2
        assert "Basic" in result.metadata["breakdown"]
        assert "Pro" in result.metadata["breakdown"]

    def test_top_product(self):
        df = pd.DataFrame(
            {
                "amount": [100, 500, 100],
                "product": ["Basic", "Enterprise", "Basic"],
                "status": ["paid", "paid", "paid"],
            }
        )
        metric = RevenueByProduct(df)
        result = metric.calculate()

        assert result.metadata["top_product"] == "Enterprise"
