import pandas as pd
import pytest

from app.services.metrics.financial import (
    CAC,
    LTV,
    BurnRate,
    GrossMargin,
    LTVCACRatio,
    Runway,
)


class TestCAC:
    def test_basic_calculation(self):
        df = pd.DataFrame({"spend": [1000, 2000, 1500], "conversions": [10, 20, 15]})
        metric = CAC(df)
        result = metric.calculate()

        assert result.value == 100.0
        assert result.metadata["total_spend"] == 4500.0
        assert result.metadata["total_conversions"] == 45

    def test_zero_conversions(self):
        df = pd.DataFrame({"spend": [1000], "conversions": [0]})
        metric = CAC(df)
        result = metric.calculate()

        assert result.value == 0.0

    def test_missing_columns(self):
        df = pd.DataFrame({"spend": [1000]})
        with pytest.raises(ValueError):
            CAC(df)


class TestLTV:
    def test_basic_calculation(self):
        df = pd.DataFrame(
            {
                "customer_id": ["A", "A", "B", "B"],
                "amount": [100, 100, 200, 200],
                "date": ["2024-01-01", "2024-02-01", "2024-01-01", "2024-02-01"],
            }
        )
        metric = LTV(df)
        result = metric.calculate(avg_lifespan_months=24)

        assert result.metadata["customer_count"] == 2
        assert result.metadata["avg_customer_revenue"] == 300.0

    def test_single_customer(self):
        df = pd.DataFrame({"customer_id": ["A", "A", "A"], "amount": [100, 200, 300]})
        metric = LTV(df)
        result = metric.calculate()

        assert result.metadata["customer_count"] == 1
        assert result.metadata["avg_customer_revenue"] == 600.0

    def test_empty_dataframe(self):
        df = pd.DataFrame({"customer_id": [], "amount": []})
        metric = LTV(df)
        result = metric.calculate()

        assert result.value == 0.0


class TestLTVCACRatio:
    def test_healthy_ratio(self):
        df = pd.DataFrame(
            {
                "customer_id": ["A", "B"],
                "amount": [1000, 1000],
                "spend": [100, 100],
                "conversions": [1, 1],
            }
        )
        metric = LTVCACRatio(df)
        result = metric.calculate()

        assert result.metadata["status"] == "healthy"
        assert result.value >= 3

    def test_concerning_ratio(self):
        df = pd.DataFrame(
            {"customer_id": ["A"], "amount": [10], "spend": [1000], "conversions": [1]}
        )
        metric = LTVCACRatio(df)
        result = metric.calculate()

        assert result.metadata["status"] == "concerning"

    def test_zero_cac(self):
        df = pd.DataFrame({"customer_id": ["A"], "amount": [100], "spend": [0], "conversions": [0]})
        metric = LTVCACRatio(df)
        result = metric.calculate()

        assert result.value == 0.0
        assert result.metadata["status"] == "unknown"


class TestGrossMargin:
    def test_basic_margin(self):
        df = pd.DataFrame({"amount": [1000], "cost": [600]})
        metric = GrossMargin(df)
        result = metric.calculate()

        assert result.value == 40.0
        assert result.metadata["gross_profit"] == 400.0

    def test_zero_revenue(self):
        df = pd.DataFrame({"amount": [0], "cost": [100]})
        metric = GrossMargin(df)
        result = metric.calculate()

        assert result.value == 0.0

    def test_hundred_percent_margin(self):
        df = pd.DataFrame({"amount": [1000], "cost": [0]})
        metric = GrossMargin(df)
        result = metric.calculate()

        assert result.value == 100.0


class TestBurnRate:
    def test_monthly_burn(self):
        df = pd.DataFrame(
            {"date": ["2024-01-15", "2024-02-15", "2024-03-15"], "expense": [10000, 12000, 11000]}
        )
        metric = BurnRate(df)
        result = metric.calculate()

        assert result.value == 11000.0
        assert result.metadata["months"] == 3
        assert result.metadata["total_expenses"] == 33000.0

    def test_single_month(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-15"], "expense": [5000, 5000]})
        metric = BurnRate(df)
        result = metric.calculate()

        assert result.value == 10000.0
        assert result.metadata["months"] == 1


class TestRunway:
    def test_healthy_runway(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "expense": [10000, 10000]})
        metric = Runway(df)
        result = metric.calculate(cash_balance=200000)

        assert result.value == 20.0
        assert result.metadata["status"] == "healthy"

    def test_critical_runway(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "expense": [10000, 10000]})
        metric = Runway(df)
        result = metric.calculate(cash_balance=30000)

        assert result.value == 3.0
        assert result.metadata["status"] == "critical"

    def test_no_cash_balance(self):
        df = pd.DataFrame({"date": ["2024-01-15"], "expense": [10000]})
        metric = Runway(df)
        result = metric.calculate()

        assert result.value == 0.0
