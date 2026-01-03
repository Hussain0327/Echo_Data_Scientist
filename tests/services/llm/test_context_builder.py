"""Tests for the DataContextBuilder."""

import pandas as pd

from app.services.llm.context_builder import DataContextBuilder


class TestDataContextBuilder:
    """Tests for data context building."""

    def test_build_data_summary_basic(self):
        """Test basic data summary generation."""
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "amount": [100.0, 200.0, 150.0],
                "customer_id": ["C001", "C002", "C001"],
            }
        )

        summary = DataContextBuilder.build_data_summary(df, "test_data.csv")

        assert "test_data.csv" in summary
        assert "Rows**: 3" in summary
        assert "Columns**: 3" in summary
        assert "amount" in summary
        assert "customer_id" in summary

    def test_build_data_summary_empty(self):
        """Test summary for empty dataframe."""
        df = pd.DataFrame()
        summary = DataContextBuilder.build_data_summary(df)
        assert summary == "No data loaded."

    def test_build_data_summary_none(self):
        """Test summary for None dataframe."""
        summary = DataContextBuilder.build_data_summary(None)
        assert summary == "No data loaded."

    def test_build_data_summary_with_date_range(self):
        """Test that date range is detected."""
        df = pd.DataFrame(
            {"date": pd.to_datetime(["2024-01-01", "2024-03-15", "2024-06-30"]), "value": [1, 2, 3]}
        )

        summary = DataContextBuilder.build_data_summary(df)
        assert "Date Range" in summary
        assert "2024-01-01" in summary
        assert "2024-06-30" in summary

    def test_build_data_summary_with_numeric_ranges(self):
        """Test that numeric ranges are included."""
        df = pd.DataFrame({"revenue": [100.0, 500.0, 1000.0], "quantity": [1, 5, 10]})

        summary = DataContextBuilder.build_data_summary(df)
        assert "Numeric Column Ranges" in summary
        assert "revenue" in summary
        assert "min=" in summary
        assert "max=" in summary

    def test_build_metrics_summary_basic(self):
        """Test basic metrics summary."""
        metrics = {
            "total_revenue": {
                "value": 50000.0,
                "unit": "$",
                "category": "revenue",
                "metadata": {"transaction_count": 100},
            },
            "conversion_rate": {
                "value": 12.5,
                "unit": "%",
                "category": "marketing",
                "metadata": {},
            },
        }

        summary = DataContextBuilder.build_metrics_summary(metrics)

        assert "Calculated Metrics" in summary
        assert "Total Revenue" in summary
        assert "$50,000.00" in summary
        assert "Conversion Rate" in summary
        assert "12.5%" in summary

    def test_build_metrics_summary_empty(self):
        """Test metrics summary for empty dict."""
        summary = DataContextBuilder.build_metrics_summary({})
        assert summary == "No metrics calculated yet."

    def test_build_metrics_summary_none(self):
        """Test metrics summary for None."""
        summary = DataContextBuilder.build_metrics_summary(None)
        assert summary == "No metrics calculated yet."

    def test_build_metrics_summary_various_units(self):
        """Test various unit formatting."""
        metrics = {
            "runway": {"value": 18.5, "unit": "months", "category": "financial"},
            "ltv_cac_ratio": {"value": 3.2, "unit": "ratio", "category": "financial"},
        }

        summary = DataContextBuilder.build_metrics_summary(metrics)
        assert "18.5 months" in summary
        assert "3.20x" in summary

    def test_build_quick_stats(self):
        """Test quick stats generation."""
        df = pd.DataFrame(
            {
                "amount": [100.0, 200.0, 300.0],
                "customer_id": ["C1", "C2", "C1"],
                "product": ["A", "B", "A"],
            }
        )

        stats = DataContextBuilder.build_quick_stats(df)

        assert "Quick Stats" in stats
        assert "Total records: 3" in stats
        assert "Unique customer_ids: 2" in stats
        assert "Unique products: 2" in stats

    def test_build_quick_stats_empty(self):
        """Test quick stats for empty dataframe."""
        df = pd.DataFrame()
        stats = DataContextBuilder.build_quick_stats(df)
        assert stats == ""

    def test_build_full_context(self):
        """Test building full context."""
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "amount": [100.0, 200.0]})
        metrics = {"total_revenue": {"value": 300.0, "unit": "$", "category": "revenue"}}

        data_summary, metrics_summary = DataContextBuilder.build_full_context(
            df=df, metrics=metrics, source_name="sales.csv"
        )

        assert "sales.csv" in data_summary
        assert "Total Revenue" in metrics_summary

    def test_build_full_context_no_data(self):
        """Test full context with no data."""
        data_summary, metrics_summary = DataContextBuilder.build_full_context()

        assert data_summary == ""
        assert metrics_summary == ""
