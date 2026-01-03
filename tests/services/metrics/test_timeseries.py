import pandas as pd

from app.services.metrics.timeseries import (
    TimeSeriesAnalyzer,
    calculate_trend,
    compare_periods,
)


class TestTimeSeriesAnalyzer:
    def test_group_by_month(self):
        df = pd.DataFrame(
            {"date": ["2024-01-15", "2024-01-20", "2024-02-10"], "amount": [100, 200, 300]}
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.group_by_period("amount", "month", "sum")

        assert len(result) == 2
        assert result.iloc[0] == 300
        assert result.iloc[1] == 300

    def test_group_by_day(self):
        df = pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-01", "2024-01-02"], "amount": [100, 200, 300]}
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.group_by_period("amount", "day", "sum")

        assert len(result) == 2

    def test_group_by_mean(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-01-20"], "amount": [100, 200]})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.group_by_period("amount", "month", "mean")

        assert result.iloc[0] == 150.0

    def test_calculate_growth(self):
        df = pd.DataFrame(
            {"date": ["2024-01-15", "2024-02-15", "2024-03-15"], "amount": [100, 150, 200]}
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.calculate_growth("amount", "month")

        assert "value" in result.columns
        assert "growth_pct" in result.columns
        assert result["growth_pct"].iloc[1] == 50.0

    def test_moving_average(self):
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "amount": [10] * 10})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.moving_average("amount", window=3, period="day")

        assert all(result.dropna() == 10.0)

    def test_detect_upward_trend(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=12, freq="M"),
                "amount": [100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320],
            }
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.detect_trend("amount", "month")

        assert result["trend"] in ["upward", "strong_upward"]
        assert result["correlation"] > 0.5

    def test_detect_downward_trend(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=6, freq="M"),
                "amount": [300, 250, 200, 150, 100, 50],
            }
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.detect_trend("amount", "month")

        assert result["trend"] in ["downward", "strong_downward"]
        assert result["correlation"] < -0.5

    def test_detect_stable_trend(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=6, freq="M"),
                "amount": [100, 102, 98, 101, 99, 100],
            }
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.detect_trend("amount", "month")

        assert result["trend"] == "stable"

    def test_insufficient_data_for_trend(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "amount": [100, 200]})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.detect_trend("amount", "month")

        assert result["trend"] == "insufficient_data"

    def test_period_comparison(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "amount": [100, 150]})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.period_comparison("amount", "month")

        assert result["current"] == 150.0
        assert result["previous"] == 100.0
        assert result["change"] == 50.0
        assert result["change_pct"] == 50.0

    def test_get_date_range(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-03-15"], "amount": [100, 200]})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.get_date_range()

        assert result["min_date"] == "2024-01-01"
        assert result["max_date"] == "2024-03-15"
        assert result["records"] == 2

    def test_seasonal_pattern_day_of_week(self):
        dates = pd.date_range("2024-01-01", periods=14)
        df = pd.DataFrame({"date": dates, "amount": [100, 200, 150, 120, 180, 90, 80] * 2})
        analyzer = TimeSeriesAnalyzer(df, "date")
        result = analyzer.seasonal_pattern("amount", "day_of_week")

        assert "Monday" in result
        assert len(result) == 7

    def test_outliers_iqr(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=10),
                "amount": [100, 100, 100, 100, 100, 100, 100, 100, 100, 1000],
            }
        )
        analyzer = TimeSeriesAnalyzer(df, "date")
        outliers = analyzer.outliers("amount", method="iqr")

        assert len(outliers) == 1
        assert outliers["amount"].iloc[0] == 1000


class TestHelperFunctions:
    def test_compare_periods(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "amount": [100, 200]})
        result = compare_periods(df, "amount", "date", "month")

        assert result["current"] == 200.0
        assert result["previous"] == 100.0
        assert result["change_pct"] == 100.0

    def test_calculate_trend(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=6, freq="M"),
                "amount": [100, 150, 200, 250, 300, 350],
            }
        )
        result = calculate_trend(df, "amount", "date", "month")

        assert result["trend"] in ["upward", "strong_upward"]
