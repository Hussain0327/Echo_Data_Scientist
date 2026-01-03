from typing import Any, Dict

import numpy as np
import pandas as pd


class TimeSeriesAnalyzer:
    def __init__(self, df: pd.DataFrame, date_column: str = "date"):
        self.df = df.copy()
        self.date_column = date_column
        self.df[date_column] = pd.to_datetime(self.df[date_column])

    def group_by_period(
        self, value_column: str, period: str = "month", agg_func: str = "sum"
    ) -> pd.Series:
        df = self.df.copy()

        period_map = {"day": "D", "week": "W", "month": "M", "quarter": "Q", "year": "Y"}

        period_code = period_map.get(period, "M")
        df["period"] = df[self.date_column].dt.to_period(period_code)

        agg_funcs = {
            "sum": "sum",
            "mean": "mean",
            "count": "count",
            "min": "min",
            "max": "max",
            "median": "median",
        }

        func = agg_funcs.get(agg_func, "sum")
        return df.groupby("period")[value_column].agg(func)

    def calculate_growth(
        self, value_column: str, period: str = "month", periods_back: int = 1
    ) -> pd.DataFrame:
        grouped = self.group_by_period(value_column, period, "sum")

        result = pd.DataFrame(
            {
                "value": grouped,
                "previous": grouped.shift(periods_back),
            }
        )

        result["change"] = result["value"] - result["previous"]
        result["growth_pct"] = (result["change"] / result["previous"] * 100).round(2)

        result = result.fillna(0)

        return result

    def moving_average(self, value_column: str, window: int = 7, period: str = "day") -> pd.Series:
        grouped = self.group_by_period(value_column, period, "sum")
        return grouped.rolling(window=window, min_periods=1).mean()

    def detect_trend(self, value_column: str, period: str = "month") -> Dict[str, Any]:
        grouped = self.group_by_period(value_column, period, "sum")

        if len(grouped) < 3:
            return {"trend": "insufficient_data", "data_points": len(grouped)}

        values = grouped.values
        x = np.arange(len(values))

        correlation = np.corrcoef(x, values)[0, 1]

        if np.isnan(correlation):
            correlation = 0.0

        if correlation > 0.7:
            trend = "strong_upward"
        elif correlation > 0.3:
            trend = "upward"
        elif correlation < -0.7:
            trend = "strong_downward"
        elif correlation < -0.3:
            trend = "downward"
        else:
            trend = "stable"

        first_val = float(values[0])
        last_val = float(values[-1])

        if first_val != 0:
            total_change = ((last_val - first_val) / first_val) * 100
        else:
            total_change = 0.0

        return {
            "trend": trend,
            "correlation": round(correlation, 3),
            "first_value": round(first_val, 2),
            "last_value": round(last_val, 2),
            "total_change_pct": round(total_change, 2),
            "data_points": len(grouped),
        }

    def period_comparison(self, value_column: str, period: str = "month") -> Dict[str, Any]:
        grouped = self.group_by_period(value_column, period, "sum")

        if len(grouped) < 2:
            return {
                "current": float(grouped.iloc[-1]) if len(grouped) > 0 else 0,
                "previous": 0,
                "change": 0,
                "change_pct": 0,
                "periods": len(grouped),
            }

        current = float(grouped.iloc[-1])
        previous = float(grouped.iloc[-2])
        change = current - previous

        if previous != 0:
            change_pct = (change / previous) * 100
        else:
            change_pct = 100.0 if current > 0 else 0.0

        return {
            "current": round(current, 2),
            "current_period": str(grouped.index[-1]),
            "previous": round(previous, 2),
            "previous_period": str(grouped.index[-2]),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
        }

    def get_date_range(self) -> Dict[str, Any]:
        dates = self.df[self.date_column]
        return {
            "min_date": dates.min().strftime("%Y-%m-%d"),
            "max_date": dates.max().strftime("%Y-%m-%d"),
            "days": (dates.max() - dates.min()).days,
            "records": len(dates),
        }

    def fill_missing_periods(
        self, value_column: str, period: str = "day", fill_value: float = 0
    ) -> pd.Series:
        grouped = self.group_by_period(value_column, period, "sum")

        if len(grouped) == 0:
            return grouped

        full_range = pd.period_range(
            start=grouped.index.min(), end=grouped.index.max(), freq=grouped.index.freq
        )

        return grouped.reindex(full_range, fill_value=fill_value)

    def seasonal_pattern(self, value_column: str, by: str = "day_of_week") -> Dict[str, float]:
        df = self.df.copy()

        if by == "day_of_week":
            df["group"] = df[self.date_column].dt.day_name()
            order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        elif by == "month":
            df["group"] = df[self.date_column].dt.month_name()
            order = None
        elif by == "hour":
            df["group"] = df[self.date_column].dt.hour
            order = None
        else:
            df["group"] = df[self.date_column].dt.day_name()
            order = None

        grouped = df.groupby("group")[value_column].mean()

        if order:
            grouped = grouped.reindex(order)

        return {str(k): round(float(v), 2) for k, v in grouped.items() if pd.notna(v)}

    def outliers(
        self, value_column: str, method: str = "iqr", threshold: float = 1.5
    ) -> pd.DataFrame:
        values = self.df[value_column]

        if method == "iqr":
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            mask = (values < lower) | (values > upper)
        elif method == "zscore":
            mean = values.mean()
            std = values.std()
            if std == 0:
                mask = pd.Series([False] * len(values))
            else:
                z_scores = (values - mean) / std
                mask = abs(z_scores) > threshold
        else:
            mask = pd.Series([False] * len(values))

        return self.df[mask]


def compare_periods(
    df: pd.DataFrame, value_column: str, date_column: str = "date", period: str = "month"
) -> Dict[str, Any]:
    analyzer = TimeSeriesAnalyzer(df, date_column)
    return analyzer.period_comparison(value_column, period)


def calculate_trend(
    df: pd.DataFrame, value_column: str, date_column: str = "date", period: str = "month"
) -> Dict[str, Any]:
    analyzer = TimeSeriesAnalyzer(df, date_column)
    return analyzer.detect_trend(value_column, period)
