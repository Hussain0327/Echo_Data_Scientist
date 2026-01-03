import re
from typing import Any, List

import pandas as pd

from app.models.schemas import ColumnInfo, SchemaInfo


class SchemaDetector:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def detect(self) -> SchemaInfo:
        columns_info = {}
        for col in self.df.columns:
            columns_info[col] = self._analyze_column(col)

        return SchemaInfo(
            columns=columns_info, total_rows=len(self.df), total_columns=len(self.df.columns)
        )

    def _analyze_column(self, col: str) -> ColumnInfo:
        series = self.df[col]
        data_type = self._detect_type(series)
        non_null = series.dropna()
        sample_values = self._get_sample_values(non_null)

        return ColumnInfo(
            name=col,
            data_type=data_type,
            nullable=series.isnull().any(),
            sample_values=sample_values,
            null_count=int(series.isnull().sum()),
            unique_count=int(series.nunique()),
        )

    def _get_sample_values(self, series: pd.Series, n: int = 5) -> List[Any]:
        samples = series.head(n).tolist()
        result = []
        for val in samples:
            if pd.isna(val):
                continue
            if isinstance(val, (int, float, str, bool)):
                result.append(val)
            else:
                result.append(str(val))
        return result

    def _detect_type(self, series: pd.Series) -> str:
        non_null = series.dropna()

        if len(non_null) == 0:
            return "unknown"

        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        if pd.api.types.is_numeric_dtype(series):
            if self._looks_like_currency(non_null):
                return "currency"
            if pd.api.types.is_integer_dtype(series):
                return "integer"
            return "numeric"

        str_series = non_null.astype(str)

        if self._is_date_string(str_series):
            return "date"

        if self._is_boolean_string(str_series):
            return "boolean"

        if self._is_email(str_series):
            return "email"

        if self._is_url(str_series):
            return "url"

        if self._is_currency_string(str_series):
            return "currency"

        return "string"

    def _looks_like_currency(self, series: pd.Series) -> bool:
        if not pd.api.types.is_numeric_dtype(series):
            return False
        sample = series.head(20)
        has_decimals = any(val != int(val) for val in sample if pd.notna(val))
        col_name = series.name.lower() if series.name else ""
        currency_keywords = ["amount", "price", "cost", "revenue", "total", "payment", "fee"]
        name_suggests_currency = any(kw in col_name for kw in currency_keywords)
        return has_decimals and name_suggests_currency

    def _is_currency_string(self, series: pd.Series) -> bool:
        sample = series.head(10)
        currency_pattern = re.compile(r"^[\$\£\€\¥]?\s*[\d,]+\.?\d*$")
        matches = sum(1 for val in sample if currency_pattern.match(str(val).strip()))
        return matches >= len(sample) * 0.8

    def _is_date_string(self, series: pd.Series) -> bool:
        sample = series.head(10)
        try:
            parsed = pd.to_datetime(sample, errors="coerce")
            valid_count = parsed.notna().sum()
            return valid_count >= len(sample) * 0.8
        except Exception:
            return False

    def _is_boolean_string(self, series: pd.Series) -> bool:
        unique_vals = set(series.astype(str).str.lower().str.strip().unique())
        bool_values = {"true", "false", "t", "f", "yes", "no", "y", "n", "1", "0", ""}
        return unique_vals.issubset(bool_values) and len(unique_vals - {""}) <= 2

    def _is_email(self, series: pd.Series) -> bool:
        sample = series.head(10)
        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        matches = sum(1 for val in sample if email_pattern.match(str(val).strip()))
        return matches >= len(sample) * 0.8

    def _is_url(self, series: pd.Series) -> bool:
        sample = series.head(10)
        url_pattern = re.compile(r"^https?://[^\s]+$")
        matches = sum(1 for val in sample if url_pattern.match(str(val).strip()))
        return matches >= len(sample) * 0.8
