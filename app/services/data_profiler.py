from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    total_count: int
    non_null_count: int
    null_count: int
    null_percentage: float
    unique_count: int
    cardinality: float

    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    variance: Optional[float] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    percentiles: Optional[dict] = None
    histogram: Optional[dict] = None
    top_values: Optional[list] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    date_range_days: Optional[int] = None
    sample_values: Optional[list] = None
    is_unique: bool = False
    is_constant: bool = False
    has_high_nulls: bool = False
    has_outliers: bool = False
    outlier_count: Optional[int] = None


@dataclass
class CorrelationInfo:
    column1: str
    column2: str
    correlation: float
    strength: str


@dataclass
class DataQualityWarning:
    severity: str
    column: Optional[str]
    issue: str
    details: str
    recommendation: str


@dataclass
class DataProfile:
    profile_timestamp: str
    source_name: Optional[str]
    row_count: int
    column_count: int
    memory_usage_mb: float
    columns: list[ColumnProfile]
    correlations: list[CorrelationInfo] = field(default_factory=list)
    correlation_matrix: Optional[dict] = None
    warnings: list[DataQualityWarning] = field(default_factory=list)
    overall_quality_score: float = 0.0
    duplicate_rows: int = 0
    duplicate_percentage: float = 0.0

    def to_dict(self) -> dict:
        return {
            "metadata": {
                "profile_timestamp": self.profile_timestamp,
                "source_name": self.source_name,
                "row_count": self.row_count,
                "column_count": self.column_count,
                "memory_usage_mb": round(self.memory_usage_mb, 2),
                "duplicate_rows": self.duplicate_rows,
                "duplicate_percentage": round(self.duplicate_percentage, 2),
                "overall_quality_score": round(self.overall_quality_score, 1),
            },
            "columns": [self._column_to_dict(col) for col in self.columns],
            "correlations": [
                {
                    "column1": c.column1,
                    "column2": c.column2,
                    "correlation": round(c.correlation, 4),
                    "strength": c.strength,
                }
                for c in self.correlations
            ],
            "warnings": [
                {
                    "severity": w.severity,
                    "column": w.column,
                    "issue": w.issue,
                    "details": w.details,
                    "recommendation": w.recommendation,
                }
                for w in self.warnings
            ],
        }

    def _column_to_dict(self, col: ColumnProfile) -> dict:
        result = {
            "name": col.name,
            "dtype": col.dtype,
            "total_count": col.total_count,
            "non_null_count": col.non_null_count,
            "null_count": col.null_count,
            "null_percentage": round(col.null_percentage, 2),
            "unique_count": col.unique_count,
            "cardinality": round(col.cardinality, 4),
            "is_unique": col.is_unique,
            "is_constant": col.is_constant,
            "has_high_nulls": col.has_high_nulls,
            "has_outliers": col.has_outliers,
        }

        # Add optional fields if present
        optional_fields = [
            "min_value",
            "max_value",
            "mean",
            "median",
            "std",
            "variance",
            "skewness",
            "kurtosis",
            "percentiles",
            "histogram",
            "top_values",
            "min_length",
            "max_length",
            "avg_length",
            "min_date",
            "max_date",
            "date_range_days",
            "sample_values",
            "outlier_count",
        ]

        for field_name in optional_fields:
            value = getattr(col, field_name)
            if value is not None:
                if isinstance(value, float):
                    result[field_name] = round(value, 4)
                else:
                    result[field_name] = value

        return result


class DataProfiler:
    def __init__(
        self,
        max_categories: int = 20,
        histogram_bins: int = 20,
        sample_size: int = 5,
        correlation_threshold: float = 0.5,
    ):
        self.max_categories = max_categories
        self.histogram_bins = histogram_bins
        self.sample_size = sample_size
        self.correlation_threshold = correlation_threshold

    def profile(
        self,
        df: pd.DataFrame,
        source_name: Optional[str] = None,
    ) -> DataProfile:
        row_count = len(df)
        column_count = len(df.columns)
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

        column_profiles = [self._profile_column(df[col], col) for col in df.columns]
        correlations, corr_matrix = self._calculate_correlations(df)
        duplicate_rows = df.duplicated().sum()
        duplicate_pct = (duplicate_rows / row_count * 100) if row_count > 0 else 0
        warnings = self._generate_warnings(df, column_profiles, duplicate_pct)
        quality_score = self._calculate_quality_score(column_profiles, duplicate_pct)

        return DataProfile(
            profile_timestamp=datetime.now().isoformat(),
            source_name=source_name,
            row_count=row_count,
            column_count=column_count,
            memory_usage_mb=memory_usage_mb,
            columns=column_profiles,
            correlations=correlations,
            correlation_matrix=corr_matrix,
            warnings=warnings,
            overall_quality_score=quality_score,
            duplicate_rows=duplicate_rows,
            duplicate_percentage=duplicate_pct,
        )

    def _profile_column(self, series: pd.Series, name: str) -> ColumnProfile:
        total = len(series)
        non_null = series.count()
        null_count = total - non_null
        null_pct = (null_count / total * 100) if total > 0 else 0
        unique = series.nunique()
        cardinality = unique / total if total > 0 else 0

        profile = ColumnProfile(
            name=name,
            dtype=str(series.dtype),
            total_count=total,
            non_null_count=non_null,
            null_count=null_count,
            null_percentage=null_pct,
            unique_count=unique,
            cardinality=cardinality,
            is_unique=(unique == non_null and non_null > 0),
            is_constant=(unique <= 1),
            has_high_nulls=(null_pct > 50),
        )

        non_null_values = series.dropna()
        if len(non_null_values) > 0:
            profile.sample_values = non_null_values.head(self.sample_size).tolist()

        if pd.api.types.is_numeric_dtype(series):
            self._profile_numeric(series, profile)
        elif pd.api.types.is_datetime64_any_dtype(series):
            self._profile_datetime(series, profile)
        else:
            self._profile_categorical(series, profile)

        return profile

    def _profile_numeric(self, series: pd.Series, profile: ColumnProfile) -> None:
        clean = series.dropna()
        if len(clean) == 0:
            return

        profile.min_value = float(clean.min())
        profile.max_value = float(clean.max())
        profile.mean = float(clean.mean())
        profile.median = float(clean.median())
        profile.std = float(clean.std()) if len(clean) > 1 else 0.0
        profile.variance = float(clean.var()) if len(clean) > 1 else 0.0

        if len(clean) > 2:
            profile.skewness = float(clean.skew())
            profile.kurtosis = float(clean.kurtosis())

        profile.percentiles = {
            "5": float(clean.quantile(0.05)),
            "25": float(clean.quantile(0.25)),
            "50": float(clean.quantile(0.50)),
            "75": float(clean.quantile(0.75)),
            "95": float(clean.quantile(0.95)),
        }

        try:
            counts, bin_edges = np.histogram(clean, bins=self.histogram_bins)
            profile.histogram = {
                "bins": [float(b) for b in bin_edges],
                "counts": [int(c) for c in counts],
            }
        except Exception:
            pass

        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        outliers = clean[(clean < lower_fence) | (clean > upper_fence)]

        if len(outliers) > 0:
            profile.has_outliers = True
            profile.outlier_count = len(outliers)

    def _profile_datetime(self, series: pd.Series, profile: ColumnProfile) -> None:
        clean = series.dropna()

        if len(clean) == 0:
            return

        profile.min_date = str(clean.min())
        profile.max_date = str(clean.max())

        date_range = clean.max() - clean.min()
        profile.date_range_days = date_range.days if hasattr(date_range, "days") else 0

    def _profile_categorical(self, series: pd.Series, profile: ColumnProfile) -> None:
        clean = series.dropna()
        if len(clean) == 0:
            return

        value_counts = clean.value_counts().head(self.max_categories)
        total = len(clean)
        profile.top_values = [
            {
                "value": str(val),
                "count": int(count),
                "percentage": round(count / total * 100, 2),
            }
            for val, count in value_counts.items()
        ]

        if series.dtype == object:
            str_series = clean.astype(str)
            lengths = str_series.str.len()
            profile.min_length = int(lengths.min())
            profile.max_length = int(lengths.max())
            profile.avg_length = float(lengths.mean())

    def _calculate_correlations(
        self,
        df: pd.DataFrame,
    ) -> tuple[list[CorrelationInfo], Optional[dict]]:
        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.shape[1] < 2:
            return [], None

        corr_matrix = numeric_df.corr()
        corr_dict = {
            col: {row: round(val, 4) for row, val in corr_matrix[col].items()}
            for col in corr_matrix.columns
        }

        correlations = []
        cols = corr_matrix.columns.tolist()

        for i, col1 in enumerate(cols):
            for col2 in cols[i + 1 :]:
                corr = corr_matrix.loc[col1, col2]

                if pd.notna(corr) and abs(corr) >= self.correlation_threshold:
                    strength = self._correlation_strength(corr)
                    correlations.append(
                        CorrelationInfo(
                            column1=col1,
                            column2=col2,
                            correlation=float(corr),
                            strength=strength,
                        )
                    )

        correlations.sort(key=lambda x: abs(x.correlation), reverse=True)

        return correlations, corr_dict

    def _correlation_strength(self, corr: float) -> str:
        abs_corr = abs(corr)
        if abs_corr >= 0.8:
            return "Very Strong"
        elif abs_corr >= 0.6:
            return "Strong"
        elif abs_corr >= 0.4:
            return "Moderate"
        elif abs_corr >= 0.2:
            return "Weak"
        else:
            return "Very Weak"

    def _generate_warnings(
        self,
        df: pd.DataFrame,
        profiles: list[ColumnProfile],
        duplicate_pct: float,
    ) -> list[DataQualityWarning]:
        warnings = []

        if duplicate_pct > 0:
            severity = "High" if duplicate_pct > 10 else "Medium" if duplicate_pct > 1 else "Low"
            warnings.append(
                DataQualityWarning(
                    severity=severity,
                    column=None,
                    issue="Duplicate Rows",
                    details=f"{duplicate_pct:.1f}% of rows are duplicates",
                    recommendation="Review and deduplicate if these are unintended",
                )
            )

        for profile in profiles:
            if profile.null_percentage > 50:
                warnings.append(
                    DataQualityWarning(
                        severity="High",
                        column=profile.name,
                        issue="High Null Percentage",
                        details=f"{profile.null_percentage:.1f}% null values",
                        recommendation="Consider imputation or investigate data source",
                    )
                )
            elif profile.null_percentage > 20:
                warnings.append(
                    DataQualityWarning(
                        severity="Medium",
                        column=profile.name,
                        issue="Moderate Null Percentage",
                        details=f"{profile.null_percentage:.1f}% null values",
                        recommendation="Assess impact on analysis",
                    )
                )

            if profile.is_constant and profile.non_null_count > 0:
                warnings.append(
                    DataQualityWarning(
                        severity="Low",
                        column=profile.name,
                        issue="Constant Column",
                        details="Column has only one unique value",
                        recommendation="Consider removing if not needed for analysis",
                    )
                )

            if (
                profile.cardinality > 0.9
                and not pd.api.types.is_numeric_dtype(df[profile.name])
                and profile.unique_count > 100
            ):
                warnings.append(
                    DataQualityWarning(
                        severity="Low",
                        column=profile.name,
                        issue="High Cardinality",
                        details=f"{profile.unique_count} unique values ({profile.cardinality:.1%} cardinality)",
                        recommendation="May need grouping or encoding for modeling",
                    )
                )

            if profile.has_outliers and profile.outlier_count:
                pct = profile.outlier_count / profile.total_count * 100
                severity = "High" if pct > 10 else "Medium" if pct > 5 else "Low"
                warnings.append(
                    DataQualityWarning(
                        severity=severity,
                        column=profile.name,
                        issue="Outliers Detected",
                        details=f"{profile.outlier_count} outliers ({pct:.1f}% of values)",
                        recommendation="Review outliers - may need capping or removal",
                    )
                )

            if profile.is_unique and profile.non_null_count > 100:
                warnings.append(
                    DataQualityWarning(
                        severity="Low",
                        column=profile.name,
                        issue="Potential ID Column",
                        details="All values are unique - may be an identifier",
                        recommendation="Exclude from statistical analysis if it's an ID",
                    )
                )

        return warnings

    def _calculate_quality_score(
        self,
        profiles: list[ColumnProfile],
        duplicate_pct: float,
    ) -> float:
        if not profiles:
            return 0.0

        scores = []

        for profile in profiles:
            col_score = 100
            col_score -= min(50, profile.null_percentage)

            if profile.has_outliers and profile.outlier_count:
                outlier_pct = profile.outlier_count / profile.total_count * 100
                col_score -= min(20, outlier_pct * 2)

            if profile.is_constant:
                col_score -= 10

            scores.append(max(0, col_score))

        avg_score = sum(scores) / len(scores)
        avg_score -= min(20, duplicate_pct * 2)

        return max(0, min(100, avg_score))

    def print_summary(self, profile: DataProfile) -> None:
        print("\n" + "=" * 60)
        print("DATA PROFILE SUMMARY")
        print("=" * 60)

        print(f"\nSource: {profile.source_name or 'Unknown'}")
        print(f"Generated: {profile.profile_timestamp}")
        print(f"\nShape: {profile.row_count:,} rows x {profile.column_count} columns")
        print(f"Memory: {profile.memory_usage_mb:.2f} MB")
        print(f"Duplicates: {profile.duplicate_rows:,} ({profile.duplicate_percentage:.1f}%)")
        print(f"Quality Score: {profile.overall_quality_score:.1f}/100")

        if profile.warnings:
            print(f"\nWarnings: {len(profile.warnings)}")
            high = sum(1 for w in profile.warnings if w.severity == "High")
            medium = sum(1 for w in profile.warnings if w.severity == "Medium")
            low = sum(1 for w in profile.warnings if w.severity == "Low")
            print(f"  High: {high}, Medium: {medium}, Low: {low}")

        print("\n" + "-" * 60)
        print("COLUMN SUMMARY")
        print("-" * 60)
        print(f"{'Column':<25} {'Type':<12} {'Non-Null':<12} {'Unique':<10}")
        print("-" * 60)

        for col in profile.columns:
            null_info = f"{col.non_null_count}/{col.total_count}"
            print(f"{col.name:<25} {col.dtype:<12} {null_info:<12} {col.unique_count:<10}")

        if profile.correlations:
            print("\n" + "-" * 60)
            print("NOTABLE CORRELATIONS")
            print("-" * 60)
            for corr in profile.correlations[:10]:
                print(
                    f"  {corr.column1} <-> {corr.column2}: {corr.correlation:.3f} ({corr.strength})"
                )

        if profile.warnings:
            print("\n" + "-" * 60)
            print("DATA QUALITY WARNINGS")
            print("-" * 60)
            for w in profile.warnings:
                col_info = f"[{w.column}] " if w.column else ""
                print(f"  [{w.severity}] {col_info}{w.issue}: {w.details}")

        print("\n" + "=" * 60)


def profile_dataframe(
    df: pd.DataFrame,
    source_name: Optional[str] = None,
) -> DataProfile:
    profiler = DataProfiler()
    return profiler.profile(df, source_name)
