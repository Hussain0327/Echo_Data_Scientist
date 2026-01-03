from typing import Any, Dict, List, Optional

import pandas as pd


class DataContextBuilder:
    @staticmethod
    def build_data_summary(df: pd.DataFrame, source_name: str = "uploaded data") -> str:
        if df is None or df.empty:
            return "No data loaded."

        lines = [f"**Data Source**: {source_name}"]
        lines.append(f"**Rows**: {len(df):,}")
        lines.append(f"**Columns**: {len(df.columns)}")
        lines.append("")

        # Column summary
        lines.append("**Columns Available**:")
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            sample = df[col].dropna().iloc[0] if non_null > 0 else "N/A"

            # Truncate long sample values
            sample_str = str(sample)
            if len(sample_str) > 30:
                sample_str = sample_str[:30] + "..."

            lines.append(f"  - {col} ({dtype}): {non_null:,} values, e.g. '{sample_str}'")

        # Date range if date column exists
        date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()
        if not date_cols:
            # Check for string columns that might be dates
            for col in df.columns:
                if "date" in col.lower():
                    try:
                        parsed = pd.to_datetime(df[col], errors="coerce")
                        if parsed.notna().sum() > 0:
                            date_cols.append(col)
                            break
                    except Exception:
                        pass

        if date_cols:
            date_col = date_cols[0]
            try:
                dates = pd.to_datetime(df[date_col], errors="coerce")
                min_date = dates.min()
                max_date = dates.max()
                if pd.notna(min_date) and pd.notna(max_date):
                    lines.append("")
                    lines.append(
                        f"**Date Range**: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
                    )
            except Exception:
                pass

        # Numeric summary
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            lines.append("")
            lines.append("**Numeric Column Ranges**:")
            for col in numeric_cols[:5]:  # Limit to 5 columns
                min_val = df[col].min()
                max_val = df[col].max()
                mean_val = df[col].mean()
                lines.append(
                    f"  - {col}: min={min_val:,.2f}, max={max_val:,.2f}, avg={mean_val:,.2f}"
                )

        return "\n".join(lines)

    @staticmethod
    def build_metrics_summary(metrics: Dict[str, Any]) -> str:
        if not metrics:
            return "No metrics calculated yet."

        lines = ["**Calculated Metrics**:"]
        lines.append("")

        # Group by category if available
        categorized: Dict[str, List[tuple]] = {}

        for metric_name, result in metrics.items():
            if isinstance(result, dict):
                category = result.get("category", "general")
                value = result.get("value")
                unit = result.get("unit", "")
                metadata = result.get("metadata", {})

                if category not in categorized:
                    categorized[category] = []
                categorized[category].append((metric_name, value, unit, metadata))
            else:
                # Handle simple values
                if "general" not in categorized:
                    categorized["general"] = []
                categorized["general"].append((metric_name, result, "", {}))

        # Format by category
        for category, items in categorized.items():
            lines.append(f"### {category.replace('_', ' ').title()}")

            for metric_name, value, unit, metadata in items:
                display_name = metric_name.replace("_", " ").title()

                # Format value based on type
                if isinstance(value, (int, float)):
                    if unit == "$":
                        formatted_value = f"${value:,.2f}"
                    elif unit == "%":
                        formatted_value = f"{value:.1f}%"
                    elif unit == "months":
                        formatted_value = f"{value:.1f} months"
                    elif unit == "ratio":
                        formatted_value = f"{value:.2f}x"
                    else:
                        formatted_value = f"{value:,.2f}{unit}"
                else:
                    formatted_value = str(value)

                lines.append(f"- **{display_name}**: {formatted_value}")

                # Add key metadata
                if metadata:
                    for key, val in list(metadata.items())[:3]:  # Limit metadata
                        if key not in ["calculated_at", "metric_name"]:
                            if isinstance(val, (int, float)):
                                lines.append(f"  - {key}: {val:,.2f}")
                            elif isinstance(val, dict):
                                # Skip complex nested structures
                                pass
                            else:
                                lines.append(f"  - {key}: {val}")

            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def build_quick_stats(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return ""

        stats = []

        # Revenue/amount detection
        amount_cols = [
            c
            for c in df.columns
            if any(term in c.lower() for term in ["amount", "revenue", "price", "total", "value"])
        ]
        for col in amount_cols[:1]:  # First match
            if df[col].dtype in ["int64", "float64"]:
                total = df[col].sum()
                avg = df[col].mean()
                stats.append(f"Total {col}: ${total:,.2f}")
                stats.append(f"Average {col}: ${avg:,.2f}")

        # Count/volume stats
        stats.append(f"Total records: {len(df):,}")

        # Unique values in key columns
        for col in ["customer_id", "product", "source", "channel", "campaign"]:
            if col in df.columns:
                unique = df[col].nunique()
                stats.append(f"Unique {col}s: {unique:,}")

        if stats:
            return "**Quick Stats**:\n" + "\n".join(f"- {s}" for s in stats)
        return ""

    @classmethod
    def build_full_context(
        cls,
        df: Optional[pd.DataFrame] = None,
        metrics: Optional[Dict[str, Any]] = None,
        source_name: str = "uploaded data",
    ) -> tuple[str, str]:
        data_summary = ""
        if df is not None and not df.empty:
            data_summary = cls.build_data_summary(df, source_name)
            quick_stats = cls.build_quick_stats(df)
            if quick_stats:
                data_summary += "\n\n" + quick_stats

        metrics_summary = cls.build_metrics_summary(metrics) if metrics else ""

        return data_summary, metrics_summary
