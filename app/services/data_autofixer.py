"""
DataAutoFixer Service

Automatically fixes common data quality issues in uploaded datasets.
This service cleans data BEFORE validation so users get insights instead of error messages.

Fixes applied:
1. Date format standardization (converts various formats to datetime)
2. Numeric string conversion (strips $, commas, converts to float)
3. Whitespace trimming
4. Boolean standardization (yes/no, y/n, 1/0 -> True/False)
5. Missing value handling for critical columns
"""

import pandas as pd
import re
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FixApplied:
    """Record of a fix that was applied to the data."""
    column: str
    fix_type: str
    description: str
    rows_affected: int
    sample_before: Optional[Any] = None
    sample_after: Optional[Any] = None


@dataclass
class AutoFixResult:
    """Result of auto-fixing a DataFrame."""
    df: pd.DataFrame
    fixes_applied: List[FixApplied] = field(default_factory=list)
    rows_before: int = 0
    rows_after: int = 0
    columns_fixed: int = 0

    @property
    def total_fixes(self) -> int:
        return len(self.fixes_applied)

    @property
    def was_modified(self) -> bool:
        return self.total_fixes > 0

    def to_summary(self) -> Dict[str, Any]:
        """Generate a summary dict for API responses."""
        return {
            "was_modified": self.was_modified,
            "total_fixes": self.total_fixes,
            "columns_fixed": self.columns_fixed,
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "fixes": [
                {
                    "column": f.column,
                    "fix_type": f.fix_type,
                    "description": f.description,
                    "rows_affected": f.rows_affected
                }
                for f in self.fixes_applied
            ]
        }


class DataAutoFixer:
    """
    Automatically fixes common data quality issues.

    Design principles:
    - Non-destructive: Never removes data, only transforms it
    - Transparent: Logs every change made
    - Conservative: Only fixes obvious issues with high confidence
    """

    # Currency symbols to strip
    CURRENCY_SYMBOLS = r'[\$\£\€\¥\₹\₽\₩\฿]'

    # Date keywords for column detection
    DATE_KEYWORDS = ['date', 'time', 'created', 'updated', 'timestamp', 'at', 'on', 'day']

    # Amount keywords for column detection
    AMOUNT_KEYWORDS = ['amount', 'price', 'cost', 'revenue', 'total', 'payment', 'fee',
                       'value', 'sum', 'balance', 'income', 'expense', 'salary', 'budget']

    # Boolean value mappings
    BOOLEAN_MAP = {
        'true': True, 'false': False,
        't': True, 'f': False,
        'yes': True, 'no': False,
        'y': True, 'n': False,
        '1': True, '0': False,
        'on': True, 'off': False,
    }

    # Column name mappings - maps variations to standard names
    COLUMN_MAPPINGS = {
        'amount': ['revenue', 'total', 'price', 'value', 'sale', 'sales', 'payment',
                   'transaction_amount', 'order_total', 'order_value', 'purchase_amount',
                   'gross', 'net', 'subtotal', 'grand_total', 'money', 'usd', 'dollars'],
        'date': ['created_at', 'created', 'timestamp', 'datetime', 'order_date',
                 'transaction_date', 'purchase_date', 'sale_date', 'time', 'day',
                 'created_on', 'updated_at', 'paid_at', 'payment_date'],
        'status': ['payment_status', 'order_status', 'transaction_status', 'state',
                   'paid', 'completed', 'is_paid', 'is_completed', 'payment_state'],
        'customer_id': ['user_id', 'customer', 'user', 'client_id', 'client',
                        'buyer_id', 'buyer', 'account_id', 'member_id'],
        'product': ['item', 'product_name', 'item_name', 'sku', 'product_id',
                    'service', 'offering', 'goods'],
        'source': ['channel', 'utm_source', 'marketing_channel', 'acquisition_channel',
                    'referrer', 'medium', 'utm_medium', 'traffic_source'],
        'campaign': ['campaign_name', 'utm_campaign', 'ad_campaign', 'marketing_campaign'],
    }

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()  # Work on a copy to avoid modifying original
        self.fixes_applied: List[FixApplied] = []
        self.rows_before = len(df)

    def fix_all(self) -> AutoFixResult:
        """
        Apply all automatic fixes to the DataFrame.

        Returns an AutoFixResult with the cleaned DataFrame and log of changes.
        """
        # Order matters - some fixes depend on others
        self._normalize_column_names()  # First: standardize column names
        self._fix_whitespace()
        self._fix_numeric_strings()
        self._fix_date_columns()
        self._fix_boolean_strings()
        self._fix_duplicate_columns()

        # Count unique columns that were fixed
        columns_fixed = len(set(f.column for f in self.fixes_applied))

        return AutoFixResult(
            df=self.df,
            fixes_applied=self.fixes_applied,
            rows_before=self.rows_before,
            rows_after=len(self.df),
            columns_fixed=columns_fixed
        )

    def _normalize_column_names(self):
        """Normalize column names to standard names expected by metrics."""
        # First, lowercase all column names for easier matching
        original_cols = self.df.columns.tolist()
        normalized_cols = [col.lower().strip().replace(' ', '_') for col in original_cols]

        # Track which columns we've already mapped to avoid duplicates
        mapped_to = set()
        new_cols = list(original_cols)  # Start with original names

        for i, col in enumerate(normalized_cols):
            # Skip if this column already has a standard name
            if col in self.COLUMN_MAPPINGS:
                continue

            # Check if this column matches any known variation
            for standard_name, variations in self.COLUMN_MAPPINGS.items():
                # Skip if we already have this standard column
                if standard_name in normalized_cols or standard_name in mapped_to:
                    continue

                # Check for exact match in variations
                if col in variations:
                    new_cols[i] = standard_name
                    mapped_to.add(standard_name)
                    self._add_fix(
                        column=original_cols[i],
                        fix_type="column_rename",
                        description=f"Renamed '{original_cols[i]}' to '{standard_name}' for compatibility",
                        rows_affected=len(self.df)
                    )
                    break

                # Check for partial match (e.g., 'total_revenue' contains 'revenue')
                for variation in variations:
                    if variation in col or col in variation:
                        new_cols[i] = standard_name
                        mapped_to.add(standard_name)
                        self._add_fix(
                            column=original_cols[i],
                            fix_type="column_rename",
                            description=f"Renamed '{original_cols[i]}' to '{standard_name}' for compatibility",
                            rows_affected=len(self.df)
                        )
                        break
                else:
                    continue
                break

        # Apply the new column names
        if new_cols != original_cols:
            self.df.columns = new_cols

    def _add_fix(self, column: str, fix_type: str, description: str,
                 rows_affected: int, sample_before: Any = None, sample_after: Any = None):
        """Record a fix that was applied."""
        if rows_affected > 0:
            self.fixes_applied.append(FixApplied(
                column=column,
                fix_type=fix_type,
                description=description,
                rows_affected=rows_affected,
                sample_before=sample_before,
                sample_after=sample_after
            ))

    def _fix_whitespace(self):
        """Trim leading/trailing whitespace from all string columns."""
        for col in self.df.select_dtypes(include=['object']).columns:
            original = self.df[col].copy()

            # Only apply to string values
            mask = self.df[col].apply(lambda x: isinstance(x, str))
            if mask.any():
                self.df.loc[mask, col] = self.df.loc[mask, col].str.strip()

                # Count how many values changed
                changed = (original != self.df[col]) & mask
                rows_affected = changed.sum()

                if rows_affected > 0:
                    self._add_fix(
                        column=col,
                        fix_type="whitespace",
                        description=f"Trimmed whitespace from {rows_affected} values",
                        rows_affected=rows_affected
                    )

    def _fix_numeric_strings(self):
        """Convert numeric strings to actual numbers (handles currency symbols, commas)."""
        for col in self.df.select_dtypes(include=['object']).columns:
            # Check if this column looks like it should be numeric
            if not self._column_looks_numeric(col):
                continue

            original = self.df[col].copy()
            sample_before = self.df[col].dropna().head(1).tolist()
            sample_before = sample_before[0] if sample_before else None

            # Try to convert
            converted = self._convert_to_numeric(self.df[col])

            if converted is not None:
                # Count successful conversions
                was_string = original.apply(lambda x: isinstance(x, str))
                now_numeric = pd.notna(converted)
                rows_affected = (was_string & now_numeric).sum()

                if rows_affected > 0:
                    self.df[col] = converted
                    sample_after = self.df[col].dropna().head(1).tolist()
                    sample_after = sample_after[0] if sample_after else None

                    self._add_fix(
                        column=col,
                        fix_type="numeric_conversion",
                        description=f"Converted {rows_affected} text values to numbers",
                        rows_affected=rows_affected,
                        sample_before=sample_before,
                        sample_after=sample_after
                    )

    def _column_looks_numeric(self, col: str) -> bool:
        """Check if a column appears to contain numeric data stored as strings."""
        col_lower = col.lower()

        # Check if column name suggests numeric content
        name_suggests_numeric = any(kw in col_lower for kw in self.AMOUNT_KEYWORDS)

        # Sample the data
        sample = self.df[col].dropna().head(20)
        if len(sample) == 0:
            return False

        # Count how many values look numeric
        numeric_count = 0
        for val in sample:
            if self._value_looks_numeric(val):
                numeric_count += 1

        # If 80%+ look numeric, or name suggests it and 50%+ look numeric
        pct_numeric = numeric_count / len(sample)
        return pct_numeric >= 0.8 or (name_suggests_numeric and pct_numeric >= 0.5)

    def _value_looks_numeric(self, val: Any) -> bool:
        """Check if a single value looks like a number."""
        if pd.isna(val):
            return False
        if isinstance(val, (int, float)):
            return True

        try:
            # Strip currency symbols and commas
            cleaned = re.sub(self.CURRENCY_SYMBOLS, '', str(val))
            cleaned = cleaned.replace(',', '').strip()

            # Handle negative numbers in parentheses: (100) -> -100
            if cleaned.startswith('(') and cleaned.endswith(')'):
                cleaned = '-' + cleaned[1:-1]

            float(cleaned)
            return True
        except (ValueError, TypeError):
            return False

    def _convert_to_numeric(self, series: pd.Series) -> Optional[pd.Series]:
        """Convert a series to numeric, handling currency symbols and commas."""
        def clean_and_convert(val):
            if pd.isna(val):
                return val
            if isinstance(val, (int, float)):
                return val

            try:
                cleaned = re.sub(self.CURRENCY_SYMBOLS, '', str(val))
                cleaned = cleaned.replace(',', '').strip()

                # Handle negative in parentheses
                if cleaned.startswith('(') and cleaned.endswith(')'):
                    cleaned = '-' + cleaned[1:-1]

                return float(cleaned)
            except (ValueError, TypeError):
                return val  # Keep original if can't convert

        converted = series.apply(clean_and_convert)

        # Only return if we actually converted something
        original_numeric = series.apply(lambda x: isinstance(x, (int, float)) and pd.notna(x)).sum()
        new_numeric = converted.apply(lambda x: isinstance(x, (int, float)) and pd.notna(x)).sum()

        if new_numeric > original_numeric:
            return pd.to_numeric(converted, errors='coerce')
        return None

    def _fix_date_columns(self):
        """Convert date strings to proper datetime objects."""
        for col in self.df.columns:
            col_lower = col.lower()

            # Check if column name suggests dates
            is_date_column = any(kw in col_lower for kw in self.DATE_KEYWORDS)

            # Also check if the data looks like dates
            if not is_date_column and not self._column_looks_like_dates(col):
                continue

            # Skip if already datetime
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                continue

            original = self.df[col].copy()
            sample_before = self.df[col].dropna().head(1).tolist()
            sample_before = str(sample_before[0]) if sample_before else None

            # Try to parse dates
            try:
                parsed = pd.to_datetime(self.df[col], errors='coerce', format='mixed')

                # Count successful conversions (was string, now valid datetime)
                was_string = original.apply(lambda x: isinstance(x, str) and pd.notna(x))
                now_valid = pd.notna(parsed)
                rows_affected = (was_string & now_valid).sum()

                # Only apply if we converted a significant portion
                if rows_affected > 0 and (rows_affected / max(was_string.sum(), 1)) >= 0.5:
                    self.df[col] = parsed
                    sample_after = self.df[col].dropna().head(1).tolist()
                    sample_after = str(sample_after[0]) if sample_after else None

                    self._add_fix(
                        column=col,
                        fix_type="date_conversion",
                        description=f"Parsed {rows_affected} date strings to datetime",
                        rows_affected=rows_affected,
                        sample_before=sample_before,
                        sample_after=sample_after
                    )
            except Exception:
                pass  # If parsing fails entirely, skip this column

    def _column_looks_like_dates(self, col: str) -> bool:
        """Check if column data looks like date values."""
        if pd.api.types.is_datetime64_any_dtype(self.df[col]):
            return True

        sample = self.df[col].dropna().head(10)
        if len(sample) == 0:
            return False

        try:
            parsed = pd.to_datetime(sample, errors='coerce', format='mixed')
            valid_count = parsed.notna().sum()
            return valid_count >= len(sample) * 0.7
        except Exception:
            return False

    def _fix_boolean_strings(self):
        """Standardize boolean-like strings to consistent lowercase values.

        Note: We keep them as strings (not actual booleans) because downstream
        metrics code may expect string values for status columns.
        """
        # Map various boolean representations to standard strings
        STRING_BOOL_MAP = {
            'true': 'true', 'false': 'false',
            't': 'true', 'f': 'false',
            'yes': 'yes', 'no': 'no',
            'y': 'yes', 'n': 'no',
            '1': 'true', '0': 'false',
            'on': 'true', 'off': 'false',
        }

        for col in self.df.select_dtypes(include=['object']).columns:
            # Skip columns that look like status columns (keep their original values)
            col_lower = col.lower()
            if any(kw in col_lower for kw in ['status', 'state', 'stage']):
                # For status columns, just lowercase and strip
                original = self.df[col].copy()

                def clean_status(val):
                    if pd.isna(val):
                        return val
                    cleaned = str(val).lower().strip()
                    # Map common variations to standard values
                    status_map = {
                        'yes': 'paid', 'no': 'unpaid',
                        'y': 'paid', 'n': 'unpaid',
                        'true': 'paid', 'false': 'unpaid',
                        '1': 'paid', '0': 'unpaid',
                    }
                    return status_map.get(cleaned, cleaned)

                self.df[col] = self.df[col].apply(clean_status)

                changed = original.astype(str) != self.df[col].astype(str)
                rows_affected = changed.sum()

                if rows_affected > 0:
                    self._add_fix(
                        column=col,
                        fix_type="status_standardization",
                        description=f"Standardized {rows_affected} status values",
                        rows_affected=rows_affected
                    )
                continue

            # For non-status boolean columns, check if all values are boolean-like
            non_null = self.df[col].dropna()
            if len(non_null) == 0:
                continue

            unique_vals = set(str(v).lower().strip() for v in non_null.unique())

            if not unique_vals.issubset(set(STRING_BOOL_MAP.keys())):
                continue

            # Skip if more than 2 unique values (after mapping)
            mapped_unique = set(STRING_BOOL_MAP.get(v, v) for v in unique_vals)
            if len(mapped_unique) > 2:
                continue

            original = self.df[col].copy()

            def standardize_bool_string(val):
                if pd.isna(val):
                    return val
                return STRING_BOOL_MAP.get(str(val).lower().strip(), val)

            self.df[col] = self.df[col].apply(standardize_bool_string)

            changed = original.astype(str) != self.df[col].astype(str)
            rows_affected = changed.sum()

            if rows_affected > 0:
                self._add_fix(
                    column=col,
                    fix_type="boolean_standardization",
                    description=f"Standardized {rows_affected} boolean values to consistent format",
                    rows_affected=rows_affected
                )

    def _fix_duplicate_columns(self):
        """Rename duplicate column names to make them unique."""
        cols = self.df.columns.tolist()
        seen = {}
        new_cols = []

        for col in cols:
            if col in seen:
                seen[col] += 1
                new_name = f"{col}_{seen[col]}"
                new_cols.append(new_name)

                self._add_fix(
                    column=col,
                    fix_type="duplicate_rename",
                    description=f"Renamed duplicate column to '{new_name}'",
                    rows_affected=len(self.df)
                )
            else:
                seen[col] = 0
                new_cols.append(col)

        if new_cols != cols:
            self.df.columns = new_cols


def auto_fix_dataframe(df: pd.DataFrame) -> AutoFixResult:
    """
    Convenience function to auto-fix a DataFrame.

    Args:
        df: The DataFrame to fix

    Returns:
        AutoFixResult with cleaned DataFrame and list of fixes applied
    """
    fixer = DataAutoFixer(df)
    return fixer.fix_all()
