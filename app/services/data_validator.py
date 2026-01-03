from typing import List, Optional

import pandas as pd

from app.models.schemas import ValidationError


class DataValidator:
    def __init__(self, df: pd.DataFrame, use_case: Optional[str] = None):
        self.df = df
        self.use_case = use_case
        self.errors: List[ValidationError] = []

    def validate(self) -> List[ValidationError]:
        self.errors = []
        self._check_empty()
        self._check_minimum_columns()
        self._check_minimum_rows()
        self._check_data_quality()
        self._check_duplicate_columns()
        self._check_date_columns()
        self._check_numeric_columns()

        if self.use_case:
            self._check_use_case_requirements()

        return self.errors

    def _add_error(self, severity: str, field: str, message: str, suggestion: str):
        self.errors.append(
            ValidationError(severity=severity, field=field, message=message, suggestion=suggestion)
        )

    def _check_empty(self):
        if len(self.df) == 0:
            self._add_error(
                severity="error",
                field="file",
                message="File is empty",
                suggestion="Upload a file with at least one row of data",
            )

    def _check_minimum_columns(self):
        if len(self.df.columns) < 2:
            self._add_error(
                severity="error",
                field="columns",
                message="File must have at least 2 columns",
                suggestion="Add more columns with relevant data",
            )

    def _check_minimum_rows(self):
        if 0 < len(self.df) < 5:
            self._add_error(
                severity="warning",
                field="rows",
                message=f"File has only {len(self.df)} rows",
                suggestion="More data will produce better analysis results",
            )

    def _check_data_quality(self):
        if len(self.df) == 0:
            return

        total_cells = len(self.df) * len(self.df.columns)
        null_cells = self.df.isnull().sum().sum()
        null_pct = (null_cells / total_cells) * 100

        if null_pct > 50:
            self._add_error(
                severity="error",
                field="data_quality",
                message=f"File has {null_pct:.1f}% missing values",
                suggestion="Clean your data or fill missing values before uploading",
            )
        elif null_pct > 20:
            self._add_error(
                severity="warning",
                field="data_quality",
                message=f"File has {null_pct:.1f}% missing values",
                suggestion="Consider filling missing values for better analysis",
            )

    def _check_duplicate_columns(self):
        cols = self.df.columns.tolist()
        duplicates = set([col for col in cols if cols.count(col) > 1])
        if duplicates:
            self._add_error(
                severity="error",
                field="columns",
                message=f"Duplicate column names found: {', '.join(duplicates)}",
                suggestion="Rename duplicate columns to have unique names",
            )

    def _check_date_columns(self):
        date_cols = self._find_date_columns()

        if not date_cols:
            self._add_error(
                severity="warning",
                field="dates",
                message="No date columns detected",
                suggestion="Add a date column for time-based analysis (e.g., 'date', 'created_at')",
            )
            return

        for col in date_cols:
            self._validate_date_column(col)

    def _find_date_columns(self) -> List[str]:
        date_keywords = ["date", "time", "created", "updated", "timestamp"]
        candidates = []
        for col in self.df.columns:
            col_lower = col.lower()
            if any(kw in col_lower for kw in date_keywords):
                candidates.append(col)
        return candidates

    def _validate_date_column(self, col: str):
        try:
            parsed = pd.to_datetime(self.df[col], errors="coerce")
            invalid_count = parsed.isna().sum() - self.df[col].isna().sum()
            if invalid_count > 0:
                self._add_error(
                    severity="warning",
                    field=col,
                    message=f"Column '{col}' has {invalid_count} unparseable date values",
                    suggestion="Use standard date formats: YYYY-MM-DD, MM/DD/YYYY, etc.",
                )
        except Exception:
            self._add_error(
                severity="error",
                field=col,
                message=f"Column '{col}' cannot be parsed as dates",
                suggestion="Check the date format in this column",
            )

    def _check_numeric_columns(self):
        numeric_cols = self.df.select_dtypes(include=["number"]).columns

        if len(numeric_cols) == 0:
            potential_numeric = self._find_potential_numeric_columns()
            if potential_numeric:
                self._add_error(
                    severity="warning",
                    field="metrics",
                    message="No numeric columns detected",
                    suggestion=f"Columns {potential_numeric} may contain numbers stored as text",
                )
            else:
                self._add_error(
                    severity="error",
                    field="metrics",
                    message="No numeric columns found",
                    suggestion="Add numeric columns for metrics (revenue, quantity, etc.)",
                )

    def _find_potential_numeric_columns(self) -> List[str]:
        potential = []
        for col in self.df.select_dtypes(include=["object"]).columns:
            sample = self.df[col].dropna().head(10)
            numeric_count = 0
            for val in sample:
                try:
                    cleaned = str(val).replace("$", "").replace(",", "").strip()
                    float(cleaned)
                    numeric_count += 1
                except ValueError:
                    pass
            if numeric_count >= len(sample) * 0.8:
                potential.append(col)
        return potential

    def _check_use_case_requirements(self):
        if self.use_case == "revenue":
            self._validate_revenue_requirements()
        elif self.use_case == "marketing":
            self._validate_marketing_requirements()

    def _validate_revenue_requirements(self):
        required = {
            "amount": ["amount", "revenue", "total", "price", "value", "payment"],
            "date": ["date", "timestamp", "created_at", "order_date", "transaction_date"],
        }
        self._check_required_fields(required, "revenue analysis")

    def _validate_marketing_requirements(self):
        required = {
            "source": ["source", "campaign", "channel", "utm_source", "medium"],
            "status": ["status", "stage", "conversion", "converted", "outcome"],
        }
        self._check_required_fields(required, "marketing analysis")

    def _check_required_fields(self, required: dict, analysis_type: str):
        cols_lower = [c.lower() for c in self.df.columns]

        for field_type, keywords in required.items():
            found = any(any(kw in col for kw in keywords) for col in cols_lower)
            if not found:
                self._add_error(
                    severity="error",
                    field=field_type,
                    message=f"Missing {field_type} column for {analysis_type}",
                    suggestion=f"Add a column named one of: {', '.join(keywords)}",
                )
