from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class ValidationResult:
    success: bool
    suite_name: str
    total_expectations: int
    successful_expectations: int
    failed_expectations: int
    failures: list = field(default_factory=list)
    statistics: dict = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        if self.total_expectations == 0:
            return 0.0
        return self.successful_expectations / self.total_expectations


class DataValidator:
    def __init__(self, context_root: Optional[str] = None):
        if context_root is None:
            context_root = str(Path(__file__).parent)
        self.context_root = context_root
        self._context = None

    @property
    def context(self):
        if self._context is None:
            try:
                import great_expectations as gx

                self._context = gx.get_context(context_root_dir=self.context_root)
            except Exception:
                self._context = None
        return self._context

    def validate(
        self, df: pd.DataFrame, suite_name: str, raise_on_failure: bool = False
    ) -> ValidationResult:
        if self.context is None:
            return self._validate_without_gx(df, suite_name)

        return self._validate_with_gx(df, suite_name, raise_on_failure)

    def _validate_with_gx(
        self, df: pd.DataFrame, suite_name: str, raise_on_failure: bool
    ) -> ValidationResult:
        from great_expectations.core.batch import RuntimeBatchRequest

        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="validation_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default"},
        )

        checkpoint_name = f"{suite_name}_checkpoint"

        try:
            checkpoint_result = self.context.run_checkpoint(
                checkpoint_name=checkpoint_name,
                validations=[
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": suite_name,
                    }
                ],
            )
        except Exception as e:
            return ValidationResult(
                success=False,
                suite_name=suite_name,
                total_expectations=0,
                successful_expectations=0,
                failed_expectations=0,
                failures=[{"error": str(e)}],
            )

        failures = []
        stats = checkpoint_result.to_json_dict().get("statistics", {})

        for run_id, run_result in checkpoint_result.to_json_dict().get("run_results", {}).items():
            validation = run_result.get("validation_result", {})
            for result in validation.get("results", []):
                if not result.get("success", True):
                    failures.append(
                        {
                            "expectation": result.get("expectation_config", {}).get(
                                "expectation_type"
                            ),
                            "column": result.get("expectation_config", {})
                            .get("kwargs", {})
                            .get("column"),
                            "result": result.get("result", {}),
                        }
                    )

        result = ValidationResult(
            success=checkpoint_result.success,
            suite_name=suite_name,
            total_expectations=stats.get("evaluated_expectations", 0),
            successful_expectations=stats.get("successful_expectations", 0),
            failed_expectations=stats.get("unsuccessful_expectations", 0),
            failures=failures,
            statistics=stats,
        )

        if raise_on_failure and not result.success:
            raise DataValidationError(f"Validation failed for {suite_name}", failures)

        return result

    def _validate_without_gx(self, df: pd.DataFrame, suite_name: str) -> ValidationResult:
        from data_quality.expectations import get_expectation_suite

        suite = get_expectation_suite(suite_name)
        if suite is None:
            return ValidationResult(
                success=False,
                suite_name=suite_name,
                total_expectations=0,
                successful_expectations=0,
                failed_expectations=0,
                failures=[{"error": f"Suite '{suite_name}' not found"}],
            )

        expectations = suite.get("expectations", [])
        passed = 0
        failures = []

        for exp in expectations:
            exp_type = exp.get("expectation_type")
            kwargs = exp.get("kwargs", {})

            success = self._check_expectation(df, exp_type, kwargs)
            if success:
                passed += 1
            else:
                failures.append(
                    {
                        "expectation": exp_type,
                        "column": kwargs.get("column"),
                        "kwargs": kwargs,
                    }
                )

        return ValidationResult(
            success=len(failures) == 0,
            suite_name=suite_name,
            total_expectations=len(expectations),
            successful_expectations=passed,
            failed_expectations=len(failures),
            failures=failures,
        )

    def _check_expectation(self, df: pd.DataFrame, exp_type: str, kwargs: dict) -> bool:
        try:
            column = kwargs.get("column")

            if exp_type == "expect_column_to_exist":
                return column in df.columns

            if exp_type == "expect_column_values_to_not_be_null":
                return df[column].isnull().sum() == 0

            if exp_type == "expect_column_values_to_be_between":
                series = df[column].dropna()
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                if min_val is not None and (series < min_val).any():
                    return False
                if max_val is not None and (series > max_val).any():
                    return False
                return True

            if exp_type == "expect_column_values_to_be_unique":
                return df[column].duplicated().sum() == 0

            if exp_type == "expect_column_values_to_be_in_set":
                value_set = set(kwargs.get("value_set", []))
                return df[column].isin(value_set).all()

            if exp_type == "expect_table_row_count_to_be_between":
                min_val = kwargs.get("min_value", 0)
                max_val = kwargs.get("max_value", float("inf"))
                return min_val <= len(df) <= max_val

            if exp_type == "expect_column_pair_values_A_to_be_greater_than_B":
                col_a = kwargs.get("column_A")
                col_b = kwargs.get("column_B")
                or_equal = kwargs.get("or_equal", False)
                if or_equal:
                    return (df[col_a] >= df[col_b]).all()
                return (df[col_a] > df[col_b]).all()

            return True

        except Exception:
            return False

    def list_available_suites(self) -> list[str]:
        from data_quality.expectations import list_suites

        return list_suites()


class DataValidationError(Exception):
    def __init__(self, message: str, failures: list):
        super().__init__(message)
        self.failures = failures
