from typing import Optional

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


class DataValidationError(Exception):
    def __init__(self, message: str, failures: list):
        super().__init__(message)
        self.failures = failures


@task
def validate_data(df: pd.DataFrame, expectation_suite: str, raise_on_failure: bool = True) -> dict:
    logger = get_run_logger()

    try:
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest
    except ImportError:
        logger.warning("Great Expectations not installed, skipping validation")
        return {"success": True, "validated": False, "reason": "gx not installed"}

    context = gx.get_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="runtime_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    checkpoint_result = context.run_checkpoint(
        checkpoint_name=f"{expectation_suite}_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite,
            }
        ],
    )

    success = checkpoint_result.success
    logger.info(f"Validation {'passed' if success else 'failed'} for {expectation_suite}")

    if not success and raise_on_failure:
        failures = _extract_failures(checkpoint_result)
        raise DataValidationError(f"Data validation failed for {expectation_suite}", failures)

    return {
        "success": success,
        "validated": True,
        "suite": expectation_suite,
        "statistics": checkpoint_result.to_json_dict().get("statistics", {}),
    }


@task
def run_expectations(df: pd.DataFrame, expectations: list[dict]) -> dict:
    logger = get_run_logger()

    results = []
    all_passed = True

    for exp in expectations:
        exp_type = exp.get("expectation_type")
        column = exp.get("column")
        kwargs = {k: v for k, v in exp.items() if k not in ["expectation_type", "column"]}

        passed, details = _run_single_expectation(df, exp_type, column, kwargs)
        results.append(
            {
                "expectation": exp_type,
                "column": column,
                "passed": passed,
                "details": details,
            }
        )

        if not passed:
            all_passed = False
            logger.warning(f"Failed: {exp_type} on {column}")

    logger.info(f"Ran {len(expectations)} expectations, {sum(r['passed'] for r in results)} passed")

    return {
        "success": all_passed,
        "results": results,
        "passed_count": sum(r["passed"] for r in results),
        "failed_count": sum(not r["passed"] for r in results),
    }


def _run_single_expectation(
    df: pd.DataFrame, exp_type: str, column: Optional[str], kwargs: dict
) -> tuple[bool, dict]:
    try:
        if exp_type == "expect_column_to_exist":
            passed = column in df.columns
            return passed, {"column_exists": passed}

        if exp_type == "expect_column_values_to_not_be_null":
            null_count = df[column].isnull().sum()
            passed = null_count == 0
            return passed, {"null_count": int(null_count)}

        if exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            series = df[column].dropna()

            violations = 0
            if min_val is not None:
                violations += (series < min_val).sum()
            if max_val is not None:
                violations += (series > max_val).sum()

            passed = violations == 0
            return passed, {"violations": int(violations)}

        if exp_type == "expect_column_values_to_be_unique":
            duplicates = df[column].duplicated().sum()
            passed = duplicates == 0
            return passed, {"duplicate_count": int(duplicates)}

        if exp_type == "expect_column_values_to_be_in_set":
            valid_set = set(kwargs.get("value_set", []))
            invalid = ~df[column].isin(valid_set)
            invalid_count = invalid.sum()
            passed = invalid_count == 0
            return passed, {"invalid_count": int(invalid_count)}

        if exp_type == "expect_column_values_to_match_regex":
            pattern = kwargs.get("regex", ".*")
            matches = df[column].astype(str).str.match(pattern)
            non_matches = (~matches).sum()
            passed = non_matches == 0
            return passed, {"non_matching_count": int(non_matches)}

        return True, {"note": f"Unknown expectation type: {exp_type}"}

    except Exception as e:
        return False, {"error": str(e)}


def _extract_failures(checkpoint_result) -> list:
    failures = []
    try:
        results = checkpoint_result.to_json_dict().get("run_results", {})
        for run_id, run_result in results.items():
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
                            "observed": result.get("result", {}),
                        }
                    )
    except Exception:
        pass
    return failures
