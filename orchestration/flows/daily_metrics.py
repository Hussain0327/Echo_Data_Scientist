import os
from datetime import datetime, timezone
from typing import Optional

from prefect import flow, get_run_logger

from orchestration.tasks.extract import extract_csv
from orchestration.tasks.load import load_to_staging
from orchestration.tasks.transform import calculate_metrics, run_dbt
from orchestration.tasks.validate import run_expectations

REVENUE_EXPECTATIONS = [
    {"expectation_type": "expect_column_to_exist", "column": "amount"},
    {"expectation_type": "expect_column_to_exist", "column": "date"},
    {"expectation_type": "expect_column_values_to_not_be_null", "column": "amount"},
    {"expectation_type": "expect_column_values_to_be_between", "column": "amount", "min_value": 0},
]

MARKETING_EXPECTATIONS = [
    {"expectation_type": "expect_column_to_exist", "column": "leads"},
    {"expectation_type": "expect_column_to_exist", "column": "conversions"},
    {"expectation_type": "expect_column_values_to_not_be_null", "column": "source"},
    {"expectation_type": "expect_column_values_to_be_between", "column": "leads", "min_value": 0},
]


@flow(name="daily_metrics_pipeline", log_prints=True)
def daily_metrics_pipeline(
    revenue_file: Optional[str] = None,
    marketing_file: Optional[str] = None,
    run_dbt_models: bool = True,
    connection_string: Optional[str] = None,
):
    logger = get_run_logger()
    logger.info(f"Starting daily metrics pipeline at {datetime.now(timezone.utc).isoformat()}")

    conn_str = connection_string or os.getenv("DATABASE_URL")
    results = {"revenue": None, "marketing": None, "dbt": None}

    if revenue_file:
        results["revenue"] = process_revenue_data(revenue_file, conn_str)

    if marketing_file:
        results["marketing"] = process_marketing_data(marketing_file, conn_str)

    if run_dbt_models:
        results["dbt"] = run_dbt_transforms()

    logger.info("Daily metrics pipeline completed")
    return results


@flow(name="process_revenue_data")
def process_revenue_data(file_path: str, connection_string: Optional[str] = None):
    logger = get_run_logger()

    df = extract_csv(file_path)

    validation = run_expectations(df, REVENUE_EXPECTATIONS)
    if not validation["success"]:
        logger.warning(f"Validation failed: {validation['failed_count']} expectations failed")

    metrics = calculate_metrics(
        df,
        metrics=[
            "total_revenue",
            "revenue_growth",
            "average_order_value",
        ],
    )

    if connection_string:
        load_to_staging(df, "revenue", connection_string)

    return {
        "rows_processed": len(df),
        "validation": validation,
        "metrics": metrics,
    }


@flow(name="process_marketing_data")
def process_marketing_data(file_path: str, connection_string: Optional[str] = None):
    logger = get_run_logger()

    df = extract_csv(file_path)

    validation = run_expectations(df, MARKETING_EXPECTATIONS)
    if not validation["success"]:
        logger.warning(f"Validation failed: {validation['failed_count']} expectations failed")

    metrics = calculate_metrics(
        df,
        metrics=[
            "conversion_rate",
            "channel_performance",
        ],
    )

    if connection_string:
        load_to_staging(df, "marketing", connection_string)

    return {
        "rows_processed": len(df),
        "validation": validation,
        "metrics": metrics,
    }


@flow(name="run_dbt_transforms")
def run_dbt_transforms():
    logger = get_run_logger()

    staging_result = run_dbt(command="run", select="staging")
    logger.info("Staging models completed")

    marts_result = run_dbt(command="run", select="marts")
    logger.info("Mart models completed")

    test_result = run_dbt(command="test")
    logger.info("dbt tests completed")

    return {
        "staging": staging_result,
        "marts": marts_result,
        "tests": test_result,
    }


if __name__ == "__main__":
    daily_metrics_pipeline(
        revenue_file="data/samples/revenue_sample.csv",
        marketing_file="data/samples/marketing_sample.csv",
        run_dbt_models=False,
    )
