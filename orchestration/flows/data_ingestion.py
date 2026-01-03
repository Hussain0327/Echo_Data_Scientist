import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger

from orchestration.tasks.extract import extract_csv, extract_excel
from orchestration.tasks.load import load_to_staging
from orchestration.tasks.transform import apply_transformations, run_dbt
from orchestration.tasks.validate import run_expectations

DATA_EXPECTATIONS = {
    "revenue": [
        {"expectation_type": "expect_column_to_exist", "column": "amount"},
        {"expectation_type": "expect_column_to_exist", "column": "date"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "amount"},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "amount",
            "min_value": 0,
            "max_value": 10000000,
        },
    ],
    "marketing": [
        {"expectation_type": "expect_column_to_exist", "column": "source"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "source"},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "leads",
            "min_value": 0,
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "conversions",
            "min_value": 0,
        },
    ],
    "customers": [
        {"expectation_type": "expect_column_to_exist", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_be_unique", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
    ],
    "experiments": [
        {"expectation_type": "expect_column_to_exist", "column": "variant"},
        {"expectation_type": "expect_column_to_exist", "column": "user_id"},
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": "variant",
            "value_set": ["control", "variant_a", "variant_b", "treatment"],
        },
    ],
}


@flow(name="data_ingestion_pipeline", log_prints=True)
def data_ingestion_pipeline(
    file_path: str,
    data_type: str,
    connection_string: Optional[str] = None,
    run_dbt_after: bool = True,
    fail_on_validation_error: bool = False,
):
    logger = get_run_logger()
    logger.info(f"Starting ingestion pipeline for {data_type} data")

    conn_str = connection_string or os.getenv("DATABASE_URL")
    path = Path(file_path)

    if path.suffix.lower() == ".csv":
        raw_df = extract_csv(file_path)
    elif path.suffix.lower() in [".xlsx", ".xls"]:
        raw_df = extract_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    expectations = DATA_EXPECTATIONS.get(data_type, [])
    if expectations:
        validation = run_expectations(raw_df, expectations)
        logger.info(f"Validation: {validation['passed_count']}/{len(expectations)} passed")

        if not validation["success"] and fail_on_validation_error:
            raise ValueError(f"Data validation failed for {data_type}")

    clean_df = apply_transformations(
        raw_df,
        [
            {"type": "drop_duplicates", "column": None, "params": {}},
        ],
    )

    staging_result = None
    if conn_str:
        staging_result = load_to_staging(clean_df, data_type, conn_str)

    dbt_result = None
    if run_dbt_after and conn_str:
        dbt_result = run_dbt(command="run", select=f"staging.stg_{data_type}")

    return {
        "file": file_path,
        "data_type": data_type,
        "rows_ingested": len(clean_df),
        "validation": validation if expectations else None,
        "staging": staging_result,
        "dbt": dbt_result,
        "ingested_at": datetime.utcnow().isoformat(),
    }


@flow(name="batch_ingestion_pipeline", log_prints=True)
def batch_ingestion_pipeline(
    source_directory: str,
    data_type: str,
    file_pattern: str = "*.csv",
    connection_string: Optional[str] = None,
):
    logger = get_run_logger()
    source_path = Path(source_directory)

    files = list(source_path.glob(file_pattern))
    logger.info(f"Found {len(files)} files to process")

    results = []
    for file_path in files:
        try:
            result = data_ingestion_pipeline(
                file_path=str(file_path),
                data_type=data_type,
                connection_string=connection_string,
                run_dbt_after=False,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            results.append({"file": str(file_path), "error": str(e)})

    if connection_string:
        run_dbt(command="run", select="staging")
        run_dbt(command="run", select="marts")

    successful = sum(1 for r in results if "error" not in r)
    logger.info(f"Processed {successful}/{len(files)} files successfully")

    return {
        "total_files": len(files),
        "successful": successful,
        "failed": len(files) - successful,
        "results": results,
    }


if __name__ == "__main__":
    data_ingestion_pipeline(
        file_path="data/samples/revenue_sample.csv",
        data_type="revenue",
        run_dbt_after=False,
    )
