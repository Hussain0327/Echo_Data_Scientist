import subprocess
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


@task(retries=1)
def run_dbt(
    command: str = "run",
    select: Optional[str] = None,
    target: str = "dev",
    project_dir: Optional[str] = None,
) -> dict:
    logger = get_run_logger()

    if project_dir is None:
        project_dir = str(Path(__file__).parent.parent.parent / "dbt")

    cmd = ["dbt", command, "--target", target, "--project-dir", project_dir]

    if select:
        cmd.extend(["--select", select])

    logger.info(f"Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=project_dir)

    if result.returncode != 0:
        logger.error(f"dbt {command} failed: {result.stderr}")
        raise RuntimeError(f"dbt {command} failed: {result.stderr}")

    logger.info(f"dbt {command} completed successfully")

    return {
        "command": command,
        "select": select,
        "returncode": result.returncode,
        "stdout": result.stdout,
    }


@task
def run_dbt_test(select: Optional[str] = None, project_dir: Optional[str] = None) -> dict:
    return run_dbt.fn(command="test", select=select, project_dir=project_dir)


@task
def run_dbt_build(select: Optional[str] = None, project_dir: Optional[str] = None) -> dict:
    return run_dbt.fn(command="build", select=select, project_dir=project_dir)


@task
def calculate_metrics(df: pd.DataFrame, metrics: Optional[list[str]] = None) -> dict:
    logger = get_run_logger()

    from app.services.data_autofixer import auto_fix_dataframe
    from app.services.metrics.registry import create_metrics_engine

    fix_result = auto_fix_dataframe(df)
    df = fix_result.df

    if fix_result.was_modified:
        logger.info(f"Applied {fix_result.total_fixes} data fixes")

    engine = create_metrics_engine(df)

    if metrics:
        results = {}
        for metric_name in metrics:
            try:
                result = engine.calculate(metric_name)
                results[metric_name] = result.model_dump()
            except Exception as e:
                logger.warning(f"Failed to calculate {metric_name}: {e}")
    else:
        calculated = engine.calculate_all()
        results = {r.metric_name: r.model_dump() for r in calculated}

    logger.info(f"Calculated {len(results)} metrics")

    return {
        "metrics": results,
        "count": len(results),
        "data_fixes_applied": fix_result.total_fixes,
    }


@task
def apply_transformations(df: pd.DataFrame, transformations: list[dict]) -> pd.DataFrame:
    logger = get_run_logger()

    for transform in transformations:
        transform_type = transform.get("type")
        column = transform.get("column")
        params = transform.get("params", {})

        if transform_type == "rename":
            new_name = params.get("new_name")
            df = df.rename(columns={column: new_name})

        elif transform_type == "to_datetime":
            df[column] = pd.to_datetime(df[column], errors="coerce")

        elif transform_type == "to_numeric":
            df[column] = pd.to_numeric(df[column], errors="coerce")

        elif transform_type == "fill_na":
            value = params.get("value", 0)
            df[column] = df[column].fillna(value)

        elif transform_type == "drop_duplicates":
            subset = params.get("subset")
            df = df.drop_duplicates(subset=subset)

        elif transform_type == "filter":
            condition = params.get("condition")
            df = df.query(condition)

        logger.info(f"Applied {transform_type} on {column}")

    return df
