from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


@task(retries=2, retry_delay_seconds=30)
def extract_csv(file_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    logger.info(f"Extracting {path.name}")
    df = pd.read_csv(path)
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")

    return df


@task(retries=2, retry_delay_seconds=30)
def extract_excel(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    logger = get_run_logger()
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    logger.info(f"Extracting {path.name}")
    df = pd.read_excel(path, sheet_name=sheet_name)
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")

    return df


@task
def extract_from_directory(directory: str, pattern: str = "*.csv") -> dict[str, pd.DataFrame]:
    logger = get_run_logger()
    dir_path = Path(directory)

    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    files = list(dir_path.glob(pattern))
    logger.info(f"Found {len(files)} files matching {pattern}")

    dataframes = {}
    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            dataframes[file_path.stem] = df
            logger.info(f"Loaded {file_path.name}: {len(df)} rows")
        except Exception as e:
            logger.warning(f"Failed to load {file_path.name}: {e}")

    return dataframes


@task
def extract_from_database(query: str, connection_string: str) -> pd.DataFrame:
    logger = get_run_logger()
    from sqlalchemy import create_engine

    engine = create_engine(connection_string)
    logger.info("Executing query against database")

    df = pd.read_sql(query, engine)
    logger.info(f"Extracted {len(df)} rows")

    return df
