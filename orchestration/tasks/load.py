from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger


@task(retries=2, retry_delay_seconds=10)
def load_to_staging(
    df: pd.DataFrame, table_name: str, connection_string: str, if_exists: str = "replace"
) -> dict:
    logger = get_run_logger()
    from sqlalchemy import create_engine

    staging_table = f"stg_{table_name}"

    engine = create_engine(connection_string)

    df["_loaded_at"] = datetime.utcnow()
    df["_source_rows"] = len(df)

    df.to_sql(
        staging_table, engine, if_exists=if_exists, index=False, method="multi", chunksize=1000
    )

    logger.info(f"Loaded {len(df)} rows to {staging_table}")

    return {
        "table": staging_table,
        "rows": len(df),
        "columns": len(df.columns),
        "loaded_at": datetime.utcnow().isoformat(),
    }


@task(retries=2, retry_delay_seconds=10)
def load_to_warehouse(
    df: pd.DataFrame,
    table_name: str,
    connection_string: str,
    if_exists: str = "append",
    schema: Optional[str] = None,
) -> dict:
    logger = get_run_logger()
    from sqlalchemy import create_engine

    engine = create_engine(connection_string)

    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000,
    )

    logger.info(
        f"Loaded {len(df)} rows to {schema}.{table_name}"
        if schema
        else f"Loaded {len(df)} rows to {table_name}"
    )

    return {
        "table": table_name,
        "schema": schema,
        "rows": len(df),
        "loaded_at": datetime.utcnow().isoformat(),
    }


@task
def load_to_parquet(
    df: pd.DataFrame, file_path: str, partition_cols: Optional[list[str]] = None
) -> dict:
    logger = get_run_logger()

    df.to_parquet(file_path, partition_cols=partition_cols, index=False)

    logger.info(f"Saved {len(df)} rows to {file_path}")

    return {
        "path": file_path,
        "rows": len(df),
        "partitions": partition_cols,
    }


@task
def upsert_to_table(
    df: pd.DataFrame, table_name: str, connection_string: str, key_columns: list[str]
) -> dict:
    logger = get_run_logger()
    from sqlalchemy import create_engine, text

    engine = create_engine(connection_string)

    temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    key_match = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])
    non_key_cols = [col for col in df.columns if col not in key_columns]
    update_set = ", ".join([f"{col} = s.{col}" for col in non_key_cols])
    all_cols = ", ".join(df.columns)
    source_cols = ", ".join([f"s.{col}" for col in df.columns])

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {temp_table} s
    ON {key_match}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({source_cols})
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(merge_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
            conn.commit()
    except Exception:
        logger.warning("MERGE not supported, falling back to delete-insert")
        with engine.connect() as conn:
            for _, row in df.iterrows():
                key_filter = " AND ".join([f"{col} = '{row[col]}'" for col in key_columns])
                conn.execute(text(f"DELETE FROM {table_name} WHERE {key_filter}"))
            conn.commit()
        df.to_sql(table_name, engine, if_exists="append", index=False)

    logger.info(f"Upserted {len(df)} rows to {table_name}")

    return {
        "table": table_name,
        "rows": len(df),
        "key_columns": key_columns,
    }
