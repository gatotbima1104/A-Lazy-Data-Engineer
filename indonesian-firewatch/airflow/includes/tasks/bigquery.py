from __future__ import annotations

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

from includes.constant import GCP_CONN_ID
from includes.notify import on_failure_callback
from utils.constant import (
    BQ_DATASET_RAW,
    BUCKET_NAME,
    PROJECT_ID,
)


def _create_load_task(
    *,
    task_id: str,
    source_uri: str,
    table_name: str,
    source_format: str,
    **load_options
):
    """ [TASK] Load GCS to BiqQuery """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "load": {
                "sourceUris": [source_uri],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_DATASET_RAW,
                    "tableId": table_name
                },
                "sourceFormat": source_format,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                **load_options
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        on_failure_callback=on_failure_callback,
    )

def load_fire_detection(source_object: str):
    source_uri = f"gs://{BUCKET_NAME}/{source_object}"
    
    return _create_load_task(
        task_id="load_fire_detection",
        source_uri=source_uri,
        table_name="raw_detections",
        source_format="PARQUET",
    )

def load_regencies(
    source_object: str,
):
    source_uri = f"gs://{BUCKET_NAME}/{source_object}"

    return _create_load_task(
        task_id="load_regencies",
        source_uri=source_uri,
        table_name="raw_regencies",
        source_format="PARQUET",
    )

def load_provinces(
    source_object: str,
):
    source_uri = f"gs://{BUCKET_NAME}/{source_object}"

    return _create_load_task(
        task_id="load_provinces",
        source_uri=source_uri,
        table_name="raw_provinces",
        source_format="PARQUET",
    )

def load_dataframe_to_bq(
    data: pd.DataFrame,
    table_name: str
) -> None:
    """
    Load a Pandas DataFrame directly to BigQuery 
    """
    
    if data.empty:
        print("No data to load.")
        return

    if "acq_date" not in data.columns:
        raise ValueError("DataFrame must contain 'acq_date'.")
    
    data = data.copy()
    
    # Normalize type data from raw -> bq
    data["acq_date"] = pd.to_datetime(
        data["acq_date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    data["acq_time"] = data["acq_time"].astype("string")
    
    data["version"] = pd.to_numeric(
        data["version"],
        errors="coerce",
    ).astype("Int64")

    if "type" in data.columns:
        data["type"] = pd.to_numeric(
            data["type"],
            errors="coerce",
        ).astype("Int64")
        
    if "regency_id" in data.columns:
        data["regency_id"] = pd.to_numeric(
            data["regency_id"],
            errors="coerce",
        ).astype("Int64")
    
    dates = data["acq_date"].dropna().unique()

    if len(dates) != 1:
        raise ValueError(
            f"Expected exactly one acq date, "
            f"but found: {dates}"
        )

    acq_date = pd.Timestamp(dates[0]).date()
    
    hook = BigQueryHook(gcp_conn_id = GCP_CONN_ID)
    client = hook.get_client(project_id = PROJECT_ID)
    
    table_id = (
        f"{PROJECT_ID}."
        f"{BQ_DATASET_RAW}."
        f"{table_name}"
    )
    
    # 1. Delete existing data for this date
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE SAFE_CAST(acq_date AS DATE) = DATE('{acq_date}')
    """

    client.query(delete_query).result()

    print(f"Deleted existing RAW data for {acq_date}")
    
     # 2. Append new data
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        
    )
    
    job = client.load_table_from_dataframe(
        data,
        table_id,
        job_config
    )
    
    job.result()

    print(f"Loaded {len(data):,} rows into {table_id}")