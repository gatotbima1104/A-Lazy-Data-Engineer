from __future__ import annotations

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import TriggerRule
from google.api_core.exceptions import NotFound
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

def _check_table_exist(
    *,
    table_name: str,
    load_task_id: str,
    skip_task_id: str,
) -> str:
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_client(project_id=PROJECT_ID)

    table_id = (
        f"{PROJECT_ID}."
        f"{BQ_DATASET_RAW}."
        f"{table_name}"
    )

    try:
        client.get_table(table_id)

        print(f"Table {table_id} already exists. Skipping load.")
        return skip_task_id

    except NotFound:
        print(f"Table {table_id} does not exist. Loading reference data.")
        return load_task_id

def _create_reference_load_tasks(
    *,
    prefix: str,
    source_object: str,
    table_name: str,
    source_format: str,
    **load_options,
):
    source_uri = f"gs://{BUCKET_NAME}/{source_object}"
    check_task_id = f"check_{prefix}_exists"
    load_task_id = f"load_{prefix}"
    skip_task_id = f"skip_{prefix}_load"
    ready_task_id = f"{prefix}_ready"

    check = BranchPythonOperator(
        task_id=check_task_id,
        python_callable=_check_table_exist,
        op_kwargs={
            "table_name": table_name,
            "load_task_id": load_task_id,
            "skip_task_id": skip_task_id,
        }
    )
    
    load = _create_load_task(
        task_id=load_task_id,
        source_uri=source_uri,
        table_name=table_name,
        source_format=source_format,
        **load_options,
    )
    
    skip = EmptyOperator(task_id=skip_task_id)
    ready = EmptyOperator(
        task_id=ready_task_id,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    check >> [load, skip]
    [load, skip] >> ready
    
    return ready

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
    return _create_reference_load_tasks(
        prefix="regencies",
        source_object=source_object,
        table_name="raw_regencies",
        source_format="CSV",
        skipLeadingRows=1,
        autodetect=True,
    )

def load_provinces(
    source_object: str,
):
   return _create_reference_load_tasks(
        prefix="provinces",
        source_object=source_object,
        table_name="raw_provinces",
        source_format="CSV",
        skipLeadingRows=1,
        autodetect=True,
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