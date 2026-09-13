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
    source_uri = (f"gs://{BUCKET_NAME}/{source_object}")

    return _create_load_task(
        task_id="load_regencies",
        source_uri=source_uri,
        table_name="raw_regencies",
        source_format="CSV",
        skipLeadingRows=1,
        autodetect=True,
    )


def load_provinces(
    source_object: str,
):
    source_uri = (f"gs://{BUCKET_NAME}/{source_object}")

    return _create_load_task(
        task_id="load_provinces",
        source_uri=source_uri,
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
    hook = BigQueryHook(gcp_conn_id = GCP_CONN_ID)
    client = hook.get_client(project_id = PROJECT_ID)
    
    table_id = (
        f"{PROJECT_ID}."
        f"{BQ_DATASET_RAW}."
        f"{table_name}"
    )
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(data):,} rows into {table_id}")