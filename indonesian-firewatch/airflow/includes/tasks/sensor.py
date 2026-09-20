from datetime import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context, task

from includes.constant import GCP_CONN_ID, PREFIX
from includes.tasks.notify import on_failure_callback
from includes.tasks.setup import BQ_TABLE_STREAMING_RAW
from utils.constant import BQ_DATASET_STREAMING, BUCKET_NAME, PROJECT_ID


def branch_source(
    **context
) -> list[str]:
    """
    Select batch and/or stream enrichment tasks based on available sources.
    """
    source = context["ti"].xcom_pull(task_ids="check_fire_detection_source")
    
    tasks = []
    if source["batch_exists"]:
        tasks.append("enrich_and_load_location")
        
    if source["stream_exists"]:
        tasks.append("enrich_stream_location")

    return tasks

def check_stream_data(
    fire_detection_date: str
) -> bool:
    """
    Check data stream available or not
    """
    client = BigQueryHook(gcp_conn_id=GCP_CONN_ID).get_client()

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{PROJECT_ID}.{BQ_DATASET_STREAMING}.{BQ_TABLE_STREAMING_RAW}`
        WHERE acquisition_date = DATE('{fire_detection_date}')
    """

    rows = client.query(query).result()
    row = next(iter(rows))

    return row.row_count > 0

@task
def get_source_object(
    source: dict
) -> str | None:
    """
    [TASK] Get key of dict
    """
    
    return source["batch_source"]

@task(on_failure_callback=on_failure_callback)
def check_fire_detection_source() -> dict:
    """[TASK] Check source file from GCS."""

    context = get_current_context()

    fire_detection_date = (
        context["params"]["date"]
        or context["logical_date"].strftime("%Y-%m-%d")
    )

    date_obj = datetime.strptime(
        fire_detection_date,
        "%Y-%m-%d",
    )

    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")

    hook = GCSHook(
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # 1. Check batch file
    prefix = f'{PREFIX["archive"]}{year}/{month}/'

    objects = hook.list(
        bucket_name=BUCKET_NAME,
        prefix=prefix,
    )

    source_objects = [
        obj
        for obj in objects
        if obj.lower().endswith(".parquet")
        and fire_detection_date in obj
    ]

    if len(source_objects) > 1:
        raise ValueError(f"Multiple Fire Detection files found for {fire_detection_date}: {source_objects}")
    
    batch_exists = len(source_objects) == 1
    batch_source = source_objects[0] if batch_exists else None

    # 2. Stream check
    stream_exists = check_stream_data(
        fire_detection_date=fire_detection_date
    )

    if not batch_exists and not stream_exists:
        raise FileNotFoundError(f"No batch file or stream data found for {fire_detection_date}")

    return {
        "date": fire_detection_date,
        "batch_exists": batch_exists,
        "batch_source": batch_source,
        "stream_exists": stream_exists,
    }