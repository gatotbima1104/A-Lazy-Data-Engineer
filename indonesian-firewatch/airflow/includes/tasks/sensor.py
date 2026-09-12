from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context, task
from utils.constant import BUCKET_NAME

from includes.constant import GCP_CONN_ID, RAW_PREFIX
from includes.notify import on_failure_callback


@task(
    on_failure_callback=on_failure_callback,
)
def check_fire_detection_source() -> str:
    """[TASK] Check source file from GCS. """
    context = get_current_context()
    date = context["params"]["date"]
    
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    
    prefix = f"{RAW_PREFIX}{year}/{month}/"
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    objects = hook.list(bucket_name=BUCKET_NAME, prefix=prefix)
    
    source_objects = [
        obj
            for obj in objects
                if obj.lower().endswith(".parquet")
                    and date in obj
    ]

    if not source_objects:
        raise FileNotFoundError(
            f"No Fire Detection file found for {date} "
            f"under gs://{BUCKET_NAME}/{prefix}"
        )

    if len(source_objects) > 1:
        raise ValueError(f"Multiple Fire Detection files found for {date}: {source_objects}")

    return source_objects[0]

