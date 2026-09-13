from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import get_current_context, task

from includes.constant import GCP_CONN_ID, RAW_PREFIX
from includes.notify import on_failure_callback
from utils.constant import BUCKET_NAME


@task(
    on_failure_callback=on_failure_callback,
)
def check_fire_detection_source() -> str:
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

    prefix = f"{RAW_PREFIX}{year}/{month}/"

    hook = GCSHook(
        gcp_conn_id=GCP_CONN_ID,
    )

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

    if not source_objects:
        raise FileNotFoundError(f"No Fire Detection file found for {fire_detection_date} under gs://{BUCKET_NAME}/{prefix}")

    if len(source_objects) > 1:
        raise ValueError(f"Multiple Fire Detection files found for {fire_detection_date}: {source_objects}")

    return source_objects[0]