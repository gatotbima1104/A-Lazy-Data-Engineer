from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import task

from includes.constant import GCP_CONN_ID
from utils.constant import BUCKET_NAME


@task
def upload_reference_to_gcs(
    source_file: str,
    destination: str,
) -> str:
    """
    Upload a local reference file to GCS
    """

    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    
    if hook.exists(
        bucket_name=BUCKET_NAME,
        object_name=destination,
    ):
        print(f"Reference already exists: gs://{BUCKET_NAME}/{destination}")
        return destination
    
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=destination,
        filename=source_file,
    )

    print(f"Uploaded {source_file} to gs://{BUCKET_NAME}/{destination}")

    return destination