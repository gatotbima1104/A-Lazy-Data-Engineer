import io

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from utils.constant import BUCKET_NAME


def upload_pq_gcs(
    df: pd.DataFrame,
    object_name: str,
    gcs_hook: GCSHook,
    bucket_name: str = BUCKET_NAME
) -> None:
    """
    Helper to upload parquet -> gcs
    """
    
    pq_buffer = io.BytesIO()
    df.to_parquet(
        pq_buffer,
        index=False,
        engine="pyarrow",
    )

    pq_buffer.seek(0)
    
    gcs_hook.upload(
        bucket_name,
        object_name,
        data=pq_buffer.getvalue(),
        mime_type="application/octet-stream",
    )
