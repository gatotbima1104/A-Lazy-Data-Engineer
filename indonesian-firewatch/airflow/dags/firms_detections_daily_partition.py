from __future__ import annotations

import io
import logging
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sdk import dag, task
from includes.constant import GCP_CONN_ID, LANDING_PREFIX, RAW_PREFIX
from includes.notify import on_failure_callback, on_success_callback

from utils.constant import BUCKET_NAME

logger = logging.getLogger(__name__)


@dag(
    dag_id="firms_detections_daily_partition",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["firms", "batch"],
    on_success_callback=on_success_callback
)
def firms_detections_daily_partition():

    @task
    def list_landing_zips() -> list[str]:
        hook = GCSHook(GCP_CONN_ID)
        objects = hook.list(bucket_name=BUCKET_NAME, prefix=LANDING_PREFIX)
        zip_objects = [
            obj for obj in objects
                if obj.lower().endswith(".zip")
        ]

        if not zip_objects:
            raise AirflowSkipException(
                f"No zip files found under gs://{BUCKET_NAME}/{LANDING_PREFIX}"
            )

        logger.info("Found %d zip file(s): %s", len(zip_objects), zip_objects)
        return zip_objects

    @task(
        on_failure_callback=on_failure_callback
    )
    def transform_zip_to_daily_pq(source: str):
        hook = GCSHook(GCP_CONN_ID)
        zip_bytes = hook.download(
            bucket_name=BUCKET_NAME,
            object_name=source
        )
        
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            csv_names = [
                name
                for name in archive.namelist()
                if name.lower().endswith(".csv")
            ]
            
            if not csv_names:
                raise ValueError(f"No CSV found inside {source}")
            
            for csv_name in csv_names:
                logger.info("Processing CSV %s from %s", csv_name, source)
            
                with archive.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file, dtype={"acq_time": str})
                
                if "acq_date" not in df.columns:
                        raise ValueError(f"'acq_date' column missing in {csv_name}")
        
                base_name = os.path.splitext(os.path.basename(csv_name))[0]
        
                for acq_date, day_df in df.groupby("acq_date"):
                    
                    date = pd.to_datetime(acq_date)
                    
                    date_str = date.strftime("%Y-%m-%d")
                    year = date.strftime("%Y")
                    month = date.strftime("%m")
                    
                    pq_buffer = io.BytesIO()
                    
                    day_df.to_parquet(
                        pq_buffer,
                        index=False,
                        engine="pyarrow"
                    )
                    
                    pq_buffer.seek(0)

                    destination = (
                        f"{RAW_PREFIX}"
                        f"{year}/"
                        f"{month}/"
                        f"{base_name}_{date_str}.parquet"
                    )
                    
                    hook.upload(
                        bucket_name=BUCKET_NAME,
                        object_name=destination,
                        data=pq_buffer.getvalue(),
                        mime_type="application/octet-stream",
                    )
                    
                    logger.info("Wrote %d rows -> gs://%s/%s", len(day_df), BUCKET_NAME, destination)
            
    zip_objects = list_landing_zips()
    transform_zip_to_daily_pq.expand(source=zip_objects)


firms_detections_daily_partition()
