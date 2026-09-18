from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from includes.constant import GCP_CONN_ID, PG_CONN_ID
from includes.tasks.notify import on_failure_callback

from dags.setup import SOURCE_TABLES
from utils.constant import BUCKET_NAME

logger = logging.getLogger(__name__)


@dag(
    dag_id="postgres_ingest_to_gsc",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["ingestion", "postgres", 'gcs']
)
def postgres_ingest_to_gsc():
    
    @task(on_failure_callback=on_failure_callback)
    def ingest_postgres_to_gcs() -> None:
        
        pg_hook = PostgresHook(
            postgres_conn_id=PG_CONN_ID
        )
        
        gcs_hook = GCSHook(
            gcp_conn_id=GCP_CONN_ID
        )
        
        for source_type, table_names in SOURCE_TABLES.items():
        
            for table_name in table_names:
                
                logger.info("Reading PostgreSQL table: %s", table_name)
                
                df = pg_hook.get_pandas_df(
                    f"""
                        SELECT *
                        FROM {table_name}
                    """
                )
        
                if df.empty:
                    logger.warning("No records found in PostgreSQL table %s", table_name)
                    continue

                # References logic
                if source_type == "reference":

                    pq_buffer = io.BytesIO()

                    df.to_parquet(
                        pq_buffer,
                        index=False,
                        engine="pyarrow",
                    )

                    pq_buffer.seek(0)

                    destination = (
                        f"reference/"
                        f"{table_name}.parquet"
                    )

                    gcs_hook.upload(
                        bucket_name=BUCKET_NAME,
                        object_name=destination,
                        data=pq_buffer.getvalue(),
                        mime_type="application/octet-stream",
                    )

                    logger.info("Wrote %d rows -> gs://%s/%s", len(df), BUCKET_NAME, destination)
                    continue

                # Archive/nrt logic
                # Partition prepare file spare
                df["acq_date"] = pd.to_datetime(
                    df["acq_date"],
                    errors="coerce",
                )

                for acq_date, day_df in df.groupby(
                    df["acq_date"].dt.date
                ):

                    date_str = acq_date.strftime("%Y-%m-%d")
                    year = acq_date.strftime("%Y")
                    month = acq_date.strftime("%m")

                    pq_buffer = io.BytesIO()

                    day_df.to_parquet(
                        pq_buffer,
                        index=False,
                        engine="pyarrow",
                    )

                    pq_buffer.seek(0)

                    destination = (
                        f"{source_type}/"
                        f"{year}/"
                        f"{month}/"
                        f"{table_name}_{date_str}.parquet"
                    )

                    gcs_hook.upload(
                        bucket_name=BUCKET_NAME,
                        object_name=destination,
                        data=pq_buffer.getvalue(),
                        mime_type="application/octet-stream",
                    )
    
    ingest_postgres_to_gcs()
        
postgres_ingest_to_gsc()
