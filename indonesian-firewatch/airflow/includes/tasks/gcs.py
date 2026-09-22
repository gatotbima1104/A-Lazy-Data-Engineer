from __future__ import annotations

import io
import logging

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.constant import BUCKET_NAME

logger = logging.getLogger(__name__)

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

def ingest_pg_to_gcs(
    pg_hook: PostgresHook,
    gcs_hook: GCSHook,
    source_type: str,
    table_name: str,
) -> None:
    """
    Ingest PostgreSQL table into GCS as Parquet.
    """

    logger.info("Reading PostgreSQL table: %s", table_name)

    df = pg_hook.get_pandas_df(
        f"""
            SELECT *
            FROM {table_name}
        """
    )

    if df.empty:
        logger.warning("No records found in PostgreSQL table %s", table_name)
        return

    if source_type == "reference":
        destination = f"reference/{table_name}.parquet"

        upload_pq_gcs(
            df,
            object_name=destination,
            gcs_hook=gcs_hook,
        )

        logger.info("Wrote %d rows -> gs://%s/%s", len(df), BUCKET_NAME, destination)
        return

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

        destination = (
            f"{source_type}/{year}/"
            f"{month}/{table_name}_{date_str}.parquet"
        )

        upload_pq_gcs(
            day_df,
            object_name=destination,
            gcs_hook=gcs_hook,
        )

        logger.info("Wrote %d rows -> gs://%s/%s", len(day_df), BUCKET_NAME, destination)