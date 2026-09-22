from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from includes.constant import GCP_CONN_ID, PG_CONN_ID
from includes.tasks.gcs import ingest_pg_to_gcs
from includes.tasks.notify import on_failure_callback

from dags.setup import SOURCE_TABLES

logger = logging.getLogger(__name__)


@dag(
    dag_id="postgres_ingest_to_gsc",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["ingestion", "postgres", 'gcs']
)
def postgres_ingest_to_gsc():
    
    @task(on_failure_callback=on_failure_callback)
    def ingest() -> None:
        
        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        for source_type, table_names in SOURCE_TABLES.items():
            for table_name in table_names:
                ingest_pg_to_gcs(
                    pg_hook=pg_hook,
                    gcs_hook=gcs_hook,
                    source_type=source_type,
                    table_name=table_name,
                )

    ingest()
        
postgres_ingest_to_gsc()
