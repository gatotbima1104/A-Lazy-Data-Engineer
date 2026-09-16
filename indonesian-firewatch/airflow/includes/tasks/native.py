import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.sdk import get_current_context, task
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from includes.tasks.bigquery import load_dataframe_to_bq
from includes.tasks.setup import (
    BOUNDARY_DIR,
    BQ_TABLE_STREAMING_ENRICHED,
    BQ_TABLE_STREAMING_RAW,
    RAW_TABLE_NAME,
)
from scripts.enrich_location import FirmsLocationEnricher
from utils.constant import BQ_DATASET_STREAMING, BUCKET_NAME, PROJECT_ID


@task
def enrich_and_load_location(
    detection_source: str,
    regency_source: str
) -> str:
    """
    Enrich fire detections from GCP -> 
    """
    
    detection_uri = f"gs://{BUCKET_NAME}/{detection_source}"
    print(f"Reading fire detections: {detection_uri}")

    data = pd.read_parquet(detection_uri)
    print(f"Loaded {len(data):,} fire detections")
    
    regency_uri = f"gs://{BUCKET_NAME}/{regency_source}"
    print(f"Reading regency: {regency_source}")

    regency_data = pd.read_parquet(regency_uri)
    print(f"Loaded {len(regency_data):,} regency records")
    
    enricher = FirmsLocationEnricher(
        boundary_dir=BOUNDARY_DIR,
        regency_data=regency_data
    )
    
    enriched_data = enricher.run(data)
    print(f"Enrichment completed: {len(enriched_data):,} fire detections")
    
    load_dataframe_to_bq(
        data=enriched_data,
        table_name=RAW_TABLE_NAME,
    )
    
    return detection_source

@task
def enrich_stream_location(
    regency_source: str,
) -> int:
    """
    Enrich new streaming fire detections with location
    and load them into the enriched streaming table.
    """
    
    context = get_current_context()
    dag_run = context["dag_run"]
    
    conf = dag_run.conf or {}
    trigger_source = conf.get("trigger_source")
    fire_detection_date = conf.get("fire_detection_date")
    
    # If transform DAG was triggered by batch,
    # skip streaming enrichment but allow downstream tasks to continue.
    if trigger_source == "batch":
        print(f"Batch trigger detected for date {fire_detection_date}. Skipping streaming enrichment.")
        return 0
    
    client = bigquery.Client(project=PROJECT_ID)

    raw_table = (
        f"{PROJECT_ID}."
        f"{BQ_DATASET_STREAMING}."
        f"{BQ_TABLE_STREAMING_RAW}"
    )

    enriched_table = (
        f"{PROJECT_ID}."
        f"{BQ_DATASET_STREAMING}."
        f"{BQ_TABLE_STREAMING_ENRICHED}"
    )

    print(f"Reading new streaming detections: {raw_table}")

    # Create enriched table if not exist
    try:
        client.get_table(enriched_table)
        print(f"Enriched table already exists: {enriched_table}")

    except NotFound:
        print(f"Creating enriched table: {enriched_table}")

        raw_bq_table = client.get_table(raw_table)
        schema = list(raw_bq_table.schema)

        # Extend column
        schema.extend([
            bigquery.SchemaField("regency_id", "INT64", mode="NULLABLE")
        ])

        table = bigquery.Table(enriched_table, schema=schema)
        
        client.create_table(table)
        print(f"Created enriched table: {enriched_table}")

        # First run: read all streaming detections
        query = f"""
            SELECT *
            FROM `{raw_table}`
        """

    else:
        # Read only detections that have not been enriched yet -> NULL
        query = f"""
            SELECT raw.*
            FROM `{raw_table}` AS raw
            LEFT JOIN `{enriched_table}` AS enriched
                ON raw.event_id = enriched.event_id
            WHERE enriched.event_id IS NULL
        """

    data = client.query(query, location="asia-southeast2").to_dataframe()

    if data.empty:
        print("No new streaming detections to enrich.")
        raise AirflowSkipException("No new streaming detections to enrich.")

    print(f"Loaded {len(data):,} new streaming detections")

    # Read regency reference
    regency_uri = f"gs://{BUCKET_NAME}/{regency_source}"
    print(f"Reading regency: {regency_uri}")

    regency_data = pd.read_parquet(regency_uri)
    print(f"Loaded {len(regency_data):,} regency records")

    # Location enrichment
    enricher = FirmsLocationEnricher(
        boundary_dir=BOUNDARY_DIR,
        regency_data=regency_data,
    )

    enriched_data = enricher.run(data)
    print(f"Enrichment completed: {len(enriched_data):,} fire detections")

    # Append enriched data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    job = client.load_table_from_dataframe(
        enriched_data,
        enriched_table,
        job_config=job_config,
    )

    job.result()
    print(f"Loaded enriched detections: {enriched_table}")

    return len(enriched_data)