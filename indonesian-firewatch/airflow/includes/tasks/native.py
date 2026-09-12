import pandas as pd
from airflow.sdk import task

from includes.tasks.bigquery import load_dataframe_to_bq
from includes.tasks.setup import BOUNDARY_DIR, RAW_TABLE_NAME, REGENCY_FILE
from scripts.enrich_location import FirmsLocationEnricher
from utils.constant import BUCKET_NAME


@task
def enrich_and_load_location(
    source_object: str
) -> str:
    """
    Enrich fire detections from GCP -> 
    """
    
    source_uri = f"gs://{BUCKET_NAME}/{source_object}"
    print(f"Reading {source_uri}")

    data = pd.read_parquet(source_uri)
    print(f"Loaded {len(data):,} fire detections")
    
    enricher = FirmsLocationEnricher(
        boundary_dir=BOUNDARY_DIR,
        regency_file=REGENCY_FILE,
    )
    
    enriched_data = enricher.run(data)
    print(f"Enrichment completed: {len(enriched_data):,} fire detections")
    
    load_dataframe_to_bq(
        data=enriched_data,
        table_name=RAW_TABLE_NAME,
    )