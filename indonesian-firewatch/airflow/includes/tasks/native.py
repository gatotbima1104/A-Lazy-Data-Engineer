import pandas as pd
from airflow.sdk import task

from includes.tasks.bigquery import load_dataframe_to_bq
from includes.tasks.setup import BOUNDARY_DIR, RAW_TABLE_NAME
from scripts.enrich_location import FirmsLocationEnricher
from utils.constant import BUCKET_NAME


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