from datetime import datetime, timezone

from airflow.sdk import dag
from includes.tasks.dbt import (
    build_intermediate,
    build_marts,
    build_staging,
)
from includes.tasks.native import enrich_stream_location

from dags.setup import REFERENCE_GCS_SOURCES


@dag(
    dag_id="transform_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["streaming", "dbt", "transformation", "geojson"]
)
def transform_fire_detections():
    
    regency = REFERENCE_GCS_SOURCES["regency"]

    # Enrich new streaming detections
    stream_location = enrich_stream_location(
        regency_source=regency
    )

    # dbt
    staging = build_staging()
    intermediate = build_intermediate()
    marts = build_marts()

    stream_location >> staging >> intermediate >> marts


transform_fire_detections()