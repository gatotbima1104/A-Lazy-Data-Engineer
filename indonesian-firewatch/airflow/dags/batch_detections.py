from datetime import datetime, timezone

from airflow.sdk import dag
from includes.notify import on_success_callback
from includes.tasks.bigquery import load_provinces, load_regencies
from includes.tasks.dbt import build_intermediate, build_marts, build_staging
from includes.tasks.native import enrich_and_load_location
from includes.tasks.sensor import check_fire_detection_source

from dags.setup import FIRE_DETECTION_DATE, REFERENCE_GCS_SOURCES


@dag(
    dag_id="batch_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule='@daily',
    tags=["batching", "bigquery", "gcs", "dbt"],
    on_success_callback=on_success_callback,
    params={
        "date": FIRE_DETECTION_DATE
    }
)
def batch_fire_detections():
    
    regency = REFERENCE_GCS_SOURCES["regency"]
    province = REFERENCE_GCS_SOURCES["province"]
    
    # Load reference
    load_reg = load_regencies(source_object=regency)
    load_prov = load_provinces(source_object=province)
    
    # Check detection file w/ sensor
    sensor_result = check_fire_detection_source()
    
    # Enrich location based on lat &lon
    enrich_location = enrich_and_load_location(
        detection_source=sensor_result,
        regency_source=regency
    )
    
    # dbt tasks
    stg_layer = build_staging()
    int_layer = build_intermediate()
    marts_layer = build_marts()
    
    # DAGs Dependencies
    sensor_result >> enrich_location
    
    [load_reg, load_prov] >> enrich_location

    enrich_location \
        >> stg_layer \
        >> int_layer \
        >> marts_layer

batch_fire_detections()
