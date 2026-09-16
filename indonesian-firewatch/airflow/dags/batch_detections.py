from datetime import datetime, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from includes.notify import on_failure_callback
from includes.tasks.bigquery import load_provinces, load_regencies
from includes.tasks.native import enrich_and_load_location
from includes.tasks.sensor import check_fire_detection_source

from dags.setup import FIRE_DETECTION_DATE, REFERENCE_GCS_SOURCES


@dag(
    dag_id="batch_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule='@daily',
    tags=["batching", "bigquery", "gcs", "dbt", "geojson"],
    on_failure_callback=on_failure_callback,
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
    
    # trigger transform
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_fire_detections",
        trigger_dag_id="transform_fire_detections",
        conf={
            "trigger_source": "batch",
            "fire_detection_date": "{{ params.get('date') or logical_date | ds }}",
        },
        wait_for_completion=False,
        deferrable=True,
    )
    
    [load_reg, load_prov, sensor_result] >> enrich_location
    enrich_location >> trigger_transform

batch_fire_detections()
