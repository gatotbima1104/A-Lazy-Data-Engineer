from datetime import datetime, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from includes.tasks.bigquery import load_provinces, load_regencies
from includes.tasks.native import enrich_and_load_location, enrich_stream_location
from includes.tasks.sensor import (
    branch_source,
    check_fire_detection_source,
    get_source_object,
)

from dags.setup import FIRE_DETECTION_DATE, REFERENCE_GCS_SOURCES


@dag(
    dag_id="batch_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule='@daily',
    tags=["batching", "bigquery", "gcs", "dbt", "geojson"],
    params={
        "date": FIRE_DETECTION_DATE
    }
)
def batch_fire_detections():
    
    regency = REFERENCE_GCS_SOURCES["regency"]
    province = REFERENCE_GCS_SOURCES["province"]
    
    # Check detection file w/ sensor
    sensor_result = check_fire_detection_source()
    source_obj = get_source_object(sensor_result)
    
    # Load location
    load_reg = load_regencies(source_object=regency)
    load_prov = load_provinces(source_object=province)
    
    # Decide batch or stream
    choose_source = BranchPythonOperator(
        task_id="choose_source",
        python_callable=branch_source,
    )
    
    # Batch task
    batch_location = enrich_and_load_location(
        detection_source=source_obj,
        regency_source=regency
    )
    
    # Stream task
    stream_location = enrich_stream_location(
        regency_source=regency
    )
    
    # Both batch and stream arrive
    source_ready = EmptyOperator(
        task_id="source_ready",
        trigger_rule="none_failed_min_one_success",
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
    )
    
    # DAG FLOWS
    sensor_result >> [load_reg, load_prov] # Check source result
    [load_reg, load_prov] >> choose_source
    
    choose_source >> batch_location
    choose_source >> stream_location
    
    batch_location >> source_ready
    stream_location >> source_ready

    # Finally
    source_ready >> trigger_transform

batch_fire_detections()
