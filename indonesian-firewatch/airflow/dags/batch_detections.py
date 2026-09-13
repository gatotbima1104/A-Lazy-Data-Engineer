from datetime import datetime, timezone

from airflow.sdk import dag
from includes.notify import on_success_callback
from includes.tasks.bigquery import load_provinces, load_regencies
from includes.tasks.dbt import (
    build_intermediate,
    build_marts,
    build_staging,
    install_packages,
)
from includes.tasks.gcs import upload_reference_to_gcs
from includes.tasks.native import enrich_and_load_location
from includes.tasks.sensor import check_fire_detection_source

from dags.setup import (
    FIRE_DETECTION_DATE,
    PROVINCE_DESTINATION,
    PROVINCE_PATH,
    REGENCY_DESTINATION,
    REGENCY_PATH,
)


@dag(
    dag_id="batch_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["firms", "batch"],
    on_success_callback=on_success_callback,
    params={
        "date": FIRE_DETECTION_DATE
    }
)
def batch_fire_detections():
    
    regency_gcs = upload_reference_to_gcs.override(
        task_id="upload_regency_reference"
    )(
        source_file=str(REGENCY_PATH),
        destination=REGENCY_DESTINATION,
    )

    province_gcs = upload_reference_to_gcs.override(
        task_id="upload_province_reference"
    )(
        source_file=str(PROVINCE_PATH),
        destination=PROVINCE_DESTINATION,
    )
    
    load_regency = load_regencies(source_object=regency_gcs)
    load_province = load_provinces(source_object=province_gcs)
    
    sensor_data_source = check_fire_detection_source()
    enrich_location = enrich_and_load_location(
        source_object=sensor_data_source
    )
    
    # dbt tasks
    stg_layer = build_staging()
    int_layer = build_intermediate()
    marts_layer = build_marts()
    
    ## Dependencies
    regency_gcs >> load_regency
    province_gcs >> load_province

    [
        load_regency,
        load_province,
        sensor_data_source
    ] >> enrich_location
    
    enrich_location \
        >> stg_layer \
        >> int_layer \
        >> marts_layer

batch_fire_detections()
