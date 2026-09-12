from datetime import datetime, timezone

from airflow.models import Param
from airflow.sdk import dag
from includes.notify import on_success_callback
from includes.tasks.bigquery import load_provinces, load_regencies
from includes.tasks.gcs import upload_reference_to_gcs
from includes.tasks.native import enrich_and_load_location
from includes.tasks.sensor import check_fire_detection_source

from dags.setup import PROVINCE_PATH, REGENCY_PATH


@dag(
    dag_id="batch_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    tags=["firms", "batch"],
    on_success_callback=on_success_callback,
    params={
        "date": Param(
            "2026-01-01",
            type="string",
            format="date",
            description="Fire detection date",
        ),
    }
)
def batch_fire_detections():
    
    regency_gcs = upload_reference_to_gcs.override(
        task_id="upload_regency_reference"
    )(
        source_file=str(REGENCY_PATH),
        destination="raw/reference/regency.csv",
    )

    province_gcs = upload_reference_to_gcs.override(
        task_id="upload_province_reference"
    )(
        source_file=str(PROVINCE_PATH),
        destination="raw/reference/province.csv",
    )
    
    load_regency = load_regencies(source_object=regency_gcs)
    load_province = load_provinces(source_object=province_gcs)
    
    sensor_data_source = check_fire_detection_source()
    enrich_location = enrich_and_load_location(
        source_object=sensor_data_source
    )
    
    ## Dependencies
    
    regency_gcs >> load_regency
    province_gcs >> load_province

    [load_regency, load_province] >> sensor_data_source

    sensor_data_source >> enrich_location

batch_fire_detections()
