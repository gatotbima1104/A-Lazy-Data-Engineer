from datetime import datetime, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from includes.tasks.dbt import (
    build_intermediate,
    build_marts,
    build_staging,
)
from includes.tasks.native import enrich_stream_location
from includes.tasks.quality_check import create_quality_group

from dags.setup import REFERENCE_GCS_SOURCES
from utils.constant import BQ_DATASET_INTERMEDIATE, BQ_DATASET_MART, BQ_DATASET_STAGING


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
    qa_stg = create_quality_group(
        group_id="quality_check_stg",
        dataset=BQ_DATASET_STAGING,
        table="stg_detection",
        not_null_columns=[
            'regency_id'
        ]
    )
    
    intermediate = build_intermediate()
    qa_int = create_quality_group(
        group_id="quality_check_int",
        dataset=BQ_DATASET_INTERMEDIATE,
        table="int_detection_join",
        not_null_columns=[
            'regency_id'
        ],
        invalid_rules={
            "null_regency_id": "regency_id IS NULL",
            "invalid_regency_id": "regency_id = -999"
        },
    )
    
    marts = build_marts()
    qa_mart = create_quality_group(
        group_id="quality_check_mart",
        dataset=BQ_DATASET_MART,
        table="daily_detection"
    )
    
    # dbt docs
    trigger_docs = TriggerDagRunOperator(
        task_id="trigger_dbt_docs",
        trigger_dag_id="generate_dbt_docs",
        wait_for_completion=False,
    )
    
    # DAG Flows
    stream_location >> staging
    staging >> qa_stg >> intermediate
    intermediate >> qa_int >> marts
    marts >> qa_mart >> trigger_docs


transform_fire_detections()