from datetime import datetime, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from dags.setup import FIRE_DETECTION_DATE
from streaming.publisher.firms_publisher import FirmsPublisher
from utils.constant import MAP_KEY


def publish_firms_data(
    date_value: str,
) -> None:

    satellite = "VIIRS_SNPP_NRT"
    areas = "95,-11,141,6"
    interval = 0.1
    day_range = 1

    publisher = FirmsPublisher(
        map_key=MAP_KEY,
        satellite=satellite,
        day_range=day_range,
        area=areas
    )

    publisher.replay(
        interval=interval,
        date=datetime.strptime(date_value, "%Y-%m-%d").date(),  # noqa: DTZ007
        dry_run=False,
    )

@dag(
    dag_id="stream_fire_detections",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule="0 0 * * *",
    tags=["stream", "bigquery", "api"],
    params={
        "date": FIRE_DETECTION_DATE
    }
)
def ingest_from_api():
    PythonOperator(
        task_id="publish_firms_data",
        python_callable=publish_firms_data,
        op_kwargs={
            "date_value": "{{ params.get('date') or logical_date | ds }}"
        }
    )

ingest_from_api()