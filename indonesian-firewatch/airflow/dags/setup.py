from pathlib import Path

from airflow.models import Param

PROJECT_ROOT = Path("/opt/airflow/project")

FIRE_DETECTION_DATE = Param(
    default=None,
    type=["null", "string"],
    format="date",
    description="Process a single fire detection date.",
)

SOURCE_TABLES = {
    "archive": ["fire_detections_archive"],
    "nrt": ["fire_detections_nrt"],
    "reference": [
        "regency",
        "province",
    ],
}

REFERENCE_GCS_SOURCES = {
     "regency" : "reference/regency.parquet",
     "province" : "reference/province.parquet"
}