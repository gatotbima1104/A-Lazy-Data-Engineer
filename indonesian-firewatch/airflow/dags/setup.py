from pathlib import Path

from airflow.models import Param

PROJECT_ROOT = Path("/opt/airflow/project")

REGENCY_PATH = (
    PROJECT_ROOT
    / "data"
    / "kaggle"
    / "regency.csv"
)

PROVINCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "kaggle"
    / "province.csv"
)

PROVINCE_DESTINATION ="raw/reference/province.csv"
REGENCY_DESTINATION="raw/reference/regency.csv"

FIRE_DETECTION_DATE = Param(
    default=None,
    type=["null", "string"],
    format="date",
    description="Process a single fire detection date.",
)