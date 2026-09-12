from pathlib import Path

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