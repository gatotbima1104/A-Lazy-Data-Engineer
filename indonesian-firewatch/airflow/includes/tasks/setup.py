import os
from pathlib import Path


PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
)

BOUNDARY_DIR = (
    PROJECT_ROOT
    / "data"
    / "geojsons"
    / "with-districts"
)

REGENCY_FILE = (
    PROJECT_ROOT
    / "data"
    / "kaggle"
    / "regency.csv"
)

RAW_TABLE_NAME = "raw_fire_detections"


print("=" * 60)
print("SETUP PATH DEBUG")
print(f"setup.py        : {Path(__file__).resolve()}")
print(f"PROJECT_ROOT    : {PROJECT_ROOT}")
print(f"BOUNDARY_DIR    : {BOUNDARY_DIR}")
print(f"BOUNDARY EXISTS : {BOUNDARY_DIR.exists()}")
print(f"BOUNDARY IS DIR : {BOUNDARY_DIR.is_dir()}")

if BOUNDARY_DIR.exists():
    geojson_files = list(
        BOUNDARY_DIR.glob("*.geojson")
    )

    print(
        f"GEOJSON COUNT   : {len(geojson_files)}"
    )

    for file in geojson_files[:5]:
        print(f"  - {file}")

print("=" * 60)