from pathlib import Path

import geopandas as gpd
import pandas as pd


BOUNDARY_DIR = Path("data/geojsons/with-districts")
REFERENCE_FILE = Path("data/kaggle/regency.csv")


def normalize_name(value: str) -> str:
    value = str(value).strip().upper()

    for prefix in ("KABUPATEN ", "KOTA "):
        if value.startswith(prefix):
            value = value[len(prefix):]

    return " ".join(value.split())


boundaries = []

for file in sorted(BOUNDARY_DIR.glob("*.geojson")):
    gdf = gpd.read_file(file)
    boundaries.append(gdf[["WADMKK", "WADMPR"]])

boundaries = pd.concat(boundaries, ignore_index=True).drop_duplicates()

regency = pd.read_csv(REFERENCE_FILE)

boundaries["match_name"] = boundaries["WADMKK"].map(normalize_name)
regency["match_name"] = regency["regency_name"].map(normalize_name)

result = boundaries.merge(
    regency[["regency_id", "province_id", "regency_name", "match_name"]],
    on="match_name",
    how="left",
)

print(result.to_string(index=False))

print()
print("Matched:", result["regency_id"].notna().sum())
print("Unmatched:", result["regency_id"].isna().sum())

print()
print("UNMATCHED:")
print(
    result.loc[
        result["regency_id"].isna(),
        ["WADMKK", "WADMPR"],
    ].to_string(index=False)
)