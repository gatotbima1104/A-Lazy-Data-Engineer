from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


BOUNDARY_DIR = Path(
    "data/geojsons/with-districts"
)


latitude = -1.24671
longitude = 116.81465

point = Point(longitude, latitude)

found = []

for file in sorted(BOUNDARY_DIR.glob("*.geojson")):
    gdf = gpd.read_file(file)

    matches = gdf[gdf.geometry.intersects(point)]

    if not matches.empty:
        found.append(
            matches[["WADMKK", "WADMPR"]]
        )

if found:
    print(pd.concat(found, ignore_index=True))
else:
    print("No administrative polygon contains this coordinate.")