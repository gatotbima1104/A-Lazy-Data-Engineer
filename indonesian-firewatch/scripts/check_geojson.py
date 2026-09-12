from pathlib import Path

import geopandas as gpd

BOUNDARY_DIR = Path("data/geojsons/with-districts")


def main() -> None:
    files = list(BOUNDARY_DIR.glob("*.geojson"))
    print(f"Found {len(files)} GeoJSON files")

    for file in files[:5]:
        gdf = gpd.read_file(file)

        print(f"\n{file.name}")
        print(f"  CRS: {gdf.crs}")
        print(f"  Geometry: {gdf.geometry.geom_type.tolist()}")
        print(f"  Valid: {gdf.geometry.is_valid.all()}")
        print(f"  Rows: {len(gdf)}")


if __name__ == "__main__":
    main()
    