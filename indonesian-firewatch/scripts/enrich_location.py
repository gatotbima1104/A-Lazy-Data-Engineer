from __future__ import annotations

import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

from scripts.setup import REGENCY_ALIASES, STANDARD_GEOGRAPHIC_COORDINATE_SYSTEM


class FirmsLocationEnricher:
    """
    Enrich location details based on lat/lon of detection
    Added Regency, Province, Country to the detections
    """
    
    def __init__(
        self,
        boundary_dir: Path,
        regency_data: pd.DataFrame
    ) -> None:
        self.boundary_dir = boundary_dir
        self.regency = self.load_regency(regency_data)
        self.boundaries = self.load_boundaries()

    def normalize_name(
        self,
        value: str
    ) -> str:
        """
        Normalize administrative name into a comparable key.

        Rules:
            KABUPATEN BOGOR -> BOGOR
            KOTA BOGOR      -> KOTA_BOGOR
            BOGOR           -> BOGOR
            KOTA BOGOR      -> KOTA_BOGOR

        This allows Kabupaten and Kota with the same name
        to remain ambigiousnes.
        """

        if pd.isna(value):
            return ""

        value = str(value).strip().upper()

        value = re.sub(r"\s+", " ", value)

        # KABUPATEN KEDIRI -> KEDIRI
        if value.startswith("KABUPATEN "):
            value = value[len("KABUPATEN "):]

        # KOTA BOGOR  -> KOTA_BOGOR
        elif value.startswith("KOTA "):
            value = "KOTA_" + value[len("KOTA "):]

        # Remove spaces and special characters.
        value = re.sub(r"[^A-Z0-9_]", "", value)
        value = REGENCY_ALIASES.get(value, value)

        return value

    def load_boundaries(
        self
    ) -> gpd.GeoDataFrame:
        """
        Load boundaries contain .geojson
        """
        frames = []

        for file in sorted(self.boundary_dir.glob("*.geojson")):
            gdf = gpd.read_file(file)

            gdf = gdf[["WADMKK", "WADMPR", "geometry"]].copy()
            
            gdf = gdf.dropna(subset=["WADMKK", "geometry"])

            # Kediri       -> KEDIRI
            # Kota Kediri  -> KOTA_KEDIRI
            gdf["regency_key"] = (gdf["WADMKK"].map(self.normalize_name))

            frames.append(gdf)

        if not frames:
            raise FileNotFoundError(f"No GeoJSON files found in {self.boundary_dir}")

        return gpd.GeoDataFrame(
            pd.concat(frames, ignore_index=True),
            crs=STANDARD_GEOGRAPHIC_COORDINATE_SYSTEM,
        )

    def load_regency(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Load regency databases
        """
        df = df.copy()
        
        df["regency_key"] = (
            df["regency_name"]
            .map(self.normalize_name)
        )

        return df

    def create_fire_points(
        self,
        df: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """
        Create fire points by Lon&Lat
        """
        df = df.copy()

        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        df = df.dropna(
            subset=[
                "latitude",
                "longitude",
            ]
        )

        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df["longitude"],
                df["latitude"],
            ),
            crs=STANDARD_GEOGRAPHIC_COORDINATE_SYSTEM,
        )

    def enrich_locations(
        self,
        fires: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        """
        Enrich location to detection.
        """

        # WADMKK = "Kediri"
        # -> regency_key = "KEDIRI"
        joined = gpd.sjoin(
            fires,
            self.boundaries[
                [
                    "regency_key",
                    "geometry",
                ]
            ],
            how="left",
            predicate="intersects",
        )


        # KEDIRI       -> KABUPATEN KEDIRI -> 3506
        # KOTA_KEDIRI  -> KOTA KEDIRI      -> 3571
        joined = joined.merge(
            self.regency[
                [
                    "regency_id",
                    "regency_key",
                ]
            ],
            on="regency_key",
            how="left",
            validate="many_to_one",
        )

        joined = joined.drop(
            columns=[
                "geometry",
                "index_right",
                "regency_key",
            ],
            errors="ignore",
        )

        joined["regency_id"] = (
            joined["regency_id"]
            .astype("Int64")
        )

        return pd.DataFrame(joined)

    def run(
        self,
        fires: pd.DataFrame,
    ) -> pd.DataFrame:
        
        print("Loading FIRMS detections...")
        fires = self.create_fire_points(fires)

        print(f"Loaded {len(fires):,} fire detections")
        
        print("Performing point-in-polygon join...")
        enriched = self.enrich_locations(fires)

        print("Enrich completed")
        return enriched