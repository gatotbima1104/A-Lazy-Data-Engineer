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
        regency_file: Path
    ) -> None:
        self.boundary_dir = boundary_dir
        self.regency_file = regency_file
        
        self.boundaries = self.load_boundaries()
        self.regency = self.load_regency()
    
    def normalize_name(
        self,
        value: str
    ) -> str:
        """
        Normalize regency_name
        """
        if pd.isna(value):
            return ""

        value = str(value).strip().upper()
        value = re.sub(r"^(KABUPATEN|KOTA)\s+", "", value)
        value = re.sub(r"[^A-Z0-9]", "", value)

        return REGENCY_ALIASES.get(value, value)

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
            
            # Drop Nan of this subset
            gdf = gdf.dropna(subset=["WADMKK", "geometry"])

            # Make the regency key
            gdf["regency_key"] = gdf["WADMKK"].map(self.normalize_name)
            frames.append(gdf)
        
        if not frames:
            raise FileNotFoundError(f"No GeoJSON files found in {self.boundary_dir}")

        return gpd.GeoDataFrame(
            pd.concat(frames, ignore_index=True),
            crs=STANDARD_GEOGRAPHIC_COORDINATE_SYSTEM,
        )

    def load_regency(
        self
    ) -> pd.DataFrame:
        """
        Load regency databases
        """
        df = pd.read_csv(self.regency_file)
        
        df["regency_key"] = df["regency_name"].map(self.normalize_name)

        return df

    def create_fire_points(
        self,
        df: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """
        Create fire points by Lon&Lat
        """
        df = df.copy()

        # Convert lat/lon to numeric -> error to Nan
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        
        df = df.dropna(subset=["latitude", "longitude"])

        # Ex. -6.20    | 106.80    | POINT(106.80 -6.20)
        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
            crs=STANDARD_GEOGRAPHIC_COORDINATE_SYSTEM,
        )

    def enrich_locations(
        self,
        fires: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """
        Enrich location to detection
        """
        
        # -6.20    | 106.80    | JAKARTASELATAN
        joined = gpd.sjoin(
            fires,
            self.boundaries[["regency_key", "geometry"]],
            how="left",
            predicate="intersects",
        )

        # BATAM       | 2171
        joined = joined.merge(
            self.regency[["regency_id", "regency_key"]],
            on="regency_key",
            how="left",
        )

        # Left off only the regency_id
        joined = joined.drop(
            columns=["geometry", "index_right", "regency_key"],
            errors="ignore",
        )
        
        # Convert id to Int64
        joined["regency_id"] = joined["regency_id"].astype("Int64")

        return pd.DataFrame(joined)

    def run(
        self,
        fires: pd.DataFrame
    ) -> None:
        
        print("Loading FIRMS detections...")
        fires = self.create_fire_points(fires)
        print(f"Loaded {len(fires):,} fire detections")
        
        print("Performing point-in-polygon join...")
        enriched = self.enrich_locations(fires)
        
        print("Enrich completed")
        return enriched
        
        # unmatched = enriched["regency_id"].isna().sum()
        # print(f"Unmatched detections: {unmatched:,}")