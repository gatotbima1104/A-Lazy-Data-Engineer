from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"


def reverse_geocode(latitude: float, longitude: float) -> dict:
    params = {
        "format": "jsonv2",
        "lat": latitude,
        "lon": longitude,
        "addressdetails": 1,
        "layer": "address",
        "accept-language": "id",
    }

    response = requests.get(NOMINATIM_URL, params=params, timeout=30)
    response.raise_for_status()

    address = response.json().get("address", {})

    return {
        "latitude": latitude,
        "longitude": longitude,
        "county": address.get("county"),
        "state": address.get("state"),
        "country": address.get("country"),
    }


def main(input_file: Path, output_file: Path, limit: int) -> None:
    df = pd.read_csv(input_file)

    coordinates = (
        df[["latitude", "longitude"]]
        .dropna()
        .assign(
            latitude=lambda x: x["latitude"].round(2),
            longitude=lambda x: x["longitude"].round(2),
        )
        .drop_duplicates()
        .sort_values(["latitude", "longitude"])
        .head(limit)
    )

    print(f"Found {len(coordinates)} unique coordinates to process")

    results = []

    for index, row in coordinates.iterrows():
        latitude = float(row["latitude"])
        longitude = float(row["longitude"])

        print(f"[{len(results) + 1}/{len(coordinates)}] {latitude}, {longitude}")

        try:
            result = reverse_geocode(latitude, longitude)
            results.append(result)
        except requests.RequestException as exc:
            print(f"Failed for {latitude}, {longitude}: {exc}")

        time.sleep(1)

    result_df = pd.DataFrame(results)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_file, index=False)

    print(f"Saved {len(result_df)} locations to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()
    main(input_file=args.input, output_file=args.output, limit=args.limit)