from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import pandas as pd
from camoufox.sync_api import Camoufox

GOOGLE_MAPS_URL = "https://www.google.com/maps/search/?api=1&query={latitude},{longitude}"
LOCATION_SELECTOR = "span.JpCtJf span.DkEaL"
COLUMNS = [
    "location_id",
    "latitude_key",
    "longitude_key",
    "county",
    "state",
    "country",
]


def get_location(page, latitude: float, longitude: float) -> str | None:
    url = GOOGLE_MAPS_URL.format(latitude=latitude, longitude=longitude)
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    try:
        element = page.locator(LOCATION_SELECTOR).first
        element.wait_for(state="visible", timeout=15_000)
        return element.inner_text().strip()
    except Exception:
        return None


def parse_location(location: str | None) -> dict:
    if not location:
        return {"county": None, "state": None, "country": None}

    parts = [part.strip() for part in location.split(",")]

    if len(parts) < 3:
        return {"county": None, "state": None, "country": "Indonesia"}

    return {
        "county": parts[-2],
        "state": parts[-1],
        "country": "Indonesia",
    }


def load_locations(output_file: Path) -> dict[tuple[float, float], int]:
    if not output_file.exists():
        return {}

    df = pd.read_csv(output_file)

    return {
        (row.latitude_key, row.longitude_key): int(row.location_id)
        for row in df.itertuples()
    }


def get_next_location_id(locations: dict[tuple[float, float], int]) -> int:
    if not locations:
        return 1

    return max(locations.values()) + 1


def append_result(output_file: Path, result: dict) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_file.exists()

    with output_file.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(result)


def main(input_file: Path, output_file: Path, start: int, limit: int) -> None:
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
        .reset_index(drop=True)
    )

    total = len(coordinates)
    end = min(start + limit, total)
    coordinates = coordinates.iloc[start:end]

    locations = load_locations(output_file)
    next_location_id = get_next_location_id(locations)

    print(f"Total unique coordinates: {total}")
    print(f"Already processed: {len(locations)}")
    print(f"Processing: {start + 1}-{end} of {total}")

    with Camoufox(headless=True) as browser:
        page = browser.new_page()

        for index, (_, row) in enumerate(coordinates.iterrows(), start=start + 1):
            latitude = float(row["latitude"])
            longitude = float(row["longitude"])
            key = (latitude, longitude)

            print(f"[{index}/{total}] {latitude}, {longitude}")

            if key in locations:
                print(f"  Already exists: location_id={locations[key]}")
                continue

            try:
                location = get_location(page, latitude, longitude)
                parsed = parse_location(location)

                location_id = next_location_id

                result = {
                    "location_id": location_id,
                    "latitude_key": latitude,
                    "longitude_key": longitude,
                    **parsed,
                }

                append_result(output_file, result)

                locations[key] = location_id
                next_location_id += 1

                if location:
                    print(
                        f"  location_id={location_id}: "
                        f"{parsed['county']}, "
                        f"{parsed['state']}, "
                        f"{parsed['country']}"
                    )
                else:
                    print(f"  location_id={location_id}: Location not found")

            except Exception as exc:
                print(f"  Failed: {exc}")

            time.sleep(2)

    print(f"Finished processing {start + 1}-{end}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    main(
        input_file=args.input,
        output_file=args.output,
        start=args.start,
        limit=args.limit,
    )