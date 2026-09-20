"""
Download all Indonesia province boundary GeoJSON files (with district-level detail).

Source: dmxsan/indonesia-admin-boundaries (GitHub)
Path: processed-data/02-provinces/with-districts/
Original data: Badan Informasi Geospasial (BIG) - https://geoportal.big.go.id/
"""

import json
import os
import urllib.request

REPO = "dmxsan/indonesia-admin-boundaries"
BRANCH = "main"
FOLDER_PATH = "processed-data/02-provinces/with-districts"

API_URL = f"https://api.github.com/repos/{REPO}/contents/{FOLDER_PATH}?ref={BRANCH}"
RAW_BASE_URL = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/{FOLDER_PATH}"

OUTPUT_DIR = "data/geojsons/with-districts"


def list_remote_files() -> list[str]:
    """
    Return a list of .geojson file names in the target folder via GitHub API.
    """
    req = urllib.request.Request(API_URL, headers={"Accept": "application/vnd.github+json"})
    
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())

    return [
        item["name"]
        for item in data
        if item.get("type") == "file" and item["name"].endswith(".geojson")
    ]


def download_file(url: str, dest_path: str) -> None:
    print(f"Downloading {url} -> {dest_path}")
    urllib.request.urlretrieve(url, dest_path)


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Listing files from {API_URL} ...")
    try:
        filenames = list_remote_files()
    except Exception as e:
        print(f"Failed to list remote files: {e}")
        return

    print(f"Found {len(filenames)} GeoJSON files.")

    for filename in filenames:
        url = f"{RAW_BASE_URL}/{filename}"
        dest_path = os.path.join(OUTPUT_DIR, filename)

        if os.path.exists(dest_path):
            print(f"Skipping {filename}, already exists.")
            continue

        try:
            download_file(url, dest_path)
        except Exception as e:
            print(f"Failed to download {filename}: {e}")

    print(f"Done. Files saved under '{OUTPUT_DIR}/' (gitignored).")


if __name__ == "__main__":
    main()