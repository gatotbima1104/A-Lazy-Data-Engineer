from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main(input_file: Path) -> None:
    df = pd.read_csv(input_file)

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df.dropna(subset=["latitude", "longitude"])

    exact_unique = (
        df[["latitude", "longitude"]]
        .drop_duplicates()
    )

    total_exact = len(exact_unique)

    print("=" * 60)
    print("FIRMS Coordinate Analysis")
    print("=" * 60)
    print(f"Total valid rows       : {len(df):,}")
    print(f"Unique exact coordinates: {total_exact:,}")
    print()

    for precision in [0, 1, 2, 3, 4, 5]:
        rounded = df[["latitude", "longitude"]].round(precision)
        unique_count = len(rounded.drop_duplicates())

        reduction = (1 - unique_count / total_exact) * 100

        print(
            f"@{precision}dp"
            f" → {unique_count:>7,} unique"
            f" → {reduction:>6.2f}% reduction"
        )

    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    args = parser.parse_args()

    main(args.input)