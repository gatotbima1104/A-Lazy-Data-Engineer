import argparse
from datetime import date, datetime

from streaming.publisher.firms_publisher import FirmsPublisher


def parse_date(
    value: str
) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()  # noqa: DTZ007

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay FIRMS NRT Parquet data from GCS to Pub/Sub."
    )
    
    parser.add_argument(
        "--date",
        required=True,
        help="FIRMS date in YYYY-MM-DD format."
    )

    parser.add_argument(
        "--key",
        required=True,
        help="FIRMS API MAP_KEY."
    )

    parser.add_argument(
        "--satellite",
        required=True,
        help="FIRMS source, e.g. MODIS_NRT or VIIRS_NOAA20_NRT."
    )

    parser.add_argument(
        "--area",
        required=True,
        help="FIRMS area in west,south,east,north format."
    )

    parser.add_argument(
        "--day-range",
        type=int,
        default=1,
        help="FIRMS API day range. Default is 1."
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=10,
        help="Delay in seconds between published detections. Default is 10s/event."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not publish events to Pub/Sub."
    )
    
    args = parser.parse_args()
    
    # Exceptions handling
    if not args.date:
        parser.error("--date is required")

    if args.day_range < 1 or args.day_range > 5:
        parser.error("--day-range must be between 1 and 5.")

    if args.interval < 0:
        parser.error("--interval must be greater than or equal to 0.")
    
    publisher = FirmsPublisher(
        map_key=args.key,
        day_range=args.day_range,
        satellite=args.satellite,
        area=args.area,
    )
    
    publisher.replay(
        date=parse_date(args.date),
        interval=args.interval,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()