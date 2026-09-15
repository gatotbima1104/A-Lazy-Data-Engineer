import argparse
from datetime import date, datetime

from streaming.publisher.replay_publisher import ReplayPublisher


def parse_date(
    value: str
) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()  # noqa: DTZ007

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay FIRMS NRT Parquet data from GCS to Pub/Sub."
    )
    
    date_group = (
        parser.add_mutually_exclusive_group()
    )
    
    date_group.add_argument(
        "--date",
        help=("Single replay date in YYYY-MM-DD format.")
    )
    
    date_group.add_argument(
        "--start-date",
        help=("Start replay date in YYYY-MM-DD format.")
    )

    parser.add_argument(
        "--end-date",
        help=("End replay date in YYYY-MM-DD format.")
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=10,
        help=("Delay in seconds between published detections. Default in 10s/Event"),
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Publish events to Pub/Sub when enabled.",
    )
    
    args = parser.parse_args()
    
    # Exceptions handling
    if args.date and args.end_date:
        parser.error("--end-date cannot be used with --date.")
    
    if args.end_date and not args.start_date:
        parser.error("--start-date is required when --end-date is provided.")
        
    if args.start_date and not args.end_date:
            parser.error("--end-date is required when --start-date is provided.")

    if not args.date and not args.start_date:
        parser.error("Provide either --date or --start-date and --end-date.")

    if args.interval < 0:
        parser.error("--interval must be greater than or equal to 0.")
        
    # Args logic
    if args.date:
        start_date = parse_date(args.date)
        end_date = start_date
    
    else:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
    
    if end_date < start_date:
        parser.error("--end-date must be greater than or equal to --start-date.")
    
    publisher = ReplayPublisher()
    publisher.replay(
        start_date=start_date,
        end_date=end_date,
        interval=args.interval,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()
  

