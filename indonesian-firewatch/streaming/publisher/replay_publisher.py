from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from fastavro import json_writer, parse_schema
from google.cloud import pubsub_v1, storage

from streaming.setup import DETECTION_SOURCE, SCHEMA_PATH, SCHEMA_VERSION, SOURCE_TABLE
from utils.constant import BUCKET_NAME, PROJECT_ID, TOPIC_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


class ReplayPublisher:
    def __init__(self):
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.bucket = self.storage_client.bucket(BUCKET_NAME)
        
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = (f"projects/{PROJECT_ID}/topics/{TOPIC_ID}")
        
        with SCHEMA_PATH.open("r", encoding="utf-8") as file:
            self.schema = parse_schema(json.load(file))
            
    def list_parquet_files(
            self,
            date_value: date
        ) -> list[str]:
    
            prefix = (f"nrt/{date_value:%Y}/{date_value:%m}/")
            date_string = date_value.strftime("%Y-%m-%d")
            blobs = self.bucket.list_blobs(prefix=prefix)
    
            return sorted(
                blob.name
                for blob in blobs
                if (
                    blob.name.endswith(".parquet")
                    and date_string in blob.name
                )
            )
    
    def read_parquet(
        self,
        blob_name: str,
    ) -> pd.DataFrame:

        blob = self.bucket.blob(blob_name)
        logger.info("Downloading gs://%s/%s", BUCKET_NAME, blob_name)

        data = blob.download_as_bytes()

        return pd.read_parquet(
            io.BytesIO(data)
        )
    
    def create_event_id(
        self,
        row: pd.Series,
    ) -> str:

        values = [
            str(row["latitude"]),
            str(row["longitude"]),
            str(row["acq_date"]),
            str(row["acq_time"]),
            str(row["satellite"]),
            str(row["instrument"])
        ]

        raw = "|".join(values)

        return hashlib.md5(
            raw.encode("utf-8")
        ).hexdigest()

    def create_event(
        self,
        row: pd.Series,
    ) -> dict:

        return {
            # Metadata
            "event_id": self.create_event_id(row),
            "source": DETECTION_SOURCE,
            "source_table": SOURCE_TABLE,
            "schema_version": SCHEMA_VERSION,
            "published_timestamp": (
                datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
            ),
            
            # Cols
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "brightness": float(row["brightness"]),
            "scan": float(row["scan"]),
            "track": float(row["track"]),
            
            "acq_date": pd.Timestamp(
                row["acq_date"]
            ).strftime("%Y-%m-%d"),
            
            "acq_time": int(row["acq_time"]),
            "satellite": str(row["satellite"]),
            "instrument": str(row["instrument"]),
            "confidence": str(row["confidence"]),
            "version": str(row["version"]),
            "bright_t31": float(row["bright_t31"]),
            "frp": float(row["frp"]),
            "daynight": str(row["daynight"]),
        }

    def serialize_avro(
        self,
        event: dict
    ) -> bytes:

        """
        Encode to Json+Avro
        """
        
        buffer = io.StringIO()
        json_writer(buffer, self.schema, [event])
        
        return buffer.getvalue().encode("utf-8")

    def publish(
        self,
        event: dict,
        dry_run: bool = False
    ) -> None:
        
        if not dry_run:
            logger.info("Event: %s", json.dumps(event, indent=2, default=str))
            
        else:
            payload = self.serialize_avro(event)
            future = self.publisher.publish(
                topic=self.topic_path,
                data=payload
            )
            
            message_id = future.result()
            logger.info("Published event_id=%s message_id=%s", event["event_id"], message_id)

    def replay(
        self,
        start_date: date,
        end_date: date,
        interval: float,
        dry_run: bool = False
    ) -> None:
        
        current_date = start_date
        total = 0
        
        try:
            while current_date <= end_date:
                files = self.list_parquet_files(current_date)
                if not files:
                    
                    logger.warning("No Parquet files found for %s", current_date)
                    
                    current_date += timedelta(days=1)
                    continue
                
                logger.info("Found %d Parquet files for %s", len(files), current_date)
                
                for file_name in files:
                    
                    data = self.read_parquet(file_name)
                    logger.info("Loaded %d records from %s", len(data), file_name)
                    
                    for _, row in data.iterrows():
                        
                        event = self.create_event(row)
                        self.publish(
                            event=event,
                            dry_run=dry_run
                        )
                        
                        total += 1
                        
                        if interval > 0:
                            time.sleep(interval)
                
                current_date += timedelta(days=1)
            logger.info("Replay completed: %d records", total)
            
        except KeyboardInterrupt:
            logger.warning("Replay interrupted by user. Processed %d records.", total)