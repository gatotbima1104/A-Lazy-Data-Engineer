from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from datetime import date, datetime, timezone

import pandas as pd
import requests
from fastavro import json_writer, parse_schema
from google.cloud import pubsub_v1, storage

from streaming.setup import DETECTION_SOURCE, SCHEMA_PATH, SCHEMA_VERSION, SOURCE_TABLE
from utils.constant import BUCKET_NAME, PROJECT_ID, TOPIC_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

class FirmsPublisher:
    def __init__(
        self,
        map_key: str,
        satellite: str,
        area: str,
        day_range: int
    ):
        self.map_key = map_key
        self.satellite = satellite
        self.areas = area
        self.day_range = day_range
        
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.bucket = self.storage_client.bucket(BUCKET_NAME)
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = (f"projects/{PROJECT_ID}/topics/{TOPIC_ID}")
        
        with SCHEMA_PATH.open("r", encoding="utf-8") as file:
            self.schema = parse_schema(json.load(file))
            
    def __list_parquet_files(
            self,
            date_value: date
        ) -> list[str]:

            """
            DEPRECATED: Since Rest API Available
            """
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
    
    def __read_parquet(
            self,
            blob_name: str,
        ) -> pd.DataFrame:
    
            """
            DEPRECATED: Since Rest API Available
            """
            blob = self.bucket.blob(blob_name)
            logger.info("Downloading gs://%s/%s", BUCKET_NAME, blob_name)
    
            data = blob.download_as_bytes()
    
            return pd.read_parquet(
                io.BytesIO(data)
            )
            
    def request_firms(
        self,
        date_val: date
    ) -> pd.DataFrame:
        
        date_str = date_val.strftime("%Y-%m-%d")
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{self.map_key}/{self.satellite}/{self.areas}/{self.day_range}/{date_val}"
        
        logger.info("Requesting FIRMS API for %s",date_str)
        
        try:
            res = requests.get(url, timeout=240)
            res.raise_for_status()
            data = pd.read_csv(io.StringIO(res.text))
        
        except requests.RequestException as exc:
            logger.error("FIRMS API request failed for %s: %s", date_str, exc)
            raise
        except pd.errors.EmptyDataError:
            logger.warning("FIRMS API returned no data for %s", date_str)
            return pd.DataFrame()

        if data.empty:
            logger.warning("No FIRMS detections found for %s", date_str)

        return data
    
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
            "brightness": float(row["bright_ti4"]),
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
            "bright_t31": float(row["bright_ti5"]),
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
        dry_run: bool = True
    ) -> None:
        """
        Publish Event to PubSUb
        """
        
        if dry_run:
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
        date: date,
        interval: float,
        dry_run: bool = True
    ) -> None:
        
        total = 0
        
        try:
            data = self.request_firms(date)
            if data.empty:    
                logger.warning("No Data found for %s", date)
                return 
            
            logger.info("Loaded %d records from API-%s", len(data), date)
            time.sleep(2)
            logger.info("Publsihing to PubSub")
            for _, row in data.iterrows():    
                event = self.create_event(row)
                self.publish(
                    event=event,
                    dry_run=dry_run
                )
                
                total += 1
                
                if interval > 0:
                    time.sleep(interval)
            
            logger.info("Replay completed: %d records", total)
                
            
        except KeyboardInterrupt:
            logger.warning("Replay interrupted by user. Processed %d records.", total)