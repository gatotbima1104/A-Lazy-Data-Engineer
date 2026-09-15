from datetime import datetime, timezone

from streaming.firms_nrt_model import FirmsNrtDetection
from streaming.setup import (
    CONFIDENCE_LEVEL,
    DETECTION_PERIOD,
    PROCESSING_TYPE,
    SATELLITE,
    VERSION_PATTERN,
)


class DetectionTransformer:
    def __init__(self):
        pass
    
    def _standardize(
        self,
        event: dict,
    ) -> None:

        event["acq_date"] = datetime.strptime(
            event["acq_date"],
            "%Y-%m-%d",
        ).date()

        event["acq_time"] = int(
            event["acq_time"]
        )

        event["published_timestamp"] = datetime.fromisoformat(
            event["published_timestamp"].replace(
                "Z",
                "+00:00",
            )
        )

        event["ingested_timestamp"] = datetime.now(
            timezone.utc,
        )

        event["latitude"] = float(event["latitude"])
        event["longitude"] = float(event["longitude"])
        event["brightness"] = float(event["brightness"])
        event["scan"] = float(event["scan"])
        event["track"] = float(event["track"])
        event["bright_t31"] = float(event["bright_t31"])
        event["frp"] = float(event["frp"])
        
    def _enrich_time(self, event: dict) -> None:
        acquisition_date = event["acq_date"]
        acquisition_time = int(event["acq_time"])

        hour = acquisition_time // 100
        minute = acquisition_time % 100

        detection_datetime = datetime(  # noqa: DTZ001
            acquisition_date.year,
            acquisition_date.month,
            acquisition_date.day,
            hour,
            minute,
        )

        event["acquisition_time"] = detection_datetime.time()

        event["detection_datetime"] = detection_datetime
        event["detection_timestamp"] = (
            detection_datetime.replace(tzinfo=timezone.utc)
        )

        event["detection_year"] = acquisition_date.year
        event["detection_month"] = acquisition_date.month
        event["detection_day_of_week"] = (
            (acquisition_date.weekday() + 1) % 7 + 1
        )
        event["detection_hour"] = hour
        
    def _business_mapping(
        self,
        event: dict
    ) -> None:

        # Confidence
        event["confidence_level_label"] = CONFIDENCE_LEVEL.get(
            str(event["confidence"]).lower(), "UNKNOWN"
        )
        
        # Period n/d
        event["detection_period"] = DETECTION_PERIOD.get(
            str(event["daynight"]).upper(),
            "UNKNOWN",
        )

        # Satellite
        event["satellite_label"] = SATELLITE.get(
            str(event["satellite"]).upper(),
            "UNKNOWN",
        )
        
        # Version
        version = str(event["version"]).upper()
        match = VERSION_PATTERN.fullmatch(version)

        if match:
            event["collection_version"] = match.group(
                "collection_version"
            )

            processing_code = match.group("processing_code")

            event["processing_type"] = PROCESSING_TYPE.get(
                processing_code,
                "STANDARD",
            )

        else:
            event["collection_version"] = None
            event["processing_type"] = "UNKNOWN"
    
    def transform(
        self,
        event: dict
    ) -> FirmsNrtDetection:
        
        self._standardize(event)
        self._enrich_time(event)
        self._business_mapping(event)
        
        return FirmsNrtDetection(
            event_id=event["event_id"],
            source=event["source"],
            source_table=event["source_table"],
            schema_version=event["schema_version"],
            
            published_timestamp=event["published_timestamp"],
            ingested_timestamp=event["ingested_timestamp"],
            
            latitude=event["latitude"],
            longitude=event["longitude"],
            brightness=event["brightness"],
            scan=event["scan"],
            track=event["track"],
            acquisition_date=event["acq_date"],
            acquisition_time=event["acquisition_time"],
            satellite=event["satellite"],
            instrument=event["instrument"],
            confidence=event["confidence"],
            version=event["version"],
            brightness_temperature_t31=event["bright_t31"],
            fire_radiative_power=event["frp"],
            
            daynight=event["daynight"],
            detection_datetime=event["detection_datetime"],
            detection_timestamp=event["detection_timestamp"],
            detection_year=event["detection_year"],
            detection_month=event["detection_month"],
            detection_day_of_week=event["detection_day_of_week"],
            detection_hour=event["detection_hour"],
            
            confidence_level_label=event["confidence_level_label"],
            detection_period=event["detection_period"],
            satellite_label=event["satellite_label"],
            collection_version=event["collection_version"],
            processing_type=event["processing_type"]
        )