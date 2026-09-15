from dataclasses import dataclass
from datetime import date, datetime, time


@dataclass
class FirmsNrtDetection:
    event_id: str
    source: str
    source_table: str
    schema_version: str
    published_timestamp: datetime
    ingested_timestamp: datetime

    latitude: float
    longitude: float
    brightness: float
    scan: float
    track: float

    acquisition_date: date
    acquisition_time: time

    satellite: str
    instrument: str
    confidence: str
    version: str

    brightness_temperature_t31: float
    fire_radiative_power: float
    daynight: str

    detection_datetime: datetime
    detection_timestamp: datetime
    detection_year: int
    detection_month: int
    detection_day_of_week: int
    detection_hour: int

    confidence_level_label: str
    detection_period: str
    satellite_label: str
    collection_version: str
    processing_type: str
