
import re
from pathlib import Path

from utils.constant import PROJECT_ID, SUBSCRIPTION_ID

SOURCE_TABLE = "fire_detection_nrt"
SCHEMA_VERSION = "1.0"
SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / "streaming"
    / "firms_nrt_schema.avsc"
)
DETECTION_SOURCE = "NASA_FIRMS"
SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
BQ_TABLE_STREAMING_RAW = "fire_detection_nrt_raw"

# Subscriber
CONFIDENCE_LEVEL = {
    "h": "HIGH",
    "n": "NOMINAL",
    "l": "LOW",
}

DETECTION_PERIOD = {
    "D": "DAY",
    "N": "NIGHT",
}

SATELLITE = {
    "SNPP": "SUOMI NPP",
    "N20": "NOAA-20",
    "N21": "NOAA-21",
}

VERSION_PATTERN = re.compile(
    r"^(?P<collection_version>\d+(?:\.\d+)?)(?P<processing_code>NRT|URT|RT)?$",
)

PROCESSING_TYPE = {
    "NRT": "NEAR_REAL_TIME",
    "URT": "ULTRA_REAL_TIME",
    "RT": "REAL_TIME",
}

FIRMS_NRT_SCHEMA = {
    "fields": [
        {
            "name": "event_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "source",
            "type": "STRING",
        },
        {
            "name": "source_table",
            "type": "STRING",
        },
        {
            "name": "schema_version",
            "type": "STRING",
        },
        {
            "name": "published_timestamp",
            "type": "TIMESTAMP",
        },
        {
            "name": "ingested_timestamp",
            "type": "TIMESTAMP",
        },
        {
            "name": "latitude",
            "type": "FLOAT64",
        },
        {
            "name": "longitude",
            "type": "FLOAT64",
        },
        {
            "name": "brightness",
            "type": "FLOAT64",
        },
        {
            "name": "scan",
            "type": "FLOAT64",
        },
        {
            "name": "track",
            "type": "FLOAT64",
        },
        {
            "name": "acquisition_date",
            "type": "DATE",
        },
        {
            "name": "acquisition_time",
            "type": "TIME",
        },
        {
            "name": "satellite",
            "type": "STRING",
        },
        {
            "name": "instrument",
            "type": "STRING",
        },
        {
            "name": "confidence",
            "type": "STRING",
        },
        {
            "name": "version",
            "type": "STRING",
        },
        {
            "name": "brightness_temperature_t31",
            "type": "FLOAT64",
        },
        {
            "name": "fire_radiative_power",
            "type": "FLOAT64",
        },
        {
            "name": "daynight",
            "type": "STRING",
        },
        {
            "name": "detection_datetime",
            "type": "DATETIME",
        },
        {
            "name": "detection_timestamp",
            "type": "TIMESTAMP",
        },
        {
            "name": "detection_year",
            "type": "INT64",
        },
        {
            "name": "detection_month",
            "type": "INT64",
        },
        {
            "name": "detection_day_of_week",
            "type": "INT64",
        },
        {
            "name": "detection_hour",
            "type": "INT64",
        },
        {
            "name": "confidence_level_label",
            "type": "STRING",
        },
        {
            "name": "detection_period",
            "type": "STRING",
        },
        {
            "name": "satellite_label",
            "type": "STRING",
        },
        {
            "name": "collection_version",
            "type": "STRING",
        },
        {
            "name": "processing_type",
            "type": "STRING",
        },
    ]
}