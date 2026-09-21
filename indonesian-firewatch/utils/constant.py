import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value

PROJECT_ID = required_env("PROJECT_ID")
REGION = required_env("REGION")
BUCKET_NAME = required_env("BUCKET_NAME")
TOPIC_ID = required_env("TOPIC_ID")
SUBSCRIPTION_ID = required_env("SUBSCRIPTION_ID")
BQ_DATASET_RAW = required_env("BQ_DATASET_RAW")
BQ_DATASET_STAGING = required_env("BQ_DATASET_STAGING")
BQ_DATASET_INTERMEDIATE = required_env("BQ_DATASET_INTERMEDIATE")
BQ_DATASET_MART = required_env("BQ_DATASET_MART")
BQ_DATASET_QUARANTINE = required_env("BQ_DATASET_QUARANTINE")
BQ_DATASET_STREAMING = required_env("BQ_DATASET_STREAMING")

TELEGRAM_BOT_TOKEN = required_env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = required_env("TELEGRAM_CHAT_ID")
MAP_KEY = required_env("MAP_KEY")