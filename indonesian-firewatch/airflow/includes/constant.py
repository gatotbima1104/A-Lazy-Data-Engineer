from utils.constant import TELEGRAM_BOT_TOKEN

PREFIX = {
    "archive": "archive/",
    "nrt": "nrt/",
    "reference": "reference/"
}

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
GCP_CONN_ID = "google_cloud_default"
PG_CONN_ID = "firewatch_postgres"