from __future__ import annotations

import logging

import requests
from airflow.sdk import Context

from includes.constant import TELEGRAM_API_URL
from utils.constant import TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

def send_telegram_message(text: str) -> None:
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        logger.exception("Failed to send Telegram notification")


def on_failure_callback(context: Context) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url
    exception = context.get("exception")

    text = (
        f"🔥 <b>DAG failed</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Run: <code>{run_id}</code>\n"
        f"Error: {exception}\n"
        f"<a href=\"{log_url}\">Logs</a>"
    )
    send_telegram_message(text)


def on_success_callback(context: Context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    text = (
        f"✅ <b>DAG succeeded</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Run: <code>{run_id}</code>"
    )
    send_telegram_message(text)
