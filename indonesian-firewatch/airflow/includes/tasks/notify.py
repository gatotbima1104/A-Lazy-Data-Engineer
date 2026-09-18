from __future__ import annotations

import html
import logging

import requests
from airflow.sdk import Context

from includes.constant import TELEGRAM_API_URL
from utils.constant import TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000

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
    
    dag = context["dag"]
    task_instance = context["task_instance"]
    exception = context.get("exception")
    run_id = context["run_id"]
    
    dag_id = dag.dag_id
    task_id = task_instance.task_id

    error_text = (
        str(exception).strip()
        if exception
        else "No exception details available."
    )

    if len(error_text) > MAX_ERROR_LENGTH:
        error_text = error_text[:MAX_ERROR_LENGTH] + "..."

    error_text = html.escape(error_text)

    text = (
        "🔥 <b>PIPELINE FAILED</b>\n\n"
        f"<b>DAG:</b> <code>{html.escape(dag_id)}</code>\n"
        f"<b>Run:</b> <code>{html.escape(run_id)}</code>\n"
        f"<b>Task:</b> <code>{html.escape(task_id)}</code>\n\n"
        f"<b>Error:</b>\n"
        f"<code>{error_text}</code>\n\n"
    )

    send_telegram_message(text)
