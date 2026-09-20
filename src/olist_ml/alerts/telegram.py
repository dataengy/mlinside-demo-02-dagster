"""Telegram-алерт (ADR-18): форматирование текста и отправка одним `httpx.post`.

Чистые функции без Dagster: `format_run_failure` — текст сообщения, `send_telegram` — отправка
(dry-run, если алерты выключены или нет токена/чата). Транспорт httpx подменяется в тестах.

    uv run python -m olist_ml.alerts.telegram "проверка канала"   # живая отправка, если .env заполнен
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

import httpx

TELEGRAM_API = "https://api.telegram.org"
MAX_ERROR_CHARS = 400


@dataclass(frozen=True)
class SendResult:
    sent: bool
    dry_run: bool
    text: str
    status_code: int | None = None
    detail: str = ""


def format_run_failure(job_name: str, run_id: str, error: str | None, ui_url: str) -> str:
    """Короткое сообщение: что упало, где посмотреть, первая строка ошибки."""
    first = (error or "").strip().splitlines()
    head = first[0][:MAX_ERROR_CHARS] if first else "(текст ошибки недоступен)"
    return f"❌ Dagster: run {job_name} завершился с ошибкой\nrun: {ui_url}/runs/{run_id}\n{head}"


def send_telegram(
    text: str,
    token: str,
    chat_id: str,
    enabled: bool,
    client: httpx.Client | None = None,
) -> SendResult:
    """POST /bot<token>/sendMessage. Выключено или нет реквизитов → dry-run (ничего не шлём)."""
    if not enabled or not token or not chat_id:
        reason = "ALERTS_ENABLED=false" if not enabled else "пустые TG_BOT_TOKEN/TG_CHAT_ID"
        return SendResult(sent=False, dry_run=True, text=text, detail=reason)
    own_client = client is None
    client = client or httpx.Client(timeout=10.0)
    try:
        r = client.post(
            f"{TELEGRAM_API}/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        )
        ok = r.status_code == 200 and r.json().get("ok", False)
        return SendResult(
            sent=ok, dry_run=False, text=text, status_code=r.status_code, detail=r.text[:200]
        )
    finally:
        if own_client:
            client.close()


def main(argv: list[str]) -> int:
    from olist_ml.settings import settings

    text = " ".join(argv) or "olist_ml: проверка Telegram-канала"
    res = send_telegram(text, settings.TG_BOT_TOKEN, settings.TG_CHAT_ID, settings.ALERTS_ENABLED)
    print(res)
    return 0 if (res.sent or res.dry_run) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
