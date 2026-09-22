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


def format_check_failure(
    check_name: str,
    asset_key: str,
    severity: str,
    run_id: str,
    description: str | None,
    ui_url: str,
) -> str:
    """Провал asset check при зелёном run: что за check, на каком ассете, severity, где посмотреть."""
    tail = (description or "").strip().splitlines()
    head = tail[0][:MAX_ERROR_CHARS] if tail else ""
    lines = [
        f"⚠️ Dagster: check {check_name} на {asset_key} провален (severity {severity}), run зелёный",
        f"run: {ui_url}/runs/{run_id}",
    ]
    if head:
        lines.append(head)
    return "\n".join(lines)


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


def discover_chats(token: str, client: httpx.Client | None = None) -> list[tuple[str, str, str]]:
    """getUpdates → уникальные (chat_id, type, title): бот должен быть в группе и видеть сообщение."""
    if not token:
        return []
    own_client = client is None
    client = client or httpx.Client(timeout=10.0)
    try:
        r = client.get(f"{TELEGRAM_API}/bot{token}/getUpdates")
        seen: dict[str, tuple[str, str, str]] = {}
        for upd in r.json().get("result", []):
            msg = upd.get("message") or upd.get("my_chat_member") or upd.get("channel_post") or {}
            chat = msg.get("chat") or {}
            if chat.get("id") is not None:
                cid = str(chat["id"])
                seen[cid] = (
                    cid,
                    chat.get("type", ""),
                    chat.get("title") or chat.get("username") or "",
                )
        return list(seen.values())
    finally:
        if own_client:
            client.close()


def main(argv: list[str]) -> int:
    from olist_ml.settings import settings

    if argv[:1] == ["--chat-id"]:
        chats = discover_chats(settings.TG_BOT_TOKEN)
        if not chats:
            print(
                "чатов не видно: добавьте бота в группу и отправьте в ней сообщение, затем повторите"
            )
            return 1
        for cid, kind, title in chats:
            print(f"TG_CHAT_ID={cid}    # {kind} {title}")
        return 0
    text = " ".join(argv) or "olist_ml: проверка Telegram-канала"
    res = send_telegram(text, settings.TG_BOT_TOKEN, settings.TG_CHAT_ID, settings.ALERTS_ENABLED)
    print(res)
    return 0 if (res.sent or res.dry_run) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
