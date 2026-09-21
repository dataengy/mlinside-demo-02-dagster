"""Telegram-алерт на мок-транспорте httpx: dry-run без реквизитов, POST в sendMessage, формат текста."""

from __future__ import annotations

import json

import httpx
import pytest

from olist_ml.alerts.telegram import TELEGRAM_API, format_run_failure, send_telegram

pytestmark = pytest.mark.unit


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_format_has_job_run_link_and_first_error_line() -> None:
    text = format_run_failure(
        "dq_job", "abc123", "Line one: unique failed\nLine two", "http://127.0.0.1:3000"
    )
    assert "dq_job" in text and "http://127.0.0.1:3000/runs/abc123" in text
    assert "Line one" in text and "Line two" not in text
    assert "недоступен" in format_run_failure("j", "r", None, "http://x")


def test_dry_run_when_disabled_or_no_credentials() -> None:
    calls: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(200, json={"ok": True})

    with _client(handler) as c:
        r1 = send_telegram("t", "tok", "42", enabled=False, client=c)
        r2 = send_telegram("t", "", "42", enabled=True, client=c)
    assert r1.dry_run and not r1.sent and "ALERTS_ENABLED" in r1.detail
    assert r2.dry_run and "пустые" in r2.detail
    assert calls == []


def test_send_posts_to_bot_api() -> None:
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    with _client(handler) as c:
        res = send_telegram("hello", "123:ABC", "-100777", enabled=True, client=c)
    assert res.sent and not res.dry_run and res.status_code == 200
    assert seen["url"] == f"{TELEGRAM_API}/bot123:ABC/sendMessage"
    assert seen["body"]["chat_id"] == "-100777" and seen["body"]["text"] == "hello"


def test_send_reports_http_error() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"ok": False, "description": "Unauthorized"})

    with _client(handler) as c:
        res = send_telegram("x", "bad", "1", enabled=True, client=c)
    assert not res.sent and res.status_code == 401 and "Unauthorized" in res.detail


def test_discover_chats_dedups_by_chat_id() -> None:
    from olist_ml.alerts.telegram import discover_chats

    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path.endswith("/getUpdates")
        return httpx.Response(
            200,
            json={
                "ok": True,
                "result": [
                    {"message": {"chat": {"id": -100777, "type": "supergroup", "title": "demo"}}},
                    {"message": {"chat": {"id": -100777, "type": "supergroup", "title": "demo"}}},
                    {"my_chat_member": {"chat": {"id": 42, "type": "private", "username": "u"}}},
                ],
            },
        )

    with _client(handler) as c:
        assert discover_chats("tok", client=c) == [
            ("-100777", "supergroup", "demo"),
            ("42", "private", "u"),
        ]
    assert discover_chats("", client=c) == []


def test_sensor_is_defined_and_running_by_default() -> None:
    import dagster as dg

    from olist_ml.definitions import defs

    sensor = defs().get_sensor_def("alert_on_run_failure")
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING
    # run'ы `dg launch` не несут remote_job_origin — без этого флага сенсор их пропускает
    assert sensor._monitor_all_code_locations is True  # noqa: SLF001
