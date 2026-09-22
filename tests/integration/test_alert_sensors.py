"""Второй сенсор ADR-18: провал asset check при зелёном run → алерт; run FAILURE → без дублей; курсор двигается.

Инстанс — `DagsterInstance.local_temp` (настоящий SQLite, run-sharded event log, как в `dg dev`): ephemeral-инстанс
не ловит ошибки вида «cursor is not run-aware».
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import dagster as dg
import pytest

from olist_ml.alerts import telegram
from olist_ml.defs.automation import sensors as sensors_mod
from olist_ml.defs.automation.sensors import alert_on_failed_check

pytestmark = pytest.mark.integration


@dg.asset
def probe() -> int:
    return 1


@dg.asset_check(asset=probe, blocking=False)
def warn_check() -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=False, severity=dg.AssetCheckSeverity.WARN, description="span > 180 дней: 3 строки"
    )


@dg.asset_check(asset=probe, blocking=True)
def hard_check() -> dg.AssetCheckResult:
    return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.ERROR)


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    box: list[str] = []

    def fake_send(text: str, token: str, chat_id: str, enabled: bool, client=None):
        box.append(text)
        return telegram.SendResult(sent=False, dry_run=True, text=text, detail="test")

    monkeypatch.setattr(sensors_mod, "send_telegram", fake_send)
    return box


@pytest.fixture
def instance(tmp_path: Path) -> Iterator[dg.DagsterInstance]:
    with dg.DagsterInstance.local_temp(str(tmp_path / "home")) as inst:
        yield inst


def _tick(instance: dg.DagsterInstance, cursor: str | None):
    ctx = dg.build_sensor_context(instance=instance, cursor=cursor)
    result = alert_on_failed_check(ctx)
    return ctx.cursor, result


def test_first_tick_initializes_cursor_without_alerts(
    instance: dg.DagsterInstance, sent: list[str]
) -> None:
    assert dg.materialize([probe, warn_check], instance=instance).success
    cursor, _ = _tick(instance, None)
    assert cursor is not None and '"ts"' in cursor
    _tick(instance, cursor)
    assert sent == []  # история не рассылается


def test_warn_check_in_green_run_alerts_once(instance: dg.DagsterInstance, sent: list[str]) -> None:
    cursor, _ = _tick(instance, None)
    res = dg.materialize([probe, warn_check], instance=instance)
    assert res.success  # non-blocking WARN не роняет run
    cursor, _ = _tick(instance, cursor)
    assert len(sent) == 1
    text = sent[0]
    assert "warn_check" in text and "probe" in text and "WARN" in text
    assert f"/runs/{res.run_id}" in text and "3 строки" in text
    cursor, _ = _tick(instance, cursor)  # повторный тик — курсор ушёл, дублей нет
    assert len(sent) == 1
    res2 = dg.materialize([probe, warn_check], instance=instance)
    _tick(instance, cursor)  # новый зелёный run с тем же провалом — новый алерт
    assert len(sent) == 2 and f"/runs/{res2.run_id}" in sent[1]


def test_blocking_failure_is_left_to_run_failure_sensor(
    instance: dg.DagsterInstance, sent: list[str]
) -> None:
    cursor, _ = _tick(instance, None)
    res = dg.materialize([probe, hard_check], instance=instance, raise_on_error=False)
    assert not res.success
    cursor, _ = _tick(instance, cursor)
    assert sent == []  # про упавший run скажет alert_on_run_failure


def test_legacy_cursor_is_reinitialized(instance: dg.DagsterInstance, sent: list[str]) -> None:
    cursor, result = _tick(instance, "805")  # курсор прежней версии (storage_id)
    assert '"ts"' in cursor and "инициализация" in str(result)
    assert sent == []


def test_both_sensors_registered() -> None:
    from olist_ml.definitions import defs

    names = {s.name for s in defs().sensors}
    assert {"alert_on_run_failure", "alert_on_failed_check"} <= names
