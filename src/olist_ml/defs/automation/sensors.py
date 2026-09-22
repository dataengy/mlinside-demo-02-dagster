"""Один канал алерта (ADR-18), два сенсора — «run упал» и «run зелёный, но asset check провален».

`alert_on_run_failure` — `run_failure_sensor`: любой run FAILURE (blocking check тоже роняет run).
`alert_on_failed_check` — курсор по событиям ASSET_CHECK_EVALUATION: `passed=False` в run'е, который
завершился SUCCESS (severity warn / non-blocking check). Run FAILURE пропускается — про него уже сказал первый
сенсор, дублей нет. Без ALERTS_ENABLED оба пишут dry-run текст в лог тика.

Сенсор включён по умолчанию (`default_status=RUNNING`), чтобы в `dg dev` ничего не переключать:
демон сенсоров опрашивает раз в 30 с — в кадре после красного `dq_job` сообщение приходит в течение полуминуты.
"""

import json
import time
from datetime import UTC, datetime

import dagster as dg
from dagster import DagsterEventType, DagsterRunStatus, RunsFilter

from olist_ml.alerts.telegram import format_check_failure, format_run_failure, send_telegram
from olist_ml.settings import Settings


def alert_text(job_name: str, run_id: str, error: str | None) -> str:
    return format_run_failure(job_name, run_id, error, Settings().DAGSTER_UI_URL)


@dg.run_failure_sensor(
    name="alert_on_run_failure",
    minimum_interval_seconds=30,
    # Без этого run'ы из `dg launch`/`dagster job execute` (нет remote_job_origin: location=None)
    # не проходят фильтр «тот же code location» и сенсор молча двигает курсор мимо них.
    monitor_all_code_locations=True,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Любой упавший run → Telegram (dry-run, пока ALERTS_ENABLED=false).",
)
def alert_on_run_failure(context: dg.RunFailureSensorContext):
    s = Settings()
    run = context.dagster_run
    error = context.failure_event.message if context.failure_event else None
    text = alert_text(run.job_name, run.run_id, error)
    _log_send_result(
        context,
        send_telegram(text, s.TG_BOT_TOKEN, s.TG_CHAT_ID, s.ALERTS_ENABLED),
        text,
        s.TG_CHAT_ID,
    )


def _log_send_result(context, res, text: str, chat_id: str) -> None:
    if res.dry_run:
        context.log.warning("alert dry-run (%s):\n%s", res.detail, text)
    elif res.sent:
        context.log.info("alert отправлен в Telegram (chat %s)", chat_id)
    else:
        context.log.error("alert не доставлен: HTTP %s %s", res.status_code, res.detail)


@dg.sensor(
    name="alert_on_failed_check",
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Asset check провален, а run зелёный (warn / non-blocking) → Telegram; run FAILURE — не дублируем.",
)
def alert_on_failed_check(context: dg.SensorEvaluationContext):
    """Курсор — по run'ам (update_timestamp), а не по id событий: дефолтный SQLite-инстанс `dg dev` хранит event log
    по-run'ово (run-sharded), и `get_event_records` с числовым курсором там не работает. Берём только завершённые
    SUCCESS-run'ы, обновлённые после курсора, и читаем их события ASSET_CHECK_EVALUATION."""
    instance = context.instance
    state = _cursor_load(context.cursor)
    if state is None:  # первый тик или курсор старого формата — историю не рассылаем
        context.update_cursor(_cursor_dump(time.time(), ""))
        return dg.SkipReason("инициализация: курсор установлен на текущий момент")
    after_ts, last_run_id = state
    records = instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS],
            updated_after=datetime.fromtimestamp(after_ts, tz=UTC),
        ),
        limit=50,
        order_by="update_timestamp",
        ascending=True,
    )
    records = [r for r in records if r.dagster_run.run_id != last_run_id]
    if not records:
        return dg.SkipReason("новых зелёных run'ов нет")

    s = Settings()
    sent = 0
    for rec in records:
        run_id = rec.dagster_run.run_id
        for entry in instance.all_logs(run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION):
            evaluation = entry.dagster_event.event_specific_data
            if evaluation.passed:
                continue
            text = format_check_failure(
                evaluation.check_name,
                evaluation.asset_key.to_user_string(),
                evaluation.severity.value,
                run_id,
                evaluation.description,
                s.DAGSTER_UI_URL,
            )
            _log_send_result(
                context,
                send_telegram(text, s.TG_BOT_TOKEN, s.TG_CHAT_ID, s.ALERTS_ENABLED),
                text,
                s.TG_CHAT_ID,
            )
            sent += 1
        context.update_cursor(_cursor_dump(rec.update_timestamp.timestamp(), run_id))
    return dg.SkipReason(f"зелёных run'ов {len(records)}, алертов {sent}")


def _cursor_dump(ts: float, run_id: str) -> str:
    return json.dumps({"ts": ts, "run_id": run_id})


def _cursor_load(cursor: str | None) -> tuple[float, str] | None:
    """None — курсора нет или он не нашего формата (например, остался от прежней версии сенсора)."""
    if not cursor:
        return None
    try:
        data = json.loads(cursor)
        return float(data["ts"]), str(data.get("run_id", ""))
    except (ValueError, TypeError, KeyError):
        return None


@dg.definitions
def automation() -> dg.Definitions:
    return dg.Definitions(sensors=[alert_on_run_failure, alert_on_failed_check])
