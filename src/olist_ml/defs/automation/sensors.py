"""Один канал алерта (ADR-18): run упал → сообщение в Telegram. Без ALERTS_ENABLED — dry-run в логах сенсора.

Сенсор включён по умолчанию (`default_status=RUNNING`), чтобы в `dg dev` ничего не переключать:
демон сенсоров опрашивает раз в 30 с — в кадре после красного `dq_job` сообщение приходит в течение полуминуты.
"""

import dagster as dg

from olist_ml.alerts.telegram import format_run_failure, send_telegram
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
    res = send_telegram(text, s.TG_BOT_TOKEN, s.TG_CHAT_ID, s.ALERTS_ENABLED)
    if res.dry_run:
        context.log.warning("alert dry-run (%s):\n%s", res.detail, text)
    elif res.sent:
        context.log.info("alert отправлен в Telegram (chat %s)", s.TG_CHAT_ID)
    else:
        context.log.error("alert не доставлен: HTTP %s %s", res.status_code, res.detail)


@dg.definitions
def automation() -> dg.Definitions:
    return dg.Definitions(sensors=[alert_on_run_failure])
