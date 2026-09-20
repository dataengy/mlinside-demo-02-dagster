"""Template-функции для defs/dbt/defs.yaml (ADR-17): asset freshness витрины признаков.

`FreshnessPolicy.time_window` — «когда ассет последний раз материализован», не свежесть бизнес-данных.
Окна — из settings (минуты), чтобы в кадре показать деградацию статуса без ожидания часов.
"""

import datetime as dt

import dagster as dg

from olist_ml.settings import settings


@dg.template_var
def mart_freshness() -> dg.FreshnessPolicy:
    return dg.FreshnessPolicy.time_window(
        fail_window=dt.timedelta(minutes=settings.FRESHNESS_FAIL_MIN),
        warn_window=dt.timedelta(minutes=settings.FRESHNESS_WARN_MIN),
    )
