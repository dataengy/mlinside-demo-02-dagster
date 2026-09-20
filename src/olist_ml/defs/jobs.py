"""Именованные выборки ассетов — джобы сюжета (ADR-04 §5.4). Порядок внутри выводится из графа.

Материализация ≠ проверка: `feature_mart_job` — только модели, `dq_job` — только dbt-checks витрины
и её upstream (dagster-dbt выставляет DBT_INDIRECT_SELECTION=empty). `dbt_build_job` — «одна кнопка»
для CI и диагностики.
"""

import dagster as dg

RAW = dg.AssetSelection.groups("raw")
MART = dg.AssetSelection.keys("mart_order_features")
# Ветка витрины без raw-снапшота: справочные seeds + staging + intermediate + mart (и их checks).
DBT = MART.upstream() - RAW

demo_prepare_job = dg.define_asset_job(
    "demo_prepare_job",
    selection=RAW,
    description=(
        "Подготовка окружения: dbt seed snapshot Olist → raw.*. Не обучает, не делает promotion."
    ),
)

feature_mart_job = dg.define_asset_job(
    "feature_mart_job",
    selection=DBT.without_checks(),
    description=(
        "Собрать/обновить витрину признаков: только модели dbt до mart_order_features, без тестов."
    ),
)

dq_job = dg.define_asset_job(
    "dq_job",
    selection=DBT - DBT.without_checks(),
    description=(
        "Только проверки качества: dbt-тесты ветки витрины как asset checks, без материализации."
    ),
)

dbt_build_job = dg.define_asset_job(
    "dbt_build_job",
    selection=DBT,
    description="«Одна кнопка» dbt build: модели + тесты в одном run — для CI и диагностики.",
)


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(jobs=[demo_prepare_job, feature_mart_job, dq_job, dbt_build_job])
