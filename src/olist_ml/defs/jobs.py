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

ML = dg.AssetSelection.groups("ml")
INFERENCE = dg.AssetSelection.groups("inference")

# Подготовка «до» (ADR-03, ADR-07a A): raw-snapshot → витрина (+checks) → baseline-версия в реестре
# БЕЗ алиаса (другой сид → другой fingerprint, тег role=baseline). Promotion остаётся живым действием в кадре.
_baseline = {"random_state": 7, "role": "baseline"}
demo_prepare_job = dg.define_asset_job(
    "demo_prepare_job",
    selection=RAW | MART.upstream() | ML,
    config={"ops": {"training_dataset": {"config": _baseline}, "model": {"config": _baseline}}},
    description=(
        "Подготовка окружения вне кадра: seed snapshot → витрина + checks → baseline-версия модели "
        "без алиаса (role=baseline). Не делает promotion, не трогает champion."
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

# ADR-04a (A): blocking dbt-checks витрины идут в одном run с ML — провал контракта пропускает ML-ветку.
MART_CHECKS = MART - MART.without_checks()

train_job = dg.define_asset_job(
    "train_job",
    selection=ML | MART_CHECKS,
    description=(
        "Обучить кандидата: checks витрины → training_dataset → model → model_evaluation → "
        "quality_gate (blocking) → model_registered (версия без алиаса)."
    ),
)


score_job = dg.define_asset_job(
    "score_job",
    selection=INFERENCE,
    description="Batch inference опубликованной версией (@champion): scoring_input → predictions.",
)


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[demo_prepare_job, feature_mart_job, dq_job, dbt_build_job, train_job, score_job]
    )
