"""Именованные выборки ассетов — джобы сюжета (ADR-04 §5.4). Порядок внутри выводится из графа."""

import dagster as dg

RAW = dg.AssetSelection.groups("raw")

# Подготовка «до» (ADR-03): raw-snapshot → схема raw. На M4 сюда добавится baseline-версия модели.
demo_prepare_job = dg.define_asset_job(
    "demo_prepare_job",
    selection=RAW,
    description=(
        "Подготовка окружения: dbt seed snapshot Olist → raw.*. Не обучает, не делает promotion."
    ),
)


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(jobs=[demo_prepare_job])
