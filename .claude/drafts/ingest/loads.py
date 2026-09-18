"""Объекты-синглтоны для DltLoadCollectionComponent (см. defs.yaml рядом).

Компонент ссылается на них по относительному пути `.loads.<имя>` — поэтому
здесь именно ГОТОВЫЕ ОБЪЕКТЫ (созданные вызовом source()/pipeline() один раз
при импорте модуля), а не фабрики. Тот же приём, что и в
mlinside-dagster-demo/dags/integrations_yaml/ingest/loads.py.

sample_frac/seed берутся из kaggle_olist_source() через ЕГО собственные
env-дефолты (KAGGLE_OLIST_SAMPLE_FRAC / KAGGLE_OLIST_SEED из kaggle_olist_source.py)
— на уровне компонента run-time dg.Config недоступен (нет run config для
loads[].source), поэтому единственная точка конфигурации — момент импорта
этого модуля. См. REPORT.md, раздел "Компонент vs @dlt_assets".

OLIST_DUCKDB_PATH / OLIST_RAW_DATASET_NAME — заглушки лейна (нет settings.py
в LANE); в финальном проекте здесь будет settings.DUCKDB_PATH и
settings.INGEST_DATASET_NAME.
"""

import os

from kaggle_olist_source import kaggle_olist_source
from kaggle_olist_source import pipeline as _pipeline_factory

_DUCKDB_PATH = os.environ.get("OLIST_DUCKDB_PATH", "olist.duckdb")
_DATASET_NAME = os.environ.get("OLIST_RAW_DATASET_NAME", "olist_raw")

kaggle_olist_source_obj = kaggle_olist_source()
kaggle_olist_pipeline_obj = _pipeline_factory(duckdb_path=_DUCKDB_PATH, dataset_name=_DATASET_NAME)
