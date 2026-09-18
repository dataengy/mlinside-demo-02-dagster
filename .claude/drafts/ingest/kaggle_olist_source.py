"""dlt-источник ``kaggle_olist``: анонимная загрузка Olist с Kaggle через
kagglehub, детерминированное связное сэмплирование, запись в DuckDB.

Черновик для демо 1 v2 (PROMPT.md, «Демо 1 — инициализация проекта и ingest»,
блок про dlt). В финальном проекте это будет ``src/olist_ml/defs/ingest/loads.py``
(объекты-синглтоны для DltLoadCollectionComponent, см. defs.yaml рядом) плюс
источник модуля — здесь оба слились в один файл, т.к. в лейне нет settings.py.

Что показывает лекции: тот же трюк, что и dags/dlt_source.py в
mlinside-dagster-demo — write_disposition="replace" даёт идемпотентность без
ручного create-or-replace, схема выводится из данных. Разница — источник не
читает локальные CSV, а сам качает их с Kaggle через kagglehub (публичный
датасет, анонимно, без интерактивного логина).

dtype=str, encoding="utf-8-sig" — обоснование:
  * dtype=str — как в scripts/make_sample.py (hw-olist): на raw-слое важны
    СВЯЗИ (order_id, customer_id, zip-префиксы), а не типы. Типизация зип-кодов
    ("01001") или id как int съедает ведущие нули и создаёт риск молчаливого
    рассинхрона с dbt-staging, которая и так делает явные касты. Компромисс:
    dlt не выводит числовые/дата-типы в целевых таблицах DuckDB (все VARCHAR) —
    это сознательный выбор "raw = как есть", а не автоматический побочный эффект.
  * encoding="utf-8-sig" — реальный файл product_category_name_translation.csv
    начинается с UTF-8 BOM (проверено на скачанном датасете: первая колонка
    без этого параметра читается как "﻿product_category_name", а не
    "product_category_name", что тихо ломает будущий join в dbt-staging).
    utf-8-sig корректно читает и файлы без BOM, поэтому применён ко всем 9
    ресурсам одинаково — не только к translation-файлу.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import dlt
import pandas as pd

from sampling import sample_connected

DATASET_SLUG_DEFAULT = "olistbr/brazilian-ecommerce"

# В реальном проекте эти три идут из settings.py (pydantic-settings, .env) —
# здесь, за неимением settings.py в лейне, дефолты читаются из env напрямую.
# Это ровно то, что "значение из env/settings при импорте" в задаче на defs.yaml:
# компонент вызывает kaggle_olist_source() без run-time config, поэтому все
# параметры обязаны иметь разумный дефолт уже на этапе импорта модуля.
SAMPLE_FRAC_DEFAULT = float(os.environ.get("KAGGLE_OLIST_SAMPLE_FRAC", "0.1"))
SEED_DEFAULT = int(os.environ.get("KAGGLE_OLIST_SEED", "42"))
CACHE_DIR_DEFAULT = os.environ.get("KAGGLEHUB_CACHE")  # None -> kagglehub сам решит (~/.cache/kagglehub)

# короткое имя ресурса (= будущий "table" в raw/<table>) -> имя CSV в архиве kagglehub
CSV_BY_RESOURCE: dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def _download_dir(dataset: str, cache_dir: str | None) -> Path:
    """Скачать (или взять из кеша) датасет через kagglehub, вернуть каталог с CSV.

    Импорт kagglehub — внутри функции: extra-зависимость, не тянем её, если
    источник просто импортируют ради sample_connected/CSV_BY_RESOURCE (напр.
    в тестах). Тот же приём, что в scripts/download_data.py (hw-olist).
    """
    import kagglehub

    if cache_dir:
        os.environ["KAGGLEHUB_CACHE"] = cache_dir
    return Path(kagglehub.dataset_download(dataset))


def _read_all(csv_dir: Path) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for resource_name, csv_name in CSV_BY_RESOURCE.items():
        tables[resource_name] = pd.read_csv(
            csv_dir / csv_name,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    return tables


@dlt.source(name="kaggle_olist")
def kaggle_olist_source(
    sample_frac: float = SAMPLE_FRAC_DEFAULT,
    seed: int = SEED_DEFAULT,
    dataset: str = DATASET_SLUG_DEFAULT,
    cache_dir: str | None = CACHE_DIR_DEFAULT,
):
    """Anonymous Kaggle download -> connected sample -> один @dlt.resource на таблицу.

    Параметры и как они попадают внутрь (важно для defs.yaml/REPORT.md):
      - В @dlt_assets-варианте (Python-декоратор) их можно прокинуть через
        dg.Config на runtime — см. dags/integrations_python/ingest.py в
        mlinside-dagster-demo, где dlt.run(dlt_source=raw_csv_source()) вызывает
        source ЗАНОВО на каждый запуск.
      - В DltLoadCollectionComponent (defs.yaml) такой возможности нет: YAML
        ссылается на уже созданный объект модуля (`.loads.<obj>` в loads.py),
        собранный ОДИН РАЗ при импорте defs.yaml. Поэтому sample_frac/seed там
        могут прийти только из значений по умолчанию (env/settings) на момент
        импорта — не из run config. Подтверждено по docs.dagster.io (Context7,
        см. REPORT.md, "Компонент vs @dlt_assets").

    ВАЖНО про "известную ловушку" (см. FACTS в задаче и dags/translator.py /
    ingest.py в mlinside-dagster-demo): dagster-dlt после первого extract
    оставляет в схеме объекта DltSource технические колонки без имени, поэтому
    ПОВТОРНОЕ использование ОДНОГО И ТОГО ЖЕ вызова source(...) для второго
    pipeline.run() в одном процессе ломается. В @dlt_assets-варианте это чинится
    явно (fresh `raw_csv_source()` внутри тела ассета на каждый run). В
    DltLoadCollectionComponent такого явного "пересоздания на каждый run" в
    документации не описано — источник в loads.py создаётся один раз при
    старте процесса Dagster. Риск актуален для долгоживущего процесса (daemon/
    webserver), где один и тот же материализованный ассет будет запущен
    повторно БЕЗ перезапуска процесса — под вопросом до практической проверки
    (см. REPORT.md, риски).
    """
    csv_dir = _download_dir(dataset, cache_dir)
    raw_tables = _read_all(csv_dir)
    # Посчитано РОВНО ОДИН РАЗ на весь source(): общее множество order_id для
    # всех ресурсов. Замыкается (closure) в каждый @dlt.resource ниже — поэтому
    # все 9 таблиц получают согласованный срез, а не независимый сэмпл каждая.
    sampled = sample_connected(raw_tables, sample_frac=sample_frac, seed=seed)

    def _make_resource(name: str):
        @dlt.resource(name=name, write_disposition="replace")
        def _resource() -> Iterator[pd.DataFrame]:
            yield sampled[name]

        # __doc__ до вызова: dagster-dlt translator (см. dags/translator.py в
        # mlinside-dagster-demo) читает docstring ресурса как описание ассета.
        _resource.__name__ = name
        _resource.__doc__ = f"Сырьё Olist: {name} (сэмпл, sample_frac={sample_frac}, seed={seed})."
        # ВАЖНО: возвращаем ВЫЗВАННЫЙ ресурс (DltResource), а не декорированную
        # функцию — как raw_orders() в dags/dlt_source.py, а не raw_orders.
        return _resource()

    return [_make_resource(name) for name in CSV_BY_RESOURCE]


def pipeline(
    duckdb_path: str,
    dataset_name: str = "olist_raw",
    pipelines_dir: str | None = None,
) -> dlt.Pipeline:
    """dlt.pipeline factory.

    dataset_name по умолчанию "olist_raw" — под эту схему должен быть заточен
    dbt sources.yml (FACTS: "схема должна совпасть с тем, что ждёт dbt").
    pipelines_dir по умолчанию рядом с duckdb_path/.dlt-pipelines, а не в
    домашнем ~/.dlt — черновик не должен писать вне LANE/$TMPDIR; в финальном
    проекте это будет settings.DLT_DATA_DIR (см. dags/dlt_source.py, PIPELINES_DIR).
    """
    if pipelines_dir is None:
        pipelines_dir = str(Path(duckdb_path).parent / ".dlt-pipelines")
    Path(pipelines_dir).mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="kaggle_olist",
        destination=dlt.destinations.duckdb(duckdb_path),
        dataset_name=dataset_name,
        pipelines_dir=pipelines_dir,
        progress=None,
    )
