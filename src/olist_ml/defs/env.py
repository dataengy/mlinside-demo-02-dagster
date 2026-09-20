"""Единственное место, где настройки из settings передаются dbt через окружение.

dbt-duckdb резолвит относительный `path` в profiles.yml от cwd процесса dbt (= dbt/), а `.env`
хранит DUCKDB_PATH относительно корня проекта. Здесь путь делается абсолютным при загрузке
definitions — и в `dg dev`, и в run-воркере (definitions загружаются в каждом). Justfile делает
то же для CLI-рецептов (`export DUCKDB_PATH`). Явно заданный абсолютный DUCKDB_PATH (тесты,
временные БД) не трогаем. Заодно dbt не ходит в сеть (version check → pypi, анонимная
статистика) — dev/check/CI работают офлайн (ADR-04b).
"""

import os
from pathlib import Path

from olist_ml.settings import settings

_current = os.environ.get("DUCKDB_PATH", "")
if not _current or not Path(_current).is_absolute():
    os.environ["DUCKDB_PATH"] = str(settings.DUCKDB_PATH)
Path(os.environ["DUCKDB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

os.environ.setdefault("DBT_TARGET", settings.DBT_TARGET)
os.environ.setdefault("DBT_VERSION_CHECK", "false")
os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
