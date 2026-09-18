"""Настройки проекта: единственное место, откуда код узнаёт хосты, порты,
пути и пороги (правило «никаких скаляров в коде», см. PROMPT.md).

Дефолты полей здесь и значения в `.env.example` ОБЯЗАНЫ совпадать — это
проверяет `tests/unit/test_settings.py` (парсит `.env.example` и сравнивает
с `Settings()` на чистом окружении). `.env.example` — источник правды для
человека (`make env` копирует его в `.env`), этот модуль — для кода.

Составные значения (URL из host+port+scheme, MLFLOW_TRACKING_URI, каталог
DuckDB) собираются один раз здесь как `computed_field`, а не в местах
использования — см. «Правило: никаких скаляров в коде» в PROMPT.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Поля-пути, которые нужно сделать абсолютными относительно PROJECT_ROOT,
# если они заданы в .env относительным значением. Список используется в
# валидаторе ниже — заводить его руками для каждого поля не нужно.
_PATH_FIELDS = (
    "DAGSTER_HOME",
    "DUCKDB_PATH",
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "DBT_TARGET_PATH",
    "DATA_DIR",
    "KAGGLEHUB_CACHE",
    "MLFLOW_DB_PATH",
    "MLFLOW_ARTIFACTS_DIR",
)


def _find_project_root(start: Path | None = None) -> Path:
    """Корень проекта — ближайший вверх каталог с `pyproject.toml` и `src/olist_ml/`.

    Поиск вверх, а не `parents[N]`: при переносе модуля фиксированная глубина
    уезжает мимо корня молча, а этот способ — нет. Второе условие (наличие
    `src/olist_ml/`) важно именно в этом репозитории: `02-dagster` до отделения
    в submodule лежит внутри `mlinside-hw-olist`, у которого тоже есть
    `pyproject.toml` — без проверки пакета поиск рисковал бы остановиться не
    на том уровне (тут он бы не ошибся, `src/olist_ml/pyproject.toml` ближе,
    но проверка ничего не стоит и защищает от будущих переносов).
    """
    start = (start or Path(__file__)).resolve()
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").is_file() and (candidate / "src" / "olist_ml").is_dir():
            return candidate
    raise RuntimeError(
        f"корень проекта (pyproject.toml + src/olist_ml/) не найден вверх от {start}"
    )


PROJECT_ROOT = _find_project_root()


class Settings(BaseSettings):
    """Вся конфигурация проекта. Экземпляр — `settings` в конце модуля."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Dagster ---
    DAGSTER_SCHEME: str = "http"
    DAGSTER_HOST: str = "127.0.0.1"
    # 3111, не 3000: порт по умолчанию у Dagster часто занят другим локальным проектом.
    DAGSTER_PORT: int = 3111
    DAGSTER_HOME: Path = PROJECT_ROOT / ".dagster_home"

    # --- DuckDB / dbt ---
    DUCKDB_PATH: Path = PROJECT_ROOT / "data" / "olist.duckdb"
    DBT_PROJECT_DIR: Path = PROJECT_ROOT / "dbt"
    DBT_PROFILES_DIR: Path = PROJECT_ROOT / "dbt"
    DBT_TARGET_PATH: Path = PROJECT_ROOT / "dbt" / "target"
    DBT_TARGET: str = "duck"

    # --- данные / Kaggle (токен — только через окружение, дефолт пустой) ---
    DATA_DIR: Path = PROJECT_ROOT / "data" / "raw"
    KAGGLE_USERNAME: str = ""
    KAGGLE_KEY: str = ""
    KAGGLE_DATASET: str = "olistbr/brazilian-ecommerce"
    KAGGLEHUB_CACHE: Path = PROJECT_ROOT / ".kagglehub_cache"

    # --- сэмплирование / ML ---
    SAMPLE_FRAC: float = 0.2
    RANDOM_STATE: int = 42
    ML_GATE_ROC_AUC: float = 0.60  # TODO(ml-лейн): подтвердить порог по факту обучения

    # --- MLflow ---
    MLFLOW_HOST: str = "127.0.0.1"
    MLFLOW_PORT: int = 5001  # 5000 на macOS занят системным AirPlay Receiver
    MLFLOW_DB_PATH: Path = PROJECT_ROOT / "mlflow.db"
    MLFLOW_ARTIFACTS_DIR: Path = PROJECT_ROOT / "mlruns"
    MLFLOW_EXPERIMENT: str = "olist_ml"
    MLFLOW_MODEL_NAME: str = "olist_late_delivery"
    # Сырое значение из env — приватный алиас. Публичное имя MLFLOW_TRACKING_URI
    # занято computed_field ниже (сам URI или дефолт от MLFLOW_DB_PATH), поэтому
    # поле читает ту же переменную окружения через validation_alias, а не своё имя.
    MLFLOW_TRACKING_URI_OVERRIDE: str = Field(default="", validation_alias="MLFLOW_TRACKING_URI")

    # --- интеграции / алерты ---
    # yaml — DbtProjectComponent/DltLoadCollectionComponent (defs.yaml), основной
    # путь по решению лектора; python — @dbt_assets/@dlt_assets, для сравнения
    # (см. dags/integrations_yaml и dags/integrations_python в mlinside-dagster-demo).
    INTEGRATIONS_STYLE: Literal["yaml", "python"] = "yaml"
    ALERTS_ENABLED: bool = False
    TG_BOT_TOKEN: str = ""
    TG_CHAT_ID: str = ""

    # --- freshness (dbt source freshness / Dagster freshness checks), минуты ---
    FRESHNESS_WARN_MIN: int = 10
    FRESHNESS_FAIL_MIN: int = 30

    # --- Postgres: только demo-стенд docker compose, не прод-секреты ---
    POSTGRES_USER: str = "dagster"
    POSTGRES_PASSWORD: str = "dagster_demo"
    POSTGRES_DB: str = "dagster"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432

    # --- observability: Grafana/Prometheus/Loki (demo-значения) ---
    GRAFANA_ADMIN_USER: str = "admin"
    GRAFANA_ADMIN_PASSWORD: str = "admin"
    GRAFANA_PORT: int = 3000
    PROMETHEUS_PORT: int = 9090
    LOKI_PORT: int = 3100

    @computed_field  # type: ignore[prop-decorator]
    @property
    def DAGSTER_UI_URL(self) -> str:
        return f"{self.DAGSTER_SCHEME}://{self.DAGSTER_HOST}:{self.DAGSTER_PORT}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def DAGSTER_GRAPHQL_URL(self) -> str:
        return f"{self.DAGSTER_UI_URL}/graphql"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def MLFLOW_UI_URL(self) -> str:
        return f"http://{self.MLFLOW_HOST}:{self.MLFLOW_PORT}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def MLFLOW_TRACKING_URI(self) -> str:
        """Явный override из env, иначе SQLite рядом с MLFLOW_DB_PATH.

        Тот же дефолт продублирован в Makefile (переменная MLFLOW_TRACKING_URI
        с `?=`) — для цели `make mlflow`, которая не ходит в Python. Держать
        в согласии вручную; автоматической проверки паритета для этой пары
        (в отличие от settings.py ↔ .env.example) пока нет — см. REPORT.md.
        """
        return self.MLFLOW_TRACKING_URI_OVERRIDE or f"sqlite:///{self.MLFLOW_DB_PATH.as_posix()}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def DUCKDB_CATALOG(self) -> str:
        """Каталог DuckDB называется по имени файла: data/olist.duckdb → `olist`.

        dbt sources (models/sources/sources.yml) ссылаются на этот каталог по
        имени напрямую, не через DUCKDB_PATH — см. комментарий в .env.example.
        """
        return self.DUCKDB_PATH.stem

    @model_validator(mode="after")
    def _resolve_relative_paths(self) -> Settings:
        """Относительные пути из .env — относительно PROJECT_ROOT, не текущей cwd.

        Дефолты в этом классе уже абсолютные (PROJECT_ROOT / ...); валидатор
        нужен, только когда .env явно переопределяет путь относительным
        значением (например DUCKDB_PATH=data/custom.duckdb).
        """
        for name in _PATH_FIELDS:
            value: Path = getattr(self, name)
            if not value.is_absolute():
                setattr(self, name, (PROJECT_ROOT / value).resolve())
        return self


settings = Settings()
