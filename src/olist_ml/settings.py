"""Настройки проекта — единственное место, откуда код узнаёт хосты, порты, пути и пороги
(ADR-09 «никаких скаляров в коде»).

Дефолты полей здесь и значения в `.env.example` обязаны совпадать — это проверяет
`tests/unit/test_settings.py`. `.env.example` — источник правды для человека (`just env` копирует
его в `.env`), этот модуль — для кода. Составные значения (URL, tracking URI, каталог DuckDB)
собираются один раз здесь как `computed_field`.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Поля-пути: относительное значение из .env делается абсолютным относительно PROJECT_ROOT.
_PATH_FIELDS = (
    "DAGSTER_HOME",
    "DATA_DIR",
    "DUCKDB_PATH",
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "DBT_TARGET_PATH",
    "ML_DATA_DIR",
    "MLFLOW_DB_PATH",
    "MLFLOW_ARTIFACTS_DIR",
)


def _find_project_root(start: Path | None = None) -> Path:
    """Корень проекта — ближайший вверх каталог с `pyproject.toml` и `src/olist_ml/`."""
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
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Dagster ---
    DAGSTER_SCHEME: str = "http"
    DAGSTER_HOST: str = "127.0.0.1"
    DAGSTER_PORT: int = 3000
    DAGSTER_HOME: Path = PROJECT_ROOT / ".dagster_home"

    # --- данные / DuckDB / dbt ---
    DATA_DIR: Path = PROJECT_ROOT / "data"
    DUCKDB_PATH: Path = PROJECT_ROOT / "data" / "olist.duckdb"
    RAW_SCHEMA: str = "raw"
    DBT_PROJECT_DIR: Path = PROJECT_ROOT / "dbt"
    DBT_PROFILES_DIR: Path = PROJECT_ROOT / "dbt"
    DBT_TARGET_PATH: Path = PROJECT_ROOT / "dbt" / "target"
    DBT_TARGET: str = "duck"

    # --- snapshot / сэмплирование (ADR-03) ---
    SNAPSHOT_N_ORDERS: int = 5000
    FIXTURES_N_ORDERS: int = 500
    RANDOM_STATE: int = 42

    # --- ML (ADR-06, ADR-07, ADR-13) ---
    ML_DATA_DIR: Path = PROJECT_ROOT / "data" / "ml"
    ML_TEST_FRAC: float = 0.2
    # Порог gate измеряется на shipped snapshot на M3 (5 сидов, минимум − 0.02); 0.60 — заглушка.
    ML_MIN_ROC_AUC: float = 0.60
    ML_SCORE_THRESHOLD: float = 0.5
    # Сид baseline из demo-prepare (ADR-07a, A): отличается от RANDOM_STATE → другой fingerprint.
    ML_BASELINE_RANDOM_STATE: int = 7
    SCORING_BATCH_ID: str = "demo-batch-001"

    # --- MLflow ---
    MLFLOW_HOST: str = "127.0.0.1"
    MLFLOW_PORT: int = 5001  # 5000 на macOS занят AirPlay Receiver
    MLFLOW_DB_PATH: Path = PROJECT_ROOT / "data" / "mlflow.db"
    MLFLOW_ARTIFACTS_DIR: Path = PROJECT_ROOT / "data" / "mlruns"
    MLFLOW_EXPERIMENT: str = "olist_ml"
    MLFLOW_MODEL_NAME: str = "olist_late_delivery"
    MLFLOW_MODEL_ALIAS: str = "champion"
    # Сырое значение из env читается через validation_alias: публичное имя MLFLOW_TRACKING_URI
    # занято computed_field ниже.
    MLFLOW_TRACKING_URI_OVERRIDE: str = Field(default="", validation_alias="MLFLOW_TRACKING_URI")

    # --- freshness (ADR-17), минуты ---
    FRESHNESS_WARN_MIN: int = 10
    FRESHNESS_FAIL_MIN: int = 30

    # --- алерты (ADR-18) ---
    ALERTS_ENABLED: bool = False
    TG_BOT_TOKEN: str = ""
    TG_CHAT_ID: str = ""

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
        """Явный override из env, иначе SQLite по MLFLOW_DB_PATH."""
        return self.MLFLOW_TRACKING_URI_OVERRIDE or f"sqlite:///{self.MLFLOW_DB_PATH.as_posix()}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def MLFLOW_MODEL_URI(self) -> str:
        """`models:/<name>@<alias>` — контракт inference (ADR-13)."""
        return f"models:/{self.MLFLOW_MODEL_NAME}@{self.MLFLOW_MODEL_ALIAS}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def DUCKDB_CATALOG(self) -> str:
        """Каталог DuckDB = имя файла: data/olist.duckdb → `olist` (его ждёт sources.yml)."""
        return self.DUCKDB_PATH.stem

    @model_validator(mode="after")
    def _resolve_relative_paths(self) -> Settings:
        for name in _PATH_FIELDS:
            value: Path = getattr(self, name)
            if not value.is_absolute():
                setattr(self, name, (PROJECT_ROOT / value).resolve())
        return self


settings = Settings()
