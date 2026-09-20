"""Паритет дефолтов `Settings` ↔ `.env.example` (ADR-09).

`.env.example` — источник правды для человека (`just env`), `settings.py` дублирует те же дефолты в
Python, чтобы код работал и без `.env`. Расхождение тихо разводит Justfile и Python по разным
портам/путям — эта проверка ловит его механически.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = ROOT / ".env.example"

# computed_field — их специально нет в .env.example; проверяются отдельными тестами ниже.
COMPUTED_ONLY = {
    "DAGSTER_UI_URL",
    "DAGSTER_GRAPHQL_URL",
    "MLFLOW_UI_URL",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_MODEL_URI",
    "DUCKDB_CATALOG",
}
# Секреты в шаблоне обязаны быть пустыми.
SECRET_KEYS = {"TG_BOT_TOKEN", "TG_CHAT_ID"}
# Переменные, которые чистим перед построением Settings, чтобы shell/CI не подменили дефолты.
ENV_PREFIXES = (
    "DAGSTER_",
    "DATA_DIR",
    "DUCKDB_",
    "RAW_SCHEMA",
    "DBT_",
    "SNAPSHOT_",
    "FIXTURES_",
    "RANDOM_STATE",
    "ML_",
    "SCORING_",
    "MLFLOW_",
    "FRESHNESS_",
    "ALERTS_",
    "TG_",
)


def _parse_env_example(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith(ENV_PREFIXES):
            monkeypatch.delenv(name, raising=False)


def _settings(**overrides: str):
    from olist_ml.settings import Settings

    return Settings(_env_file=None, **overrides)  # type: ignore[arg-type]


def _values_match(actual: object, raw: str) -> bool:
    from olist_ml.settings import PROJECT_ROOT

    if isinstance(actual, Path):
        return actual == ((PROJECT_ROOT / raw).resolve() if raw else PROJECT_ROOT)
    if isinstance(actual, bool):
        return raw.strip().lower() == str(actual).lower()
    if isinstance(actual, int | float):
        try:
            return float(raw) == float(actual)
        except ValueError:
            return False
    return raw == str(actual)


def test_env_example_has_no_duplicate_keys() -> None:
    keys = [
        line.split("=", 1)[0].strip()
        for line in ENV_EXAMPLE.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#") and "=" in line
    ]
    assert len(keys) == len(set(keys)), "в .env.example есть повторяющиеся ключи"


def test_secret_keys_are_blank_in_env_example() -> None:
    example = _parse_env_example(ENV_EXAMPLE)
    for key in SECRET_KEYS:
        assert key in example
        assert example[key] == "", f"{key}: секрет не должен иметь значение в шаблоне"


def test_settings_defaults_match_env_example(clean_env: None) -> None:
    example = _parse_env_example(ENV_EXAMPLE)
    settings = _settings()
    mismatches: dict[str, tuple[str, str]] = {}
    for key, raw in example.items():
        if key in COMPUTED_ONLY or key == "MLFLOW_TRACKING_URI":
            continue
        if not hasattr(settings, key):
            mismatches[key] = (raw, "<нет такого поля в Settings>")
            continue
        actual = getattr(settings, key)
        if not _values_match(actual, raw):
            mismatches[key] = (raw, str(actual))
    assert not mismatches, "расхождение .env.example ↔ Settings:\n" + "\n".join(
        f"  {k}: .env.example={t!r} settings={s!r}" for k, (t, s) in mismatches.items()
    )


def test_every_settings_field_is_documented_in_env_example(clean_env: None) -> None:
    settings = _settings()
    example = _parse_env_example(ENV_EXAMPLE)
    raw_fields = set(type(settings).model_fields) - {"MLFLOW_TRACKING_URI_OVERRIDE"}
    undocumented = raw_fields - set(example)
    assert not undocumented, f"поля Settings без строки в .env.example: {sorted(undocumented)}"


def test_settings_override_via_env(clean_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAGSTER_PORT", "4000")
    monkeypatch.setenv("DAGSTER_HOST", "0.0.0.0")
    settings = _settings()
    assert settings.DAGSTER_PORT == 4000
    assert settings.DAGSTER_UI_URL == "http://0.0.0.0:4000"
    assert settings.DAGSTER_GRAPHQL_URL == "http://0.0.0.0:4000/graphql"


def test_relative_path_override_is_anchored_to_project_root(
    clean_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from olist_ml.settings import PROJECT_ROOT

    monkeypatch.setenv("DUCKDB_PATH", "data/custom.duckdb")
    settings = _settings()
    assert (PROJECT_ROOT / "data" / "custom.duckdb").resolve() == settings.DUCKDB_PATH


def test_duckdb_catalog_is_file_stem(clean_env: None) -> None:
    settings = _settings()
    assert settings.DUCKDB_CATALOG == "olist"


def test_mlflow_tracking_uri_defaults_to_sqlite(clean_env: None) -> None:
    settings = _settings()
    assert f"sqlite:///{settings.MLFLOW_DB_PATH.as_posix()}" == settings.MLFLOW_TRACKING_URI


def test_mlflow_tracking_uri_override_wins(
    clean_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "postgresql://u:p@h/db")
    assert _settings().MLFLOW_TRACKING_URI == "postgresql://u:p@h/db"


def test_model_uri_uses_alias(clean_env: None) -> None:
    settings = _settings()
    assert f"models:/{settings.MLFLOW_MODEL_NAME}@champion" == settings.MLFLOW_MODEL_URI
