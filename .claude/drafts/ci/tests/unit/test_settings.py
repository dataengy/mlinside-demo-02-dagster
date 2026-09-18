"""Паритет дефолтов `Settings` ↔ `.env.example`.

`.env.example` — источник правды для человека (`make env` копирует его в `.env`,
`Makefile` читает тот же файл через `-include`). `src/olist_ml/settings.py`
дублирует те же дефолты в Python, чтобы код работал и без отрендеренного `.env`.
Расхождение тихо разводит Makefile и Python по разным значениям порта/пути —
эта проверка ловит его механически, а не на глаз при код-ревью.

Черновик лейна CI/Makefile/packaging: тест написан ДО существования реального
пакета `olist_ml` (scaffold `create-dagster project` ещё не запускался). Как
только пакет появится и станет импортируемым (`uv sync`), тест должен пройти
без правок — если что-то не сошлось, проверить сначала расхождение имён/типов
полей между .env.example и Settings, а не сам тест.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = ROOT / ".env.example"

# Составные поля (computed_field) — их специально нет в .env.example, см.
# «Правило: никаких скаляров в коде» в PROMPT.md. Паритет для них не проверяется
# построчно, а проверяется отдельными тестами ниже (test_computed_* в конце файла).
COMPUTED_ONLY = {
    "DAGSTER_UI_URL",
    "DAGSTER_GRAPHQL_URL",
    "MLFLOW_UI_URL",
    "MLFLOW_TRACKING_URI",
    "DUCKDB_CATALOG",
}

# Секреты: в .env.example ОБЯЗАНЫ быть пустыми (см. security.yml#env_template_rules
# и ТЗ лейна). Значения задаются только в локальном, некоммиченном .env.
SECRET_KEYS = {"KAGGLE_USERNAME", "KAGGLE_KEY", "TG_BOT_TOKEN", "TG_CHAT_ID"}

# Префиксы переменных окружения, которые чистим перед построением Settings в
# тесте — иначе реальный shell/CI-окружение разработчика подменит дефолт и
# тест будет врать. Список шире, чем набор полей Settings, — с запасом.
ENV_PREFIXES = (
    "DAGSTER_",
    "DUCKDB_",
    "DBT_",
    "DATA_DIR",
    "KAGGLE",
    "SAMPLE_FRAC",
    "RANDOM_STATE",
    "ML_GATE_",
    "MLFLOW_",
    "INTEGRATIONS_STYLE",
    "ALERTS_ENABLED",
    "TG_",
    "FRESHNESS_",
    "POSTGRES_",
    "GRAFANA_",
    "PROMETHEUS_PORT",
    "LOKI_PORT",
)


def _parse_env_example(path: Path) -> dict[str, str]:
    """Плоский .env.example: `KEY=value` без `${VAR:-fallback}` — сравниваем как есть."""
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
    """Убрать из окружения теста всё, что может подменить дефолты Settings."""
    for name in list(os.environ):
        if name.startswith(ENV_PREFIXES):
            monkeypatch.delenv(name, raising=False)


def _settings(**overrides: str):
    """Settings на дефолтах класса: `_env_file=None` — не читать реальный .env."""
    from olist_ml.settings import Settings

    return Settings(_env_file=None, **overrides)  # type: ignore[arg-type]


def _values_match(actual: object, raw: str) -> bool:
    """Сравнение с учётом типа поля: Path — относительно PROJECT_ROOT, bool/число — по значению."""
    from olist_ml.settings import PROJECT_ROOT

    if isinstance(actual, Path):
        expected = (PROJECT_ROOT / raw).resolve() if raw else PROJECT_ROOT
        return actual == expected
    if isinstance(actual, bool):
        return raw.strip().lower() == str(actual).lower()
    if isinstance(actual, (int, float)):
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
    """Правило секретов: KAGGLE_*/TG_* — только пустое значение в шаблоне."""
    example = _parse_env_example(ENV_EXAMPLE)
    for key in SECRET_KEYS:
        assert key in example, f"{key}: ожидался в .env.example"
        assert example[key] == "", f"{key}: секрет не должен иметь значение по умолчанию в шаблоне"


def test_settings_defaults_match_env_example(clean_env: None) -> None:
    example = _parse_env_example(ENV_EXAMPLE)
    settings = _settings()

    mismatches: dict[str, tuple[str, str]] = {}
    for key, raw in example.items():
        if key in COMPUTED_ONLY:
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
    """Обратное направление: у каждого «сырого» (не computed) поля Settings — строка в шаблоне."""
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


def test_relative_path_override_is_anchored_to_project_root(
    clean_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from olist_ml.settings import PROJECT_ROOT

    monkeypatch.setenv("DUCKDB_PATH", "data/custom.duckdb")
    settings = _settings()
    assert (PROJECT_ROOT / "data" / "custom.duckdb").resolve() == settings.DUCKDB_PATH
    assert settings.DUCKDB_PATH.is_absolute()


def test_project_root_contains_pyproject(clean_env: None) -> None:
    from olist_ml.settings import PROJECT_ROOT

    assert (PROJECT_ROOT / "pyproject.toml").is_file()


# --- computed_field: свойства, которых нет в .env.example, проверяем отдельно ---


def test_duckdb_catalog_is_file_stem(clean_env: None) -> None:
    settings = _settings()
    assert settings.DUCKDB_CATALOG == settings.DUCKDB_PATH.stem == "olist"


def test_dagster_urls_are_composed_from_parts(clean_env: None) -> None:
    settings = _settings()
    assert (
        f"{settings.DAGSTER_SCHEME}://{settings.DAGSTER_HOST}:{settings.DAGSTER_PORT}"
    ) == settings.DAGSTER_UI_URL
    assert f"{settings.DAGSTER_UI_URL}/graphql" == settings.DAGSTER_GRAPHQL_URL


def test_mlflow_tracking_uri_defaults_to_sqlite_next_to_db(clean_env: None) -> None:
    settings = _settings()
    assert f"sqlite:///{settings.MLFLOW_DB_PATH.as_posix()}" == settings.MLFLOW_TRACKING_URI


def test_mlflow_tracking_uri_explicit_override_wins(
    clean_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "postgresql://user:pass@host/db")
    settings = _settings()
    assert settings.MLFLOW_TRACKING_URI == "postgresql://user:pass@host/db"
