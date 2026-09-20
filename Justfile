# Justfile — единственный интерфейс проекта (ADR-15): человек, CI и Docker вызывают одни и те же рецепты.
# Конфигурация — в .env (см. .env.example); здесь только вызовы uv / dg / dbt, без логики.
# `just --list` — справка. Рецепты, чьи джобы появятся на следующих шагах TODO, помечены «(M<n>)».

set dotenv-load := true
set shell := ["bash", "-euo", "pipefail", "-c"]

# Fallback'и обязаны совпадать с .env.example (нужны, пока .env не создан).
dagster_port := env("DAGSTER_PORT", "3000")
dagster_home := env("DAGSTER_HOME", ".dagster_home")
mlflow_host := env("MLFLOW_HOST", "127.0.0.1")
mlflow_port := env("MLFLOW_PORT", "5001")
mlflow_db := env("MLFLOW_DB_PATH", "data/mlflow.db")
mlflow_uri := env("MLFLOW_TRACKING_URI", "")
dbt_dir := env("DBT_PROJECT_DIR", "dbt")
dbt_profiles := env("DBT_PROFILES_DIR", "dbt")
dbt_target_path := env("DBT_TARGET_PATH", "dbt/target")

# DAGSTER_HOME всегда внутри проекта (иначе dagster пишет в ~/.dagster); абсолютный путь нужен dg/dagster.
export DAGSTER_HOME := absolute_path(dagster_home)
export MLFLOW_DISABLE_AGENT_HINT := "1"

# Показать рецепты
default:
    @just --list --unsorted

# --- окружение ---------------------------------------------------------------

# Установить зависимости (uv sync) и пакеты dbt, если dbt-проект уже есть
install:
    uv sync
    @[ -d "{{dbt_dir}}" ] && just dbt-deps || echo "dbt/ ещё нет — dbt deps пропущен (M1)"

# Создать .env из .env.example (не перезаписывает)
env:
    cp -n .env.example .env || true

# --- запуск --------------------------------------------------------------------

# Dagster UI (dg dev) на DAGSTER_PORT; dagster.yaml копируется в DAGSTER_HOME при первом запуске
dev:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "$DAGSTER_HOME"
    cp -n dagster.yaml "$DAGSTER_HOME/dagster.yaml" || true
    uv run dg dev --port "{{dagster_port}}"

# MLflow UI на MLFLOW_PORT (backend — из .env)
mlflow:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "$(dirname "{{mlflow_db}}")"
    uri="{{mlflow_uri}}"; [ -n "$uri" ] || uri="sqlite:///$(pwd)/{{mlflow_db}}"
    uv run mlflow ui --host "{{mlflow_host}}" --port "{{mlflow_port}}" --backend-store-uri "$uri"

# --- проверки и тесты ---------------------------------------------------------------

# dg check defs + ruff + dbt parse (если dbt/ есть) — без сети и без UI
check:
    mkdir -p "$DAGSTER_HOME"
    uv run dg check defs
    uv run ruff check .
    @[ -d "{{dbt_dir}}" ] && just dbt-parse || echo "dbt/ ещё нет — dbt parse пропущен (M1)"

# ruff check
lint:
    uv run ruff check .

# ruff --fix + format
fmt:
    uv run ruff check --fix .
    uv run ruff format .

# smoke + unit + integration (e2e исключён через addopts в pyproject)
test:
    uv run pytest

# только e2e
test-e2e:
    uv run pytest -m e2e

# все уровни
test-all: test test-e2e

# то же, что CI: install → check → test
ci: install check test

# --- dbt ------------------------------------------------------------------------------

# dbt deps
dbt-deps:
    uv run dbt deps --project-dir "{{dbt_dir}}" --profiles-dir "{{dbt_profiles}}"

# dbt parse → manifest.json без обращения к БД
dbt-parse:
    uv run dbt parse --project-dir "{{dbt_dir}}" --profiles-dir "{{dbt_profiles}}" --target-path "{{dbt_target_path}}"

# --- сюжет демо (джобы появляются по шагам TODO) ------------------------------------------

# Подготовка «до»: raw-snapshot, dbt deps, baseline-версия без алиаса (M1/M4)
demo-prepare:
    uv run dg launch --job demo_prepare_job

# Материализовать витрину признаков (M2)
feature-mart:
    uv run dg launch --job feature_mart_job

# Только dbt-checks витрины (M2)
dq:
    uv run dg launch --job dq_job

# Обучить кандидата → gate → версия без алиаса (M3)
train:
    uv run dg launch --job train_job

# Перевести алиас champion (M4)
promote:
    uv run dg launch --job promote_job

# Batch inference опубликованной версией (M4)
score:
    uv run dg launch --job score_job

# --- очистка ------------------------------------------------------------------------------

# Очистка данных: raw | derived | all (M5, ADR-10)
clean kind:
    uv run dg launch --job clean_{{kind}}_job

# Только кеши сборки — данные не трогает
clean-build:
    rm -rf "{{dbt_target_path}}" .ruff_cache .pytest_cache
    find . -name '__pycache__' -not -path './.venv/*' -exec rm -rf {} +
