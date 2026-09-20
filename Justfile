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
# Абсолютные пути: dbt кеширует project_root в partial_parse, а Dagster запускает dbt с cwd = dbt/ —
# относительный --project-dir ломает загрузку seeds ("No files found ... dbt/seeds/raw/orders.csv").
dbt_dir := absolute_path(env("DBT_PROJECT_DIR", "dbt"))
dbt_profiles := absolute_path(env("DBT_PROFILES_DIR", "dbt"))
dbt_target_path := absolute_path(env("DBT_TARGET_PATH", "dbt/target"))

# DAGSTER_HOME всегда внутри проекта (иначе dagster пишет в ~/.dagster); абсолютный путь нужен dg/dagster.
export DAGSTER_HOME := absolute_path(dagster_home)
# dbt-duckdb резолвит относительный path от cwd (= dbt/) — отдаём dbt абсолютный путь (см. defs/env.py).
export DUCKDB_PATH := absolute_path(env("DUCKDB_PATH", "data/olist.duckdb"))
export MLFLOW_DISABLE_AGENT_HINT := "1"
export DBT_SEND_ANONYMOUS_USAGE_STATS := "false"
export DBT_VERSION_CHECK := "false"

# Показать рецепты
default:
    @just --list --unsorted

# --- окружение ---------------------------------------------------------------

# Установить зависимости (uv sync), пакеты dbt и собрать manifest — единственный рецепт, которому нужна сеть
install:
    uv sync
    @[ -d "{{dbt_dir}}" ] && just dbt-deps dbt-parse || echo "dbt/ ещё нет — dbt deps пропущен (M1)"

# Создать .env из .env.example (не перезаписывает)
env:
    cp -n .env.example .env || true

# --- запуск --------------------------------------------------------------------

# Dagster UI (dg dev) на DAGSTER_PORT; dagster.yaml копируется в DAGSTER_HOME при первом запуске;
# manifest dbt собирается заранее (prepare_if_dev выключен — ADR-04b), после правки SQL: `just dbt-parse` + reload
dev: dbt-parse
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

# dbt parse (manifest) + dg check defs + ruff — без сети и без UI
check: dbt-parse
    mkdir -p "$DAGSTER_HOME"
    uv run dg check defs
    uv run ruff check .

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

# dbt deps (сеть: hub.getdbt.com)
dbt-deps:
    uv run dbt deps --no-version-check --project-dir "{{dbt_dir}}" --profiles-dir "{{dbt_profiles}}"

# dbt parse → dbt/target/manifest.json без обращения к БД и к сети
dbt-parse:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{dbt_dir}}" ]; then echo "dbt/ ещё нет — dbt parse пропущен"; exit 0; fi
    uv run dbt parse --quiet --no-version-check --project-dir "{{dbt_dir}}" --profiles-dir "{{dbt_profiles}}"

# Копия канонического dbt-проекта + разрешённые отличия (ADR-04); CANON=<path> переопределяет источник
dbt-sync:
    bash scripts/sync_dbt_from_canonical.sh

# Snapshot raw-таблиц (SNAPSHOT_N_ORDERS) → dbt/seeds/raw и фикстуры (FIXTURES_N_ORDERS) → tests/fixtures/raw
seeds-sample *args:
    uv run python scripts/make_seeds_sample.py {{args}}

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
