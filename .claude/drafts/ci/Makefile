# Makefile — короткие цели для демо 02-dagster (лекция «Оркестрация ML-пайплайнов
# на Dagster», курс MLInside). Вся конфигурация — в .env (см. .env.example);
# здесь только вызовы uv / dg / docker compose, без логики.
#
# CI (.github/workflows/ci.yml) вызывает ЭТИ ЖЕ цели (make install/check/test) —
# отдельной логики в YAML нет.

SHELL := /bin/bash

# .env подхватывается, если уже создан (`make env`); "-" — не падать, если файла
# ещё нет. `export` без аргументов отдаёт ВСЕ переменные дочерним процессам
# (uv run, dg, docker compose) — этого просит ТЗ дословно.
-include .env
export

# --- fallback'и ОБЯЗАНЫ совпадать со значениями по умолчанию в .env.example ---
# Ни одного порта/пути литералом в целях ниже — только через эти переменные.
DAGSTER_SCHEME       ?= http
DAGSTER_HOST         ?= 127.0.0.1
DAGSTER_PORT         ?= 3111
DAGSTER_HOME         ?= .dagster_home
DUCKDB_PATH          ?= data/olist.duckdb
DBT_PROJECT_DIR      ?= dbt
DBT_PROFILES_DIR     ?= dbt
DBT_TARGET_PATH      ?= dbt/target
DBT_TARGET           ?= duck
DATA_DIR             ?= data/raw
KAGGLE_DATASET       ?= olistbr/brazilian-ecommerce
KAGGLEHUB_CACHE      ?= .kagglehub_cache
SAMPLE_FRAC          ?= 0.2
RANDOM_STATE         ?= 42
ML_GATE_ROC_AUC      ?= 0.60
MLFLOW_HOST          ?= 127.0.0.1
MLFLOW_PORT          ?= 5001
MLFLOW_DB_PATH       ?= mlflow.db
MLFLOW_ARTIFACTS_DIR ?= mlruns
MLFLOW_EXPERIMENT    ?= olist_ml
MLFLOW_MODEL_NAME    ?= olist_late_delivery
INTEGRATIONS_STYLE   ?= yaml
ALERTS_ENABLED       ?= false
FRESHNESS_WARN_MIN   ?= 10
FRESHNESS_FAIL_MIN   ?= 30
GRAFANA_PORT         ?= 3000
PROMETHEUS_PORT      ?= 9090
LOKI_PORT            ?= 3100

# MLFLOW_TRACKING_URI — составное значение; тот же default, что в settings.py
# (см. решение в REPORT.md). Если задан явно в .env (например, Postgres в
# docker) — используется он; иначе собирается из MLFLOW_DB_PATH.
MLFLOW_TRACKING_URI  ?= sqlite:///$(MLFLOW_DB_PATH)

# URL собираются из переменных, а не пишутся литералом (то же правило, что в settings.py).
DAGSTER_URL := $(DAGSTER_SCHEME)://$(DAGSTER_HOST):$(DAGSTER_PORT)
MLFLOW_URL  := http://$(MLFLOW_HOST):$(MLFLOW_PORT)
GRAFANA_URL := http://$(DAGSTER_HOST):$(GRAFANA_PORT)

# `open` есть только на macOS; на Linux — xdg-open; иначе печатаем ссылку и молчим.
OPEN := $(shell command -v open 2>/dev/null || command -v xdg-open 2>/dev/null || echo true)

.PHONY: help install env data-sample mlflow dev \
        demo1 demo2 demo3 demo4 \
        test test-e2e test-all check lint fmt dbt-parse dbt-deps \
        clean clean-ingest clean-dbt clean-ml clean-all \
        docker-build docker-up docker-up-observability docker-down docker-test

help: ## Показать список целей с описаниями
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

# --- окружение ────────────────────────────────────────────────────────────

install: ## Установить зависимости проекта (uv sync)
	uv sync

env: ## Создать .env из .env.example, если его ещё нет (не перезаписывает)
	cp -n .env.example .env

data-sample: ## Подготовить сэмпл данных для локального прогона (sample_frac/random_state — из .env)
	uv run python scripts/download_data.py --sample-frac $(SAMPLE_FRAC) --random-state $(RANDOM_STATE)

# --- интерфейсы ─────────────────────────────────────────────────────────────

mlflow: ## Поднять MLflow UI в фоне (порт и backend — из .env)
	uv run mlflow ui --host $(MLFLOW_HOST) --port $(MLFLOW_PORT) --backend-store-uri "$(MLFLOW_TRACKING_URI)" &
	@echo "MLflow UI: $(MLFLOW_URL)"

dev: ## Поднять Dagster (dg dev) на порту из .env
	uv run dg dev --port $(DAGSTER_PORT)

# --- демо-подсказки (шаги и команды — из docs/runbook.md) ───────────────────

demo1: ## Демо 1 — инициализация проекта и ingest (seed_job)
	@echo "Демо 1 — инициализация проекта и ingest. Сценарий и тайминг: docs/runbook.md#демо-1"
	@echo "  1. make dev            # если ещё не запущен"
	@echo "  2. В UI Dagster: Assets → Materialize (seed-таблицы raw/olist_*)"
	@$(OPEN) "$(DAGSTER_URL)" >/dev/null 2>&1 || echo "Откройте вручную: $(DAGSTER_URL)"

demo2: ## Демо 2 — dbt: загрузка и DQ как отдельные стадии
	@echo "Демо 2 — dbt build / раздельные transform_job и dq_job. Подробности: docs/runbook.md#демо-2"
	@echo "  1. В UI Dagster: Jobs → dbt_build_job (одной кнопкой) или transform_job → dq_job (раздельно)"
	@$(OPEN) "$(DAGSTER_URL)" >/dev/null 2>&1 || echo "Откройте вручную: $(DAGSTER_URL)"

demo3: ## Демо 3 — ML: обучение, оценка, регистрация модели в MLflow
	@echo "Демо 3 — от ingest до model_registered. Подробности: docs/runbook.md#демо-3"
	@echo "  1. В UI Dagster: Jobs → full_pipeline_job (Materialize)"
	@echo "  2. MLflow: метрики run'а и модель $(MLFLOW_MODEL_NAME) в реестре"
	@$(OPEN) "$(DAGSTER_URL)" >/dev/null 2>&1 || true
	@$(OPEN) "$(MLFLOW_URL)" >/dev/null 2>&1 || echo "Откройте вручную: $(DAGSTER_URL) и $(MLFLOW_URL)"

demo4: ## Демо 4 — CI/CD и Observability (Grafana-стек, алерты)
	@echo "Демо 4 — CI/CD и Observability. Подробности: docs/runbook.md#демо-4"
	@echo "  1. make docker-up-observability"
	@echo "  2. Сломать asset check / freshness → алерт в Telegram"
	@$(OPEN) "$(GRAFANA_URL)" >/dev/null 2>&1 || echo "Откройте вручную: $(GRAFANA_URL)"

# --- тесты ──────────────────────────────────────────────────────────────────

test: ## smoke + unit + integration (addopts по умолчанию исключает e2e, см. pyproject.toml)
	uv run pytest

test-e2e: ## Только e2e — полный пайплайн на фикстурах (дольше, отдельная стадия CI)
	uv run pytest -m e2e

test-all: test test-e2e ## Все уровни тестов подряд

# --- проверки и стиль ────────────────────────────────────────────────────────

check: ## dg check defs + ruff check + dbt parse — без сети, без поднятого UI
	uv run dg check defs
	uv run ruff check .
	$(MAKE) dbt-parse

lint: ## ruff check
	uv run ruff check .

fmt: ## ruff check --fix + ruff format
	uv run ruff check --fix .
	uv run ruff format .

dbt-deps: ## Установить пакеты dbt (dbt_packages)
	uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-parse: ## dbt parse — собрать manifest.json без обращения к БД
	uv run dbt parse --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --target-path $(DBT_TARGET_PATH)

# --- очистка ──────────────────────────────────────────────────────────────

clean: ## Удалить локальные артефакты сборки (кеши, dbt target, __pycache__) — НЕ данные
	rm -rf $(DBT_TARGET_PATH) .ruff_cache .pytest_cache
	find . -name '__pycache__' -not -path './.venv/*' -exec rm -rf {} +

# TODO-проверить (этап реализации): точный синтаксис `dg launch` для запуска
# джобы по имени. В доках Dagster встречаются два варианта одной и той же
# команды — `dg launch --job <name>` (job-execution.md, run-configuration.md,
# и именно так, в единственном числе, называется опция в CLI-референсе:
# .../dg-cli-reference#dg-launch) и `dg launch --jobs <name>` (job-execution.md,
# множественное число). Ниже — singular `--job`: так делает рабочий пример
# mlinside-dagster-demo/Justfile (`dg launch --job ingest_job` и т.д.). Сверить
# командой `uv run dg launch --help` на реальном проекте и поправить при расхождении.
clean-ingest: ## Дропнуть raw/seed-таблицы в DuckDB и кеш ingest (job clean_ingest_job)
	uv run dg launch --job clean_ingest_job

clean-dbt: ## Дропнуть схемы staging/marts и target/ (job clean_dbt_job)
	uv run dg launch --job clean_dbt_job

clean-ml: ## Удалить эксперимент и зарегистрированную модель MLflow, артефакты (job clean_ml_job)
	uv run dg launch --job clean_ml_job

clean-all: ## clean-ingest + clean-dbt + clean-ml по порядку (job clean_all_job)
	uv run dg launch --job clean_all_job

# --- Docker (демо 4 и прод-подобный стенд; docker-compose.yml — вне этого лейна) ──

docker-build: ## Собрать образ(ы) docker compose
	docker compose build

docker-up: ## Поднять профиль core (Dagster + MLflow)
	docker compose --profile core up -d

docker-up-observability: ## Поднять core + observability (Grafana/Prometheus/Loki/Alloy)
	docker compose --profile core --profile observability up -d

docker-down: ## Остановить и убрать контейнеры
	docker compose down

docker-test: ## Поднять core и прогнать full_pipeline_job внутри стенда (см. TODO у dg launch)
	docker compose --profile core up -d
	docker compose exec dagster-webserver dg launch --job full_pipeline_job
