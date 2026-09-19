# TODO — mlinside-demo-02-dagster

Пошаговый план. Решения — [`docs/decisions.md`](../docs/decisions.md) (ADR-01…ADR-13), целевое
состояние — [`docs/overview.md`](../docs/overview.md), полный план всех четырёх демо —
[`PLAN.md`](PLAN.md), постановка — [`PROMPT.md`](PROMPT.md).

**Правила для каждого шага:** TDD (failing test → код → PASS), `make check && make test` зелёные,
запуск `make dev` и проверка UI, строка в `docs/progress.md`, коммит Conventional Commits, push.
Шаги идут строго по порядку: M0 → M9. Детализация крупных — в [`tasks/`](tasks/).

## MVP

- [ ] M0 Scaffold `create-dagster`, `settings.py` + `.env.example`, ядро Makefile, `dagster.yaml`, smoke-тесты — [tasks/ghi-m0-scaffold.md](tasks/ghi-m0-scaffold.md), ADR-02 ADR-09 #scaffold #mvp
- [ ] M1 Копия dbt из канона: `sync_dbt_from_canonical.sh`, `profiles.yml`, макросы, `sources.yml` с `meta.dagster.asset_key`, модель `mart_order_features` — [tasks/ghi-m1-dbt-copy.md](tasks/ghi-m1-dbt-copy.md), ADR-04 ADR-05 #dbt #mvp
- [ ] M2 Ingest dlt из Kaggle: `sampling.py`, `kaggle_olist.py`, `loads.py`, `defs/ingest/defs.yaml`, фикстуры, `ingest_job` — [tasks/ghi-m2-ingest-dlt.md](tasks/ghi-m2-ingest-dlt.md), ADR-03 #ingest #mvp
- [ ] M3 `DbtProjectComponent` с `select +mart_order_features`, связный граф от `raw/*`, `dbt_build_job` — ADR-04 ADR-05 #dbt #mvp
- [ ] M4 ML: ассеты `training_dataset → model → model_evaluation → model_registered`, `MlflowResource`, `quality_gate`, калибровка `ML_MIN_ROC_AUC` на сэмпле, `ml_job` и `full_pipeline_job` — [tasks/ghi-m4-ml.md](tasks/ghi-m4-ml.md), ADR-06 ADR-07 ADR-13 #ml #mvp
- [ ] M5 Очистка данных: `clean_raw_job` / `clean_derived_job` / `clean_all_job`, Make-цели `clean-raw` `clean-derived` `clean-all`, тесты идемпотентности каждого этапа — ADR-10 #maintenance #mvp
- [ ] M6 CI GitHub Actions: `ci.yml` из Make-целей (`install` / `check` / `test`, e2e на main) — ADR-11 #ci #mvp
- [ ] M7 Docker: `Dockerfile` multi-stage uv, `docker-compose.yml` профиль `core` (Postgres + webserver + daemon + MLflow), `deploy/dagster.prod.yaml` — [tasks/ghi-m7-compose.md](tasks/ghi-m7-compose.md), ADR-12 #docker #mvp
- [ ] M8 Сквозные тесты: e2e `clean_all → full_pipeline → повтор`, зелёный `make test-all`, отчёт в `docs/progress.md` #tests #mvp
- [ ] M9 Финал документации: `docs/runbook.md` сверен с реальными командами, [`DEMO.md`](../DEMO.md) прогнан целиком, бейдж CI в README #docs #mvp

## После MVP

- [ ] P1 Ingest v1 `dbt seed` из сэмпл-CSV и переключатель `INGEST_MODE=seed|dlt` — PLAN D2 #ingest
- [ ] P2 Раздельные стадии `transform_job` / `dq_job` — падение dbt-теста как красный чек при зелёной модели — PLAN D3 #dbt
- [ ] P3 Source freshness: `dbt source freshness` как `multi_asset_check` + `FreshnessPolicy.time_window` — PLAN D4 #dbt
- [ ] P4 Python-стиль интеграций: `integrations_python/`, `INTEGRATIONS_STYLE`, `test_style_parity` — PLAN D1 #scaffold
- [ ] P5 Batch-инференс: `predictions` (daily partitions, загрузка по алиасу `@champion`), `prediction_monitoring`, `SIM_TODAY` — PLAN лейн L-H, [overview §6.3 и §6.5](../docs/overview.md) #ml
- [ ] P6 Observability: профиль `observability` в compose, экспортер метрик, Grafana dashboard и алерты в Telegram — PLAN D10 #observability
- [ ] P7 `run_failure_sensor` → Telegram как «уровень 0» алертинга с unit-тестом на мок HTTP — PLAN D10 #observability
- [ ] P8 `docs/deploy/`: Hetzner VM и Dagster+ (Serverless/Hybrid), сравнительная таблица, mermaid — PROMPT шаг 4 #docs
- [ ] P9 Agentic BRD-watch: хук, скилл, субагент из `.claude/drafts/agentic/` — PLAN D13 #agentic
- [ ] P10 Сенсор на `data/incoming/` или `AutomationCondition.eager()` на ML-ассетах — PLAN лейн L-D #ml
