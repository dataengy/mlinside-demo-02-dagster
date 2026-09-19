# Решения (ADR) — первая реализация (MVP)

> Формат: контекст → решение → последствия, 5–10 строк на запись. Полная аргументация — в
> [`.claude/PLAN.md`](../.claude/PLAN.md) (ссылки вида «PLAN D6» = раздел 1, решение D6).
> Целевое состояние проекта и контуры обучения/инференса — [`overview.md`](overview.md).
> Пошаговый план — [`.claude/TODO.md`](../.claude/TODO.md). Статус: **принято 2026-09-20**, ждёт реализации.

## Что такое MVP

Сквозной пайплайн **Kaggle → DuckDB `raw` → dbt (staging → intermediate → `mart_order_features`) →
обучение → оценка с gate → реестр MLflow**, воспроизводимый из чистого клона командой
`make install && make dev`, с зелёным `make test-all`, GitHub Actions и `docker compose --profile core up`.
Всё, что не нужно для этой цепочки, отложено (см. таблицу в конце и «После MVP» в TODO).

---

## ADR-01. Отдельный репозиторий на демо

**Контекст.** Изначально демо жило в `mlinside-hw-olist/demo/02-dagster`; монорепо `mlinside-demo` давало цикл
сабмодулей hw-olist → demo → hw-olist (PLAN D9, D14).
**Решение.** Демо вынесено в [dataengy/mlinside-demo-02-dagster](https://github.com/dataengy/mlinside-demo-02-dagster)
(история сохранена через `git subtree split`) и подключено в `mlinside-hw-olist` сабмодулем `demo/02-dagster`.
**Последствия.** Все команды — из корня этого репо; CI и issues — здесь; `MLInside-course` подключает ровно
нужные демо. Ловушка вложенных сабмодулей — `git submodule update --init` **без** `--recursive`.

## ADR-02. Раскладка `create-dagster` + YAML-компоненты; python-fallback отложен

**Контекст.** ТЗ требует строго дефолтную структуру `create-dagster project` и компоненты `defs.yaml` как
основной способ интеграций; python-декораторы — только где компонент не покрывает (PLAN D1).
**Решение.** MVP использует только `dg.load_from_defs_folder(...)` из scaffold-`definitions.py`;
интеграции — `defs/ingest/defs.yaml` (`DltLoadCollectionComponent`), `defs/dbt/defs.yaml`
(`DbtProjectComponent`); ML, jobs, maintenance — обычные python-модули внутри `defs/`, которые
автозагрузка подхватывает без правок `definitions.py`.
**Последствия.** Переключатель `INTEGRATIONS_STYLE=yaml|python` и пакет `integrations_python/`
(PLAN D1) — после MVP; `definitions.py` остаётся нетронутым, как требует ТЗ. Если компонент dlt
не даст нужного поведения (см. ADR-03, риск «протухшего source»), fallback — `@dlt_assets` в
`defs/ingest/assets.py`, и это будет первым отклонением.

## ADR-03. Ingest = dlt из Kaggle (анонимно) с сэмплированием по `order_id`; `data/raw` как кеш

**Контекст.** Два кандидата: v1 `dbt seed` из закоммиченных CSV и v2 dlt из Kaggle (PLAN D2, D11).
Пользователь выбрал v2 для MVP (2026-09-20): очистка raw должна **реально перезагружать данные из Kaggle**.
Лейн-разведка подтвердила: `kagglehub` качает `olistbr/brazilian-ecommerce` без учётки (16 с холодный,
0.6 с из кеша), `product_category_name_translation.csv` содержит BOM.
**Решение.**
- `@dlt.source kaggle_olist(sample_frac, seed, geo_max_rows, source_dir)`: если в `source_dir`
  (`DATA_DIR/raw`) уже лежат 9 CSV — читает их; иначе `kagglehub.dataset_download` → копирует CSV в
  `source_dir`. Все CSV читаются `dtype=str`, `encoding="utf-8-sig"`; касты — в dbt staging.
- Сэмпл **до** загрузки: `sample_connected(tables, sample_frac, seed, geo_max_rows)` — одна выборка заказов,
  остальные таблицы подтягиваются по ключам (`orders → order_items/payments/reviews → customers/products/sellers`,
  `geolocation` — по zip-префиксам, ограничен `GEO_MAX_ROWS`).
- 8 ресурсов `write_disposition="replace"` → DuckDB схема `raw`, ключи ассетов `raw/<table>`, группа `raw`.
- `SAMPLE_FRAC` только из `settings` (компонент строит source при импорте); run-time конфиг сэмпла — после MVP.
- Тесты: тот же source с `source_dir=tests/fixtures` — **без сети**; `kagglehub` в CI не вызывается.
**Последствия.** Демо 1 показывает «настоящий» ingest; `make clean-raw` удаляет схему `raw`, `data/raw/`
и кеш `kagglehub` → следующий запуск качает заново. dbt-seed-вариант (v1) — после MVP, если понадобится
для слайда. Служебные `_dlt_*` таблицы в `sources.yml` не заносятся.

## ADR-04. dbt — копия канона, в графе только `+mart_order_features`

**Контекст.** Канонический dbt-проект `mlinside-hw-olist/dbt`: 8 sources, 8 staging, 3 intermediate,
6 marts, 2 справочных seed. Submodule даёт цикл, symlink не переживает клон (PLAN D9). Для демо нужен
минимум узлов, но переписывать модели нельзя.
**Решение.** `scripts/sync_dbt_from_canonical.sh` (rsync + наложение разрешённых отличий, идемпотентно) →
`dbt/`. Разрешённые отличия: `profiles.yml` (только `duck`), `sources.yml` (см. ADR-05),
`macros/generate_schema_name.sql` (схемы без префикса `main_`), `macros/cross_db/date_parts.sql`
(+`month_of`), новая модель `marts/mart_order_features.sql` + её yml. Компонент
`defs/dbt/defs.yaml`: `select: "+mart_order_features"` — в граф попадают 8 stg + 3 int + 1 mart
(+ справочные seeds), остальные 5 marts остаются в копии, но не в графе. `cli_args: [build]`
→ `dbt_build_job` = модели как ассеты + dbt-тесты как asset checks.
**Последствия.** Smoke-тест `test_dbt_copy_in_sync` проверяет, что отличий от канона ровно столько;
`mart_order_features` + `month_of()` — кандидаты на PR в канон. Раздельные `transform_job`/`dq_job`
и freshness (PLAN D3, D4) — после MVP.

## ADR-05. Сшивка графа: ключ dbt-source = ключ ingest-ассета

**Контекст.** Граф должен быть одним связным компонентом от Kaggle до `model_registered` (ТЗ, п. 1).
**Решение.** В `sources.yml` копии у каждой таблицы `meta: {dagster: {asset_key: [raw, <table>]}}`,
`database: olist`, `schema: raw`, без `external_location`; dlt-ресурсы называются как таблицы
source (`orders`, `order_items`, …) и транслируются в `raw/{{ resource.name }}`.
**Последствия.** Переход на любой другой ingest (seed, локальные CSV) не меняет `sources.yml`.
Проверяется smoke-тестом `test_graph_connected` (BFS от `raw/orders` достигает все ключи).

## ADR-06. ML-задача, витрина без утечек, хеш-сплит, gate от минимума по сидам на сэмпле

**Контекст.** PLAN D6 и [`REPORT.md`](../.claude/drafts/ml/REPORT.md) лейна ML: «заказ доставлен с
опозданием», 6.8 % положительных; признаки после доставки дают утечку (AUC → 1.0); HGB на 20 % — ROC AUC 0.78
(мин. по сидам 0.726), честный time-split — 0.71.
**Решение.**
- Витрина `mart_order_features`: одна строка на доставленный заказ, 22 признака, известных на момент покупки;
  запрещены `delivery_*`, `review_*`, `order_status`, все `mart_*`-агрегаты.
- Модель по умолчанию `HistGradientBoostingClassifier` (параметры из PLAN D6), `ML_MODEL=logreg` — второй
  вариант, удобен, чтобы честно уронить gate на демо.
- Сплит — в Python по `md5(order_id|split|seed)`, holdout `ML_TEST_FRAC`; колонки `split` в витрине нет.
- **MVP-сэмпл минимальный**: `SAMPLE_FRAC=0.05` (~5 тыс. заказов, обучение < 1 с). Порог
  `ML_MIN_ROC_AUC` **калибруется на этом сэмпле** в задаче M4: прогон по 5 сидам, порог = минимум − 0.02;
  стартовое значение в `.env.example` — 0.60. Неблокирующий `pr_auc_floor` — WARN.
- Наглядность — метаданные в Dagster UI (`MetadataValue.md` таблица метрик, ROC/importance как PNG data-URI,
  `MetadataValue.url` на MLflow run); ноутбук не делаем.
**Последствия.** ⚠️ Хеш-сплит завышает метрику относительно time-split — оговорка в runbook и на слайде;
`TrainConfig.split = hash | time` — после MVP (overview §6.2). `purchase_month` даёт половину качества
(память об инцидентах 2017-11, 2018-03) — это честно проговаривается.

## ADR-07. Свой `MlflowResource`; регистрация идемпотентна по `dataset_fingerprint`

**Контекст.** `dagster-mlflow` — op-ориентированный legacy (`mlflow_tracking` + хук), не ложится на ассеты
(PLAN D5). Повторный запуск не должен плодить версии в реестре (ТЗ, п. 5).
**Решение.** `MlflowResource(dg.ConfigurableResource)` — тонкая обёртка: `setup()` (tracking/registry URI,
эксперимент), `run_url()`. Backend `sqlite:///data/mlflow.db`, артефакты `data/mlruns`, UI — `make mlflow`
на порту **5001** (5000 на macOS занят AirPlay). Модель логируется `serialization_format=cloudpickle`
(обход падения skops при `dlt` в процессе). `model_registered`: тег `dataset_fingerprint =
md5(sorted order_id + params)`; совпал с существующей версией → новая не создаётся, `reused_existing=True`;
алиас `champion` переставляется на актуальную версию.
**Последствия.** MLflow-run на каждый запуск создаётся (это нормально), версия — только при изменении данных
или параметров. В compose tracking URI переключается на сервер через `MLFLOW_TRACKING_URI_OVERRIDE`.

## ADR-08. DuckDB — единственное хранилище; `in_process_executor`

**Контекст.** DuckDB допускает одного писателя на файл (PLAN D7).
**Решение.** Один файл `data/olist.duckdb` для raw/staging/intermediate/marts; `executor=in_process_executor`
объявлен один раз в `defs/executor.py`; в compose дополнительно `max_concurrent_runs: 1`.
**Последствия.** Параллелизма внутри run нет — для демо это не важно; в prod — ClickHouse + `multiprocess`
(overview §4). `DuckDBResource.connect(read_only=True)` для чтения в ML-ассетах.

## ADR-09. Никаких скаляров в коде; плоский `.env.example`

**Контекст.** Правило ТЗ: хосты, порты, пути, пороги, `sample_frac`, имена экспериментов — только через
`settings.py` (pydantic-settings). Глобальная конвенция `~/.claude/CLAUDE.md` предлагает
`config/*.template` + `.env-render.sh`.
**Решение.** `src/olist_ml/settings.py` — единственное место констант; составные значения
(`DAGSTER_UI_URL`, `MLFLOW_TRACKING_URI`, `DUCKDB_CATALOG`) — `computed_field`. `.env.example` —
плоский `KEY=value` с комментариями, секреты пустые; Makefile делает `-include .env` + `export`.
Тест `test_settings.py` проверяет, что каждый ключ `.env.example` — поле `Settings`, а дефолты совпадают.
**Последствия.** ⚠️ Отступление от глобальной конвенции `config/`: в демо один файл `.env` понятнее аудитории;
`config/`-раскладка не вводится. Правило `${VAR:-fallback}` не нарушается — литералов в шаблоне нет,
кроме дефолтов, которые и есть значения `Settings`.

## ADR-10. Три уровня очистки: `raw` / `derived` / `all`

**Контекст.** Требование пользователя: полная очистка данных из Makefile с двумя опциями — «всё кроме
raw/source» и «только raw/source, чтобы перезагрузить из Kaggle». ТЗ п. 6 требует джобы очистки в
`defs/maintenance/` (единственное законное место `@op`/`@job`).
**Решение.**
| Джоба | Make | Что удаляет |
|---|---|---|
| `clean_raw_job` | `make clean-raw` | схема `raw` в DuckDB, `data/raw/*.csv`, кеш `KAGGLEHUB_CACHE`, `data/dlt/` (state dlt) |
| `clean_derived_job` | `make clean-derived` | схемы `staging`/`intermediate`/`marts`/`seeds` в DuckDB, `dbt/target/`, эксперимент и registered model в MLflow, `data/mlruns/` |
| `clean_all_job` | `make clean-all` | `clean_raw_job` + `clean_derived_job` |
| — | `make clean` | только кеши сборки (`__pycache__`, `.pytest_cache`, `dbt/target`, `dbt/dbt_packages`) — данные не трогает |
Каждая джоба идемпотентна: на пустом состоянии — успех. Make-цели зовут `uv run dg launch --job <name>`
(если флаг отличается — `dagster job execute -m olist_ml.definitions -j <name>`).
**Последствия.** Демо «с нуля» = `make clean-all && make full`; `make clean-raw` = единственный способ
получить новый сэмпл после смены `SAMPLE_FRAC`/`RANDOM_STATE`. Отдельные `clean_dbt`/`clean_ml` — не нужны.

## ADR-11. CI = вызовы Make-целей

**Контекст.** ТЗ: только `Makefile` + `.github/workflows/ci.yml`, без логики в YAML, без Kaggle и секретов.
**Решение.** Job `ci` (`ubuntu-latest`, Python 3.12, `astral-sh/setup-uv` с кешем): `make install`,
`make check`, `make test`; job `e2e` (`needs: ci`, не на PR): `make test-e2e`. `make check` =
`dg check defs` + `ruff check` + `dbt parse`. Черновик — `.claude/drafts/ci/`.
**Последствия.** Всё, что зелёное в CI, повторяется локально `make ci`. Docker-образ в CI не собирается.

## ADR-12. Docker Compose профиль `core` на Postgres; observability — после MVP

**Контекст.** ТЗ демо 4 требует работающий compose; правило выбора SQLite/Postgres — по стабильности
двух процессов (PLAN D8): SQLite на общей volume Docker Desktop даёт `database is locked`.
**Решение.** `docker-compose.yml` профиль `core`: `postgres:16-alpine`, `dagster-webserver`, `dagster-daemon`
(один образ `olist_ml:local`, multi-stage uv), `mlflow` (`ghcr.io/mlflow/mlflow`, `MLFLOW_SERVER_ALLOWED_HOSTS`);
общая volume для `data/`. `deploy/dagster.prod.yaml`: Postgres storage, `QueuedRunCoordinator`
(`max_concurrent_runs: 1`), `--log-format json`. Основа — `.claude/drafts/observability/docker-compose.yml`
без observability-сервисов.
**Последствия.** dev и prod отличаются `dagster.yaml` + `.env`. Профиль `observability`
(Grafana/Prometheus/Loki/Alloy, экспортер, Telegram) — после MVP; compose пишется так, чтобы профиль
добавлялся, а не переписывался.

## ADR-13. Обучение и инференс — разные контуры; MVP покрывает только обучение

**Контекст.** В проде обучение (периодическое, batch) и применение модели (постоянное, batch или online)
живут раздельно и связаны через **реестр моделей**, а не через один запуск; несоответствие между ними —
*training-serving skew*. Подробно — [`overview.md` §6](overview.md#6-периодическое-обучение-и-постоянный-инференс-как-это-обычно-устроено)
(контуры A/B/C, типичные ошибки §6.6).
**Решение.** MVP реализует **контур A**: `training_dataset → model → model_evaluation → quality_gate →
model_registered` с алиасом `@champion`. Контракт с контуром B зафиксирован уже сейчас: инференс загружает
модель **только по алиасу** (`models:/<name>@champion`), никогда «последнюю версию»; `model_version` пишется
в результат. Сам контур B — партиционированный `predictions`, `prediction_monitoring`, `SIM_TODAY` —
лейн **L-H** ([PLAN §6](../.claude/PLAN.md)), после MVP.
**Последствия.** На лекции вопрос «а где инференс?» закрывается слайдом по overview §6 и пунктирной частью
графа; в коде MVP инференса нет намеренно. `@challenger` + джоба `promote`, time-split, поздние метки — кандидаты
после лекции (overview §6.7).

---

## Отложено (не в MVP)

| Тема | PLAN | Куда |
|---|---|---|
| dbt seed v1, `INGEST_MODE=seed\|dlt` | D2 | TODO «После MVP» |
| `transform_job` / `dq_job`, source freshness, `FreshnessPolicy` | D3, D4 | TODO «После MVP» |
| `INTEGRATIONS_STYLE=python`, `integrations_python/`, `test_style_parity` | D1 | TODO «После MVP» |
| Grafana-стек, экспортер, Telegram-алерты, `run_failure_sensor` | D10 | TODO «После MVP» |
| `predictions`, `prediction_monitoring`, `SIM_TODAY` | L-H | TODO «После MVP» |
| `docs/deploy/` (Hetzner VM, Dagster+) | §11 | TODO «После MVP» |
| Agentic BRD-watch | D13 | TODO «После MVP» |
| Параллельные лейны и object-lock | D12 | не нужно для MVP (один агент, последовательно) |
