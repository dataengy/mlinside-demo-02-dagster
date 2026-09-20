# mlinside-demo / 02-dagster — план имплементации

> **For agentic workers:** REQUIRED SUB-SKILL: `superpowers:subagent-driven-development` (по задаче — свежий субагент + ревью) или `superpowers:executing-plans`. Параллельные лейны — `superpowers:dispatching-parallel-agents`. Шаги отмечаются чекбоксами `- [ ]`.
> Статус: **v2, 2026-09-18, ждёт ревью пользователя до шага 1** (шаг 1 необратим). Учтены отчёты всех шести лейнов-разведки (git-split, ML, CI, Observability, Ingest/Kaggle, agentic); их артефакты — в `.claude/drafts/<lane>/` рядом с планом (§12).

**Goal:** демо-проект `02-dagster` на четыре демо (ingest → dbt → ML/MLflow → CI/Observability), воспроизводимый из чистого клона `dataengy/mlinside-demo` командой `make install && make data-sample && make dev`, с зелёным `make test-all` и работающим `docker compose up`.

**Architecture:** один Dagster code location `olist_ml` (scaffold `create-dagster project`); интеграции — YAML-компонентами в `defs/` (`DbtProjectComponent`, `DltLoadCollectionComponent`) с Python-fallback на те же ключи ассетов; граф склеен ключами `raw/<table>` (seed/dlt ↔ dbt sources ↔ модели ↔ `mart_order_features` ↔ ML-ассеты ↔ MLflow); DuckDB — единственное хранилище (один файл, `in_process_executor`), MLflow на SQLite; Docker Compose с профилями `core`/`observability` для демо 4.

**Tech Stack:** Python 3.12, uv, dagster 1.13.23 (+ dagster-webserver, dagster-dg-cli 1.13.23), dagster-dbt / dagster-dlt / dagster-duckdb / dagster-postgres 0.29.23, dbt-core 1.12.x (< 1.13 по требованию dagster-dbt) + dbt-duckdb 1.11.0, dlt[duckdb] 1.30.0, mlflow 3.16.1, duckdb 1.5.5, scikit-learn 1.9.1, pandas, kagglehub 1.0.2, pydantic-settings 2.15.0, httpx, pytest 8, ruff; Docker Compose, Grafana + Prometheus + Loki + Alloy.

**Spec:** `PROMPT.md` (рядом; после scaffold — `.claude/PROMPT.md`). Все решения аргументируются от него; отклонения помечены «⚠️ отклонение» и уходят в `docs/decisions.md`.

## Global Constraints (из PROMPT.md)

- Приоритеты: 1 простота, 2 надёжность, 3 современность (`dg`, `defs.yaml`, declarative automation, не legacy API), 4 выразительность в UI. Соответствие слайдам приоритетом не является.
- Структура — **строго дефолтная от `create-dagster project`**, ничего не переименовывать; `definitions.py` — см. решение D1.
- **Никаких скаляров в коде**: хосты, порты, пути, URL, схемы, таблицы, пороги, `sample_frac`, имена экспериментов — только `settings.py` (pydantic-settings, читает `.env`); составные значения собираются один раз в `settings.py`; `.env.example` полный; Makefile читает тот же `.env`; тесты берут значения из `settings` через `monkeypatch`.
- Ключ каждого ingest-ассета `raw/<table>` = ключ dbt-source через `meta.dagster.asset_key`; `dg list defs` — один связный компонент от ingest до `model_registered`.
- Компоненты `defs.yaml` — основной способ; Python-декораторы — только где компонент не покрывает, с записью в `decisions.md`.
- `@op`/`@job`-стиль — только `defs/maintenance/` (clean_* джобы) и именованные выборки ассетов (`define_asset_job`) для стадий и расписаний.
- Идемпотентность: seed/dlt `write_disposition="replace"`; dbt `table`; ML детерминирован (`RANDOM_STATE` из settings, стабильная сортировка, хеш-сплит); `test_idempotency` на каждом уровне.
- Тесты: smoke / unit / integration / e2e; `make test` = smoke+unit+integration, без Docker и без сети (Kaggle, Telegram — моки); `make test-all` зелёный до сдачи этапа.
- CI: только `Makefile` + `.github/workflows/ci.yml`, каждый шаг = вызов Make, `ubuntu-latest`, Python 3.12, кеш uv, без логики в YAML, без секретов, Kaggle в CI не дёргать.
- Git: Conventional Commits; коммит + пуш после каждого шага в `main` репозитория `mlinside-demo`; пуши в `mlinside-hw-olist` и `MLInside-course` — только после подтверждения пользователя; history-rewriting — только после подтверждения; системные пакеты не ставить без спроса.
- Останавливаться и спрашивать: перед history-rewriting git-операциями, перед push в существующие репозитории, перед установкой системных пакетов, если API Dagster расходится с ТЗ, перед откатом v2-ingest на локальные CSV, при конфликтах слияния между агентами.

---

## 0. Что выяснено при чтении четырёх источников (факты, на которых стоит план)

| Источник | Факт | Следствие |
|---|---|---|
| `mlinside-hw-olist/dbt` | 8 sources (`olist_raw.*`), 8 staging, 3 intermediate, 6 marts, 2 справочных seed, 4 singular-теста, unit-тесты; пакеты dbt_utils/dbt_date/dbt_expectations/dbt_project_evaluator (последний — только duckdb). `profiles.yml` закоммичен; таргет `duck` читает CSV через `meta.external_location` и `DATA_DIR`; каталог DuckDB называется по имени файла (`olist.duckdb` → `olist`); схемы по умолчанию `main_staging`/`main_intermediate`/`main_marts`. Freshness задан только у `orders` (`loaded_at_field: order_purchase_timestamp`, 3650/7300 дней). `meta.dagster.asset_key` нет. `int_orders_enriched` — ровно одна строка на заказ (99 441) с `is_late_delivery`, `items_cnt`, `order_value`, `freight_value`, `customer_state`, `customer_seller_distance_km` и т.д. | Витрина признаков строится только от `int_orders_enriched` + `stg_order_items` + `stg_products`; копия `sources.yml` получает `meta.dagster.asset_key` и переключается с CSV на таблицы raw-слоя; `profiles.yml` урезается до `duck`. |
| `mlinside-hw-olist/dbt` (рабочее дерево) | В индекс добавлены **полные** CSV как seeds (120 МБ, `geolocation` 58 МБ). Не закоммичено. | В `02-dagster/dbt/seeds/raw/` — **сэмпл** (~5 тыс. заказов, ≈8 МБ). Вопрос по `01-dbt` — §9. |
| `mlinside-hw-olist/demo` (лейн git-split, репетиция на временном клоне) | `git subtree split --prefix=demo` с ветки `dev` даёт **4 коммита, 123 файла, 3.16 МБ** — это **старая** структура `01-dbt/{dbt-project,dbt-empty,…}`. Новая плоская структура (50 файлов: models/seeds/macros/tests) — **только в индексе, не закоммичена** → в split не попадёт. В индексе также `01-dbt/seeds/*.csv` 120 МБ (`geolocation` 58 МБ > предупреждение GitHub 50 МБ). 117 отслеживаемых файлов удалены в рабочем дереве (`.github/`, `Makefile`, `README.md`, `docs/`, `config/`, `scripts/`, `.claude/`, старые `dbt-project/`, `dbt-empty/`). Untracked и **не игнорируется** `01-dbt/olist.duckdb` — **106 МБ, GitHub заблокирует push** при случайном `git add`. Блобов > 50 МБ в истории split нет; `git bundle --all` = 2.9 МБ. `demo/02-dagster/` не в git (PROMPT.md, `.idea`). | Предусловие «чистый status» **не выполнено**; форма «01-dbt как есть» должна быть зафиксирована коммитом пользователя до split; `*.duckdb` под `demo/` — в `.gitignore` до коммита. |
| `mlinside-dagster-demo` | Ветка `feat/dlt-ingest-and-separated-jobs` (18.09): dagster 1.13.23; стили `python`/`yaml` через `DAGSTER_INTEGRATIONS_STYLE`; `DbtProjectComponent` с `translation`; `DltLoadCollectionComponent` с `loads[].source/pipeline` как объектами модуля; `MLflowResource` на SQLite + cloudpickle (обход бага skops×dlt.hub); алиас `champion`; `build_metadata_bounds_checks`; `FreshnessPolicy.time_window`; `run_failure_sensor` → Telegram/Mattermost с dry-run; `in_process_executor` отдельным модулем; 56 тестов + e2e; `test_style_parity`; CI GitHub+GitLab на just. HANDOFF: `gh` keyring-токен невалиден, GitLab CI ждёт identity verification. | Переносим паттерны (translator → `translation` в YAML, MLflow-ресурс, алерты, executor, тесты паритета). Не переносим: just-ориентацию (в ТЗ — Makefile), Mattermost, `.run/` (используем scaffold-пути). |
| `MLInside-course` | Сабмодули уже есть: `homework/mlinside-hw-olist`, `data/code/dagster_demo` (→ `mlinside-dagster-demo`); `demo/` в корне нет; `presentations.yml` и `docs/dagster-demo-runbook.md` указывают на старый демо-репо; рабочее дерево грязное (content/*.yml, pptx-черновики); CLAUDE.md: несколько сессий в одном чекауте, ветки не переключать, коммитить по путям, `git add -A` опасен. Дека v2: три демо-слайда (017/026/042) с другой сюжетной линией — слайды пользователь правит сам. | Шаг 1.6 коммитит **только** `.gitmodules` + `demo`; репоинт `presentations.yml`/`docs/dagster-demo-runbook.md` — отдельный коммит после подтверждения. Ловушка вложенных сабмодулей (`homework/mlinside-hw-olist/demo`) — в README обоих репо, без `--recursive` по умолчанию. |
| `dbt/olist.duckdb` (лейн ML, read-only) | 99 441 заказ, доставлено 96 476, **просрочено 6 535 = 6.77 %** (по месяцам 0.7 % → 19 %); без позиций 775, без отзыва 768, без расстояния 1 265. HistGradientBoosting на 20 % сэмпле: **ROC AUC 0.782** (мин. по 5 сидам 0.726), обучение 0.6 с; логрегрессия 0.675; на 10 % худший сид 0.683. Признаки после доставки дают утечку (`delivery_delay_days` → AUC 1.0, `delivery_days` → 0.97, `review_score` → 0.83). Все шесть существующих `mart_*` содержат агрегаты по всему периоду (target encoding из будущего) — как признаки непригодны. Витрине нужен макрос `month_of()` в `macros/cross_db/date_parts.sql`. Честный time-based holdout даёт 0.71 (датасет обрывается в 2018-10). | D6: HGB, `SAMPLE_FRAC=0.2`, gate 0.70, хеш-сплит; предупреждение об утечке — в `decisions.md` и в описание витрины. |
| Инструменты | uv 0.12.7, Python 3.12.14 (uv), `gh` установлен, токены keyring для `dataengy` и `hnkovr` **невалидны**; `git-filter-repo` нет (→ `git subtree split`); Docker 29.8 (контекст desktop-linux); `dg`/`create-dagster` — через `uvx …@1.13.23`; `~/.kaggle` нет, `KAGGLE_*` в окружении нет; на macOS порт 5000 занят AirPlay; `ssh -T git@github.com` из sandbox не проверяется (DNS закрыт). | `gh auth refresh -h github.com` и `ssh -T git@github.com` — действия пользователя до шага 1.4; MLflow — 5001; Dagster — 3111 (3000 отдаём Grafana). |
| Документация Dagster 1.13 (Context7) | `DbtProjectComponent`: `project{project_dir, prepare_project_cli_args}`, `cli_args`, `select`, `exclude`, `translation`, `translation_settings`, `prepare_if_dev`, `include_metadata`. Раздельный запуск dbt-тестов: джоба из одних asset checks; dagster-dbt сам выставляет `DBT_INDIRECT_SELECTION=empty`, когда выбраны только чеки или чеки исключены. Freshness из dbt-конфига: `build_freshness_checks_from_dbt_assets` читает `meta.dagster.freshness_check` **моделей**; декларативный аналог — `FreshnessPolicy.time_window`. `dagster-mlflow` = `mlflow_tracking` (legacy `@resource` для op) + хук `end_mlflow_on_run_finished`. | D3, D4, D5. |

---

## 1. Ключевые решения (ADR-кандидаты для `docs/decisions.md`)

**D1. Раскладка и переключатель `INTEGRATIONS_STYLE=yaml|python`.** YAML-компоненты — в scaffold-путях `src/olist_ml/defs/ingest/defs.yaml`, `src/olist_ml/defs/ingest_dlt/defs.yaml`, `src/olist_ml/defs/dbt/defs.yaml` — ровно то, что создаёт `dg scaffold defs …` на лекции. Python-fallback (`@dlt_assets`, `@dbt_assets` + транслятор) — **вне автозагрузки**, в `src/olist_ml/integrations_python/`. Переключатель — ветка в `definitions.py`: `yaml` → scaffold-строка `dg.load_from_defs_folder(...)`; `python` → те же модули `defs/*` без YAML-папок интеграций + `integrations_python`. ⚠️ отклонение от «`definitions.py` не трогаем»: явное требование пользователя «fallback pythonic initializing ability» (2026-09-18); без ветки оба стиля загрузятся вместе и дадут дубли ключей. Паритет стилей — `tests/integration/test_style_parity.py`.

**D2. Ingest v1 = `dbt seed` отдельным компонентом; raw-схема без префикса.** Сэмплы — `dbt/seeds/raw/{orders,order_items,order_payments,order_reviews,customers,sellers,products,geolocation}.csv` (имена = имена source-таблиц); справочные seeds канона остаются в `dbt/seeds/`. Компонент `defs/ingest/defs.yaml` = `DbtProjectComponent` с `select: "path:seeds/raw"`, `translation.key: "raw/{{ node.name }}"`, группа `raw`; компонент `defs/dbt/defs.yaml` — `exclude: "path:seeds/raw"`. Два компонента снимают конфликт «seed и source с одним ключом внутри одного multi-asset». `sources.yml` копии: `database: olist`, `schema: raw`, `meta.dagster.asset_key: [raw, <table>]`, без `external_location`. Схемы без префикса `main_` (`raw`/`staging`/`intermediate`/`marts`) даёт макрос `generate_schema_name` (5 строк). ⚠️ отклонение: макрос, `sources.yml`, `profiles.yml`, `mart_order_features` + `month_of()` — единственные правки копии канона; список проверяет `tests/smoke/test_dbt_copy_in_sync.py`. dlt v2 пишет в ту же схему `raw` те же таблицы → `sources.yml` при переходе v1→v2 не меняется; активный режим — `INGEST_MODE=seed|dlt`.

**D3. Раздельные стадии без Python-декоратора.** `RAW = groups("raw")`, `DBT = kind("dbt") - RAW`, `transform_job = define_asset_job(selection=DBT.without_checks())`, `dq_job = define_asset_job(selection=(DBT - DBT.without_checks()) | checks(source_freshness))`. Компонент остаётся с `cli_args: [build]`: выборка «только модели» → dagster-dbt исключает тесты (эквивалент `dbt run`); выборка «только чеки» → `dbt build --select <tests>` с `DBT_INDIRECT_SELECTION=empty` (эквивалент `dbt test`). Падение теста — красный чек, модель зелёная. `dbt_build_job = define_asset_job(selection=DBT)` (одна кнопка) остаётся для сравнения. `@dbt_assets` — в `integrations_python/dbt.py` для слайда про декоратор. Интеграционный тест: `dq_job` не создаёт материализаций, `transform_job` — check evaluations.

**D4. Freshness — два слоя.** (a) Dagster-native: `FreshnessPolicy.time_window(warn_window, fail_window)` на `raw/*` и `mart_order_features` (окна в минутах из settings; статус в UI отдельно от материализации; демон включён в `dagster.yaml`). (b) dbt-native: `sources.yml` сохраняет `loaded_at_field` + `freshness`; в `dq_job` входит `@dg.multi_asset_check` `source_freshness`: запускает `dbt source freshness`, парсит `target/sources.json`, отдаёт `AssetCheckResult` на каждый `raw/<table>` (pass / warn → severity WARN / error). `build_freshness_checks_from_dbt_assets` не используем: требует `meta.dagster.freshness_check` на моделях и дублирует (a). Последний заказ — 2018-10-17, поэтому нарушение показывается сужением окна через `.env` (`DBT_FRESHNESS_WARN_DAYS=30`).

**D5. MLflow — свой `MlflowResource(dg.ConfigurableResource)`**; `dagster-mlflow` не подключаем (op-ориентированный legacy). Backend `sqlite:///<DATA_DIR>/mlflow.db`, артефакты `<DATA_DIR>/mlruns`, `make mlflow` → UI на 5001. Модель логируется `serialization_format=cloudpickle` (обход падения skops при `dlt` в процессе). Реестр: `register_model` + алиас `champion`; идемпотентность — тег `dataset_fingerprint = md5(sorted order_id + params)` (0.01 с на 96 тыс. id): совпал с последней версией → новая не создаётся, `model_registered` отдаёт `reused_existing=True`.

**D6. ML-задача: «заказ доставлен с опозданием» (бинарная классификация, 6.8 % положительных).** Витрина `mart_order_features` — одна строка на доставленный заказ, 22 признака, известные в момент покупки (`items_cnt`, `distinct_sellers_cnt`, `items_value`, `freight_value`, `freight_share`, `order_value`, `is_large_order`, `main_payment_type`, `max_installments`, `customer_state`, `customer_region`, `customer_seller_distance_km`, `order_hour`, `order_dow`, `purchase_month`, `estimated_delivery_span_days`, категория и вес/объём товаров); **запрещены** `delivery_days`, `delivery_delay_days`, `order_delivered_*`, `review_*`, `has_review`, `order_status`, `is_canceled` и любые `mart_*`. Модель по умолчанию — `HistGradientBoostingClassifier(max_iter=100, max_leaf_nodes=15, min_samples_leaf=50, l2_regularization=1.0)` (0.6 с на 20 %); `ML_MODEL=logreg` — второй вариант, им удобно честно ронять gate. `SAMPLE_FRAC=0.2` (на 0.1 худший сид 0.683 < порога). Quality gate: blocking `roc_auc >= ML_MIN_ROC_AUC` (0.70 — от минимума по сидам, не от медианы) + неблокирующий `pr_auc >= ML_MIN_PR_AUC` (0.18; baseline 0.068); accuracy в gate не брать (0.93 у константы). Сплит — в Python: `sort_values("order_id", kind="mergesort")`, `bucket = int(md5(f"{order_id}|split|{seed}")[:8], 16) % 10000`, holdout 20 %; в витрине колонки `split` нет (иначе смена `sample_frac`/`test_frac` = пересборка витрины; `hash()` DuckDB ≠ `cityHash64` ClickHouse). ⚠️ честный time-based holdout даёт 0.71 (датасет обрывается в 2018-10, positive rate в хвосте 3.5 %) — фиксируем в `decisions.md`, в аудитории MLE это спросят. Наглядность — метаданные в Dagster UI: `MetadataValue.md` таблица метрик, ROC и топ-10 важностей как PNG (0.16 с, ~35 КБ, встроены в markdown data-URI), `MetadataValue.url` на MLflow run, числовые метаданные (`roc_auc`, `pr_auc`, `rows_train/test`) — Dagster сам строит по ним график. Ноутбук не нужен. Permutation importance — только на сэмпле holdout ≤ `ML_IMPORTANCE_MAX_ROWS` (на полном объёме 7 с).

**D7. Executor и параллелизм.** `in_process_executor` объявлен один раз (`defs/executor.py`): DuckDB — один писатель на файл; в compose дополнительно `QueuedRunCoordinator.max_concurrent_runs: 1`.

**D8. Хранилище Dagster в compose — Postgres** (подтверждено лейном Observability): webserver, daemon и каждый run-подпроцесс внутри контейнера daemon (`DefaultRunLauncher`) пишут в event log; SQLite на общем volume Docker Desktop (virtiofs) даёт `database is locked`, а упрощение compose было бы в один сервис. Postgres — один сервис (`postgres:16-alpine`) и три переменные; dev (`dg dev`, SQLite в `DAGSTER_HOME`) и prod различаются только `storage` + `run_coordinator` в `dagster.yaml` и `.env`. Структурированные логи включаются **только флагом** `--log-format json` у `dagster-webserver` и `dagster-daemon run` (переменной `DAGSTER_LOG_FORMAT` не существует); логи run-воркеров остаются текстовыми — полная картина запуска в event log (Postgres).

**D9. Копия dbt, а не submodule/symlink.** Submodule `mlinside-hw-olist` внутрь `mlinside-demo` даёт цикл (hw-olist → demo → hw-olist), symlink `../../dbt` не существует в клоне. Копия + `scripts/sync_dbt_from_canonical.sh` (rsync с исключениями) + smoke-тест на разрешённый список отличий; `mart_order_features` и `month_of()` — кандидаты на перенос в канон отдельным PR.

**D10. Метрики для Grafana — один sidecar-экспортер** `observability/exporter/dagster_exporter.py` (186 строк, порт 9101; poll GraphQL Dagster `runsOrError` / `assetNodes{freshnessStatusInfo, assetChecksOrError{executionForLatestMaterialization}}` + REST MLflow `runs/search` → `prometheus_client`; сверено по исходникам `dagster-graphql 1.13.23` и `mlflow 3.16.1`: `startTime/endTime` в секундах, `latestMaterializationTimestamp` в миллисекундах). Альтернативы отброшены: метрики из логов Loki не дают статус asset check и freshness (в логи они не пишутся); Grafana Infinity без Prometheus нарушает ТЗ и не хранит историю. Freshness — через `freshnessStatusInfo` (0 HEALTHY / 1 WARNING / 2 DEGRADED / −1 UNKNOWN); `dagster.yaml` обязан содержать `freshness: {enabled: true}`, иначе всегда UNKNOWN. Alloy — только логи контейнеров (`discovery.docker` → Loki; функция `sys.env`, не `env`); Prometheus скрейпит экспортер сам (OTel Collector отпал: его filelog receiver на macOS не видит `/var/lib/docker/containers`). Алерты — Grafana Alerting с Telegram contact point из provisioning (`${TG_BOT_TOKEN}`/`${TG_CHAT_ID}`; отрицательный chat_id — строкой в кавычках); «уровень 0» — `@run_failure_sensor` → `httpx.post` в Bot API (dry-run, пока `ALERTS_ENABLED=false`; `httpx` — в основных зависимостях, не в dev). Гочи: профили compose всегда парой `--profile core --profile observability` (один `observability` падает на `depends_on`); MLflow 3.x отбивает `Host: mlflow:5001` с 403 без `MLFLOW_SERVER_ALLOWED_HOSTS`; порог метрики модели в `rules.yml` — число, а `${VAR}` Grafana подставляет только в строки → порог либо дублируется, либо `rules.yml` рендерится из `.env` скриптом (вопрос §9).

**D11. Kaggle — анонимно, без учёток** (проверено лейном Ingest: холодная загрузка 15.9 с, архив 42.6 МБ → 9 CSV 120.3 МБ, повтор из кеша 0.6 с). `KAGGLE_USERNAME/KAGGLE_KEY` остаются опциональными полями `.env`; в CI Kaggle не вызывается — фикстуры. Кеш — `KAGGLEHUB_CACHE` из settings. kagglehub 1.0.2 ходит на **`api.kaggle.com`** (плюс `www.kaggle.com`, `storage.googleapis.com`) — учесть в allowlist sandbox. `product_category_name_translation.csv` содержит UTF-8 BOM → все CSV читаются с `encoding="utf-8-sig"`. Raw-слой грузится `dtype=str`, касты — в dbt staging (как в каноне). Geolocation не сжимается пропорционально (27.6 % строк при `sample_frac=0.02` — zip-префиксы грубые) → в `sample_connected` добавляется `geo_max_rows` из settings, как `geo_max` в каноническом `make_sample.py`. Служебные `_dlt_loads/_dlt_pipeline_state/_dlt_version` в `sources.yml` не заносятся. Fallback-лестница на локальные CSV **не нужна**: v1 (dbt seed) навсегда остаётся независимым путём ingest без сети.

**D12. Параллельные агенты.** Лейны (§6) владеют непересекающимися путями; общие файлы (`settings.py`, `Makefile`, `jobs.py`, `.env.example`, `ci.yml`, `docs/progress.md`) правятся под объектной блокировкой `/agent-session-lock --object <slug>` (`~/.claude/skills/agent-session-lock`: атомарный `mkdir`-CAS, TTL, `.ai/.locks/` в gitignore), минимальный аддитивный diff, `git pull --rebase` перед пушем, коммит только своих путей. Гейты — строки `- [x] <шаг> — <sha> — <дата>` в `docs/progress.md`. Модели: оркестратор/интеграция — текущая; ML и Observability — opus; CI, Kaggle-спайк, git-репетиция и всё agentic (скиллы/хуки/агенты) — sonnet/haiku, низкий effort.

**D13. Agentic-обвязка «BRD-watch» — собрана, ждёт установки пользователем.** Хук `brd-watch-hook.sh` (`UserPromptSubmit` + `SessionStart`, print-only, всегда exit 0; паттерны `PROMPT.md`/`BRD*.md`/`PRD*.md`/`SPEC*.md`/`PLAN.md`/`docs/decisions.md`, `max_files`, `diff_lines`, `state_file` — в `settings.yml` хуков) сравнивает sha256 со стампом `.ai/.brd-state.json`, печатает «⚠️ BRD изменился: <файл> (+a/−d строк) — перечитай перед продолжением» + голову `diff -u` со снапшотом `.ai/.brd-state/*.prev`; протестирован на scratch-репо (bootstrap тихий, изменение — предупреждение, новый файл — отдельное сообщение, `enabled: false` — тихо). Скилл `reread-brd-and-sync-plan` — пока candidate (формат `create-skill-candidate`, scope=global, prj=mlinside-demo): `create-skill` интерактивен (review-gate) и пишет в закрытые пути. Субагент `brd-watcher.md` (haiku, Read/Grep/Glob/Bash read-only, таблица дельты требований: №, тип added/changed/removed, строки, затронутый шаг плана, рекомендация). Sandbox этой сессии запрещает запись во **все** реальные места (`~/.claude/skills|hooks`, `~/.ai/subagents`, `~/.ai/skills/_skills_candidates_glob`, `.claude/{hooks,skills,agents,settings.local.json}` проекта) → артефакты в `.claude/drafts/agentic/` + идемпотентный `apply.sh` (`REPO=<repo> bash apply.sh`: `add-session-hook … -apply` ×2, подмена тела хука, yq-мерж настроек, `.gitignore += .ai/.brd-state*`, pipe-тест, кандидат скилла + symlink в `skills-candidates-ai-HLs`, копия агента в `<repo>/.claude/agents/` + dry-run `sync-project-agents`). После выделения `mlinside-demo` — повторить с `REPO=<mlinside-demo>`.

---

**D14. Отдельный репозиторий на демо (2026-09-20) — заменяет монорепо `mlinside-demo` из D9/§4.** `demo/02-dagster` вынесен в [dataengy/mlinside-demo-02-dagster](https://github.com/dataengy/mlinside-demo-02-dagster) (public, история сохранена `git subtree split --prefix=demo/02-dagster`) и подключён в `mlinside-hw-olist` сабмодулем `demo/02-dagster`. Причины: демо живут и версионируются независимо, нет цикла hw-olist → demo → hw-olist, `MLInside-course` может подключать ровно нужные демо. Шаг 1 (§4) исполняется в урезанном виде: только `02-dagster`; `01-dbt` и `MLInside-course` — отдельными шагами по тому же скиллу `/extract-dir-as-github-submodule`. Все команды этапов 1–2 выполняются из корня нового репо (не из `mlinside-demo/02-dagster/`).

## 2. Целевая структура файлов (что создаём и за что каждый файл отвечает)

```
mlinside-demo/                         # новый репозиторий dataengy/mlinside-demo (шаг 1)
  README.md                            # индекс демо, ссылки на лекции, ловушка вложенных сабмодулей
  01-dbt/                              # как есть — после фиксации пользователем (§9)
  02-dagster/
    .claude/PROMPT.md  .claude/PLAN.md # ТЗ и план (переезжают из корня 02-dagster при scaffold)
    .claude/drafts/<lane>/             # артефакты лейнов-разведки (§12) — не код проекта
    pyproject.toml  uv.lock            # deps с пинами, [tool.dg], pytest markers, ruff
    .env.example                       # ВСЕ переменные с комментариями; секреты пустые
    Makefile                           # единственный интерфейс: install/dev/test/check/demoN/docker-*/clean-*
    Dockerfile  .dockerignore          # multi-stage uv; один образ для webserver и daemon
    docker-compose.yml                 # профили core | observability
    dagster.yaml                       # dev: telemetry off, freshness daemon on, python_logs
    deploy/dagster.prod.yaml           # prod: postgres storage, queued coordinator, json-логи
    .github/workflows/ci.yml           # шаги = make-цели
    src/olist_ml/
      definitions.py                   # scaffold + ветка INTEGRATIONS_STYLE (D1)
      settings.py                      # ЕДИНСТВЕННОЕ место констант (pydantic-settings)
      resources.py                     # DuckDBResource, MlflowResource
      components/__init__.py           # scaffold (пусто)
      defs/
        resources.py                   # @dg.definitions → resources={duckdb, mlflow_res, dbt}
        executor.py                    # @dg.definitions → executor=in_process_executor (D7)
        ingest/defs.yaml               # DbtProjectComponent select path:seeds/raw → raw/<table> (v1)
        ingest_dlt/{defs.yaml,loads.py,kaggle_olist.py,sampling.py}   # dlt из Kaggle → raw/<table> (v2)
        dbt/defs.yaml                  # DbtProjectComponent exclude path:seeds/raw, translation ключей/групп
        dbt/freshness.py               # source_freshness multi_asset_check (D4b) + FreshnessPolicy (D4a)
        ml/{features.py,assets.py,checks.py}   # чистые функции / ассеты / quality gate
        jobs.py                        # seed_job / transform_job / dq_job / dbt_build_job / ml_job / full_pipeline_job
        maintenance/jobs.py            # clean_ingest_job / clean_dbt_job / clean_ml_job / clean_all_job (@op/@job)
        automation/{schedules.py,sensors.py}   # full_pipeline_schedule; alert_on_run_failure; incoming_file_sensor (опц.)
      integrations_python/{__init__.py,ingest.py,dbt.py}   # fallback-стиль (D1), не автозагружается
      alerts/telegram.py               # format_run_failure(), send_telegram(text, http) — тест на моке
    dbt/                               # копия канона + разрешённые отличия (D2, D9)
      profiles.yml (только duck)  macros/generate_schema_name.sql  macros/cross_db/date_parts.sql (+month_of)
      models/sources/sources.yml       # schema raw, meta.dagster.asset_key, loaded_at_field + freshness
      models/marts/mart_order_features.sql (+ _marts__models.yml)
      seeds/raw/*.csv                  # сэмпл 8 таблиц (~5 тыс. заказов)
    data/README.md                     # data/raw, olist.duckdb, mlflow.db, mlruns, kagglehub — всё в .gitignore
    scripts/{sync_dbt_from_canonical.sh,make_seeds_sample.py,make_fixtures.py,check_ui.py,step1-restructure.sh}
    tests/{conftest.py,fixtures/,smoke/,unit/,integration/,e2e/}
    observability/{exporter/,alloy/,prometheus/,loki/,grafana/provisioning/,grafana/dashboards/}
    docs/{runbook.md,decisions.md,progress.md,observability.md,deploy/}
```

---

## 3. Порядок работ и зависимости (гейты через `docs/progress.md`)

```
ШАГ 1 (git, необратимо, после ревью плана и подтверждения)  ── L-0 git-split
   │
ЭТАП 1 (основной агент, последовательно, коммит+пуш после каждого шага):
   S1 scaffold ─► S2 dbt-seed-ingest ─► S3 dbt build ─► S4 MLflow ─► S5 CI ─► S6 тесты/зелёное
   │              │                       │              │
ЭТАП 2 (параллельные лейны, стартуют по гейтам):
   │              ├─ L-A dlt/Kaggle (после S2)          ├─ L-D ML-наглядность + автоматизация (после S4)
   │              └─ L-B transform/dq + freshness (после S3)   ├─ L-E Compose + Grafana + Telegram (после S4)
   │                                                        ├─ L-H batch-инференс + SIM_TODAY (после S4)
   ├─ L-C maintenance (после S2; каждая clean_* — после своего этапа)   └─ L-F docs (после S3; финал после S6)
   └─ L-G agentic BRD-watch (не зависит от кода; уже запущен)
```

Оценка (агент-часы): шаг 1 — 1 (+ ожидание пользователя); S1 1; S2 3; S3 2; S4 4; S5 1.5; S6 3; L-A 4; L-B 3; L-C 2; L-D 3; L-E 8; L-F 4; L-G 2; L-H 4. Итого ≈ 46 при 3–4 параллельных лейнах — 2–3 календарных дня.

---

## 4. Шаг 1 — реструктуризация git (`demo` → `dataengy/mlinside-demo`)

> **Заменено D14 (2026-09-20):** для `02-dagster` шаг выполнен как вынос в отдельный репо `dataengy/mlinside-demo-02-dagster` + сабмодуль `demo/02-dagster`. Процедура ниже — историческая, для `01-dbt`/`MLInside-course` использовать `/extract-dir-as-github-submodule`.

**Предусловия на 2026-09-18 — не выполнены:**

| Предусловие | Состояние (лейн git-split) | Действие пользователя |
|---|---|---|
| Чистый `git status` в `mlinside-hw-olist` | 466 записей; `demo/01-dbt`: новая структура только в индексе, 117 старых файлов удалены в дереве, 120 МБ seeds в индексе, `olist.duckdb` 106 МБ не игнорируется | Решить §9 Q1, добавить `demo/**/*.duckdb` в `.gitignore`, закоммитить целевое `01-dbt` на `dev` |
| Чистый `git status` в `MLInside-course` | `content/*.yml`, сабмодуль, untracked pptx | Закоммитить/stash **или** разрешить путь-скоуп коммит `.gitmodules` + `demo` (§9 Q2) |
| Чистый `git status` в `mlinside-dagster-demo` | Только `.idea/` | Ничего (или в .gitignore) |
| `gh` авторизован под `dataengy` | Токены keyring невалидны (проверено дважды) | `gh auth refresh -h github.com` |
| SSH к github.com | Из sandbox не проверить (DNS закрыт) | `ssh -T git@github.com` |
| Решение по `01-dbt` | см. §9 Q1 | ответ |

**Процедура** — `step1-restructure.sh` (лейн L-0, лежит в `.claude/drafts/git-split/`; bash 3.2-совместим, `DRY_RUN=1` по умолчанию, `run()` через `"$@"`, режим `--verify` собирает все ошибки; dry-run прошёл на временном клоне + двух фейковых репо, стадия `gh auth` корректно упала — гейт работает):

- [ ] **1.1** `--verify`: `assert_clean` для трёх репозиториев (`git status --porcelain` пуст), `gh auth status`, `ssh -T git@github.com` — иначе стоп с подсказкой.
- [ ] **1.2** Бэкап: `cp -R ~/gi/@dataengy/mlinside-hw-olist ~/gi/_backup/2026-09-18/mlinside-hw-olist` (4.7 ГБ, диск 515 ГБ свободно) + `git bundle create ~/gi/_backup/2026-09-18/hw-olist.bundle --all` (2.9 МБ) как дополнительный снапшот.
- [ ] **1.3** История: `git -C mlinside-hw-olist subtree split --prefix=demo -b demo-split` (с `dev`); проверить `git log --oneline demo-split | wc -l` (4 + коммиты из 1.1) и `git rev-list --objects demo-split | git cat-file --batch-check` — нет блобов > 50 МБ.
- [ ] **1.4** `git clone -b demo-split mlinside-hw-olist $TMPDIR/mlinside-demo-split && git -C … branch -m main`; `gh repo create dataengy/mlinside-demo --public --source=$TMPDIR/mlinside-demo-split --push`.
- [ ] **1.5** В `mlinside-hw-olist`: перенести untracked/ignored `demo/*` в `.stash/demo.pre-split/2026-09-18/` (`02-dagster/PROMPT.md`, `PLAN.md`, `.venv`, `olist.duckdb`, `target`, `.env*`), `git rm -r demo`, `git submodule add git@github.com:dataengy/mlinside-demo.git demo`, `git commit -m "refactor(demo): demo/ → submodule dataengy/mlinside-demo"`.
- [ ] **1.6** В `MLInside-course`: `git submodule add git@github.com:dataengy/mlinside-demo.git demo`, `git commit -m "feat(demo): подключить dataengy/mlinside-demo как demo/" -- .gitmodules demo`.
- [ ] **1.7** Показать `git show --stat HEAD` обоих репо; **push не делать**; ждать подтверждения.
- [ ] **1.8** В `mlinside-demo` (= `mlinside-hw-olist/demo`): `README.md` (индекс, вложенные сабмодули: `git submodule update --init` без `--recursive`, отдельная цель `make submodules-full`), `02-dagster/.claude/{PROMPT.md,PLAN.md}` и `.claude/drafts/` из `.stash/demo.pre-split/…`, коммит, пуш в `mlinside-demo` (новый репозиторий — разрешён).
- [ ] **1.9** Отдельный коммит в `MLInside-course` (после подтверждения): `content/presentations.yml` → `demos.repo/submodule/runbook` на `demo/02-dagster`; `docs/dagster-demo-runbook.md` → указатель на `demo/02-dagster/docs/runbook.md`.

---

## 5. Этап 1 — минимальный рабочий end-to-end (задачи с шагами)

Все команды — из `mlinside-demo/02-dagster/`. После каждой задачи: `make dev` в фоне → `uv run python scripts/check_ui.py` (GraphQL: ассеты/джобы шага присутствуют, `repositoriesOrError` без ошибок) → строка в `docs/progress.md` → коммит → пуш в `main`.

### Task S1: scaffold, settings, Makefile-ядро, smoke

**Files:** Create `pyproject.toml`, `src/olist_ml/{definitions.py,settings.py,resources.py,defs/resources.py,defs/executor.py}`, `.env.example`, `Makefile`, `dagster.yaml`, `scripts/check_ui.py`, `tests/conftest.py`, `tests/smoke/test_defs_load.py`, `tests/unit/test_settings.py`, `docs/{progress.md,decisions.md}`, `.gitignore`.

**Interfaces:** Produces `olist_ml.settings.settings: Settings` (поля = `.env.example` + константы датасета `RAW_TABLES`, `ML_*` списки; свойства `PROJECT_ROOT`, `DAGSTER_UI_URL`, `DAGSTER_GRAPHQL_URL`, `DUCKDB_CATALOG`, `MLFLOW_TRACKING_URI`, `MLFLOW_UI_URL`, метод `abs(path) -> Path`); `olist_ml.resources.{DuckDBResource, MlflowResource}`; `tests/conftest.py::tmp_settings` (фикстура: `monkeypatch.setenv` + новый `Settings()`, tmp DuckDB и tmp MLflow).

- [ ] **Step 1: scaffold.** `cd mlinside-demo && uvx create-dagster@1.13.23 project olist_ml` (на вопрос про `uv sync` — нет), `mv olist_ml 02-dagster && cd 02-dagster`, `mkdir -p .claude && mv ../.stash-copy/{PROMPT.md,PLAN.md} .claude/`, `uv python pin 3.12`.
- [ ] **Step 2: зависимости.** `[project] dependencies`: `dagster==1.13.23`, `dagster-dbt==0.29.23`, `dagster-dlt==0.29.23`, `dagster-duckdb==0.29.23`, `dbt-core>=1.12,<1.13`, `dbt-duckdb>=1.11,<1.12`, `dlt[duckdb]>=1.30,<2`, `duckdb>=1.5,<2`, `mlflow>=3.16,<4`, `scikit-learn>=1.9,<2`, `pandas>=2.2`, `pyarrow`, `kagglehub>=1.0,<2`, `pydantic-settings>=2.15,<3`, `httpx>=0.28`, `matplotlib>=3.9`; `[dependency-groups] dev`: `dagster-webserver==1.13.23`, `dagster-dg-cli==1.13.23`, `pytest>=8`, `pytest-timeout`, `ruff`; `[tool.pytest.ini_options] testpaths=["tests"] markers=["e2e: сквозной прогон"] addopts="-q -m 'not e2e'"`; `[tool.ruff] line-length=100 lint.select=["E","F","I","UP","B","SIM"] exclude=["dbt"]`. `uv sync`.
- [ ] **Step 3: failing test settings** `tests/unit/test_settings.py`:

```python
from pathlib import Path
from olist_ml.settings import Settings

def _env_example() -> dict[str, str]:
    text = (Path(__file__).parents[2] / ".env.example").read_text()
    return dict(l.split("=", 1) for l in text.splitlines() if l and not l.startswith("#") and "=" in l)

def test_defaults_match_env_example():
    s = Settings(_env_file=None)
    for key, raw in _env_example().items():
        if raw == "":                       # секреты и пустые — не сравниваем
            continue
        assert str(getattr(s, key)) == raw, key

def test_every_env_key_is_a_field():
    assert set(_env_example()) <= set(Settings.model_fields)

def test_composed_urls(monkeypatch):
    monkeypatch.setenv("DAGSTER_PORT", "4242")
    s = Settings(_env_file=None)
    assert s.DAGSTER_UI_URL == f"{s.DAGSTER_SCHEME}://{s.DAGSTER_HOST}:4242"
    assert s.DAGSTER_GRAPHQL_URL == s.DAGSTER_UI_URL + "/graphql"
    assert s.DUCKDB_CATALOG == s.DUCKDB_PATH.stem
```

- [ ] **Step 4: run → FAIL** (`uv run pytest tests/unit/test_settings.py -v`, «No module named olist_ml.settings»).
- [ ] **Step 5: `settings.py`** (полный набор полей = `.env.example`; здесь — каркас):

```python
"""Единственное место констант. Значения — из .env; составные — здесь и только здесь."""
from pathlib import Path
from typing import ClassVar, Literal
from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

def _find_root(start: Path) -> Path:
    for p in (start, *start.parents):
        if (p / "pyproject.toml").is_file():
            return p
    raise RuntimeError(f"pyproject.toml не найден вверх от {start}")

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    # константы датасета — не переменные окружения
    RAW_TABLES: ClassVar[tuple[str, ...]] = ("orders", "order_items", "order_payments", "order_reviews",
                                             "customers", "sellers", "products", "geolocation")
    ML_TARGET: ClassVar[str] = "is_late_delivery"
    ML_NUMERIC: ClassVar[tuple[str, ...]] = ("items_cnt", "distinct_sellers_cnt", "items_value", "freight_value",
        "freight_share", "order_value", "is_large_order", "max_installments", "customer_seller_distance_km",
        "order_hour", "order_dow", "purchase_month", "estimated_delivery_span_days", "total_weight_g", "total_volume_cm3")
    ML_CATEGORICAL: ClassVar[tuple[str, ...]] = ("main_payment_type", "customer_state", "customer_region", "main_category")
    PROJECT_ROOT: Path = _find_root(Path(__file__).resolve())
    # --- Dagster ---
    DAGSTER_SCHEME: str = "http";  DAGSTER_HOST: str = "127.0.0.1";  DAGSTER_PORT: int = 3111
    DAGSTER_HOME: Path = Path("dagster_home")
    INTEGRATIONS_STYLE: Literal["yaml", "python"] = "yaml"
    INGEST_MODE: Literal["seed", "dlt"] = "seed"
    FULL_PIPELINE_CRON: str = "0 6 * * *"
    # --- данные ---
    DATA_DIR: Path = Path("data");  DUCKDB_PATH: Path = Path("data/olist.duckdb");  RAW_SCHEMA: str = "raw"
    SAMPLE_FRAC: float = 0.2;  RANDOM_STATE: int = 42;  SEEDS_N_ORDERS: int = 5000;  FIXTURES_N_ORDERS: int = 500
    # --- dbt ---
    DBT_PROJECT_DIR: Path = Path("dbt");  DBT_TARGET: str = "duck"
    DBT_FRESHNESS_WARN_DAYS: int = 3650;  DBT_FRESHNESS_ERROR_DAYS: int = 7300
    FRESHNESS_WARN_MIN: int = 10;  FRESHNESS_FAIL_MIN: int = 30
    # --- MLflow / ML ---
    MLFLOW_SCHEME: str = "http";  MLFLOW_HOST: str = "127.0.0.1";  MLFLOW_PORT: int = 5001
    MLFLOW_DB_PATH: Path = Path("data/mlflow.db");  MLFLOW_ARTIFACTS_DIR: Path = Path("data/mlruns")
    MLFLOW_TRACKING_URI_OVERRIDE: str = ""      # непусто → сервер (compose)
    MLFLOW_EXPERIMENT: str = "olist_ml";  MLFLOW_MODEL_NAME: str = "olist_late_delivery"
    ML_MODEL: Literal["hgb", "logreg"] = "hgb";  ML_TEST_FRAC: float = 0.2
    ML_MIN_ROC_AUC: float = 0.70;  ML_MIN_PR_AUC: float = 0.18;  ML_IMPORTANCE_MAX_ROWS: int = 5000
    # --- Kaggle / dlt ---
    KAGGLE_DATASET: str = "olistbr/brazilian-ecommerce";  KAGGLE_USERNAME: str = "";  KAGGLE_KEY: str = ""
    KAGGLEHUB_CACHE: Path = Path("data/kagglehub")
    # --- алерты ---
    ALERTS_ENABLED: bool = False;  TG_BOT_TOKEN: str = "";  TG_CHAT_ID: str = ""

    def abs(self, p: Path) -> Path:
        return p if p.is_absolute() else self.PROJECT_ROOT / p

    @computed_field  # type: ignore[misc]
    @property
    def DAGSTER_UI_URL(self) -> str: return f"{self.DAGSTER_SCHEME}://{self.DAGSTER_HOST}:{self.DAGSTER_PORT}"
    @computed_field  # type: ignore[misc]
    @property
    def DAGSTER_GRAPHQL_URL(self) -> str: return f"{self.DAGSTER_UI_URL}/graphql"
    @computed_field  # type: ignore[misc]
    @property
    def DUCKDB_CATALOG(self) -> str: return self.DUCKDB_PATH.stem
    @computed_field  # type: ignore[misc]
    @property
    def MLFLOW_TRACKING_URI(self) -> str:
        return self.MLFLOW_TRACKING_URI_OVERRIDE or f"sqlite:///{self.abs(self.MLFLOW_DB_PATH).as_posix()}"
    @computed_field  # type: ignore[misc]
    @property
    def MLFLOW_UI_URL(self) -> str: return f"{self.MLFLOW_SCHEME}://{self.MLFLOW_HOST}:{self.MLFLOW_PORT}"

settings = Settings()
```

- [ ] **Step 6: `.env.example`** — те же ключи (кроме `ClassVar`) с комментариями, по одному в строке; секреты пустые (`TG_BOT_TOKEN=`, `KAGGLE_KEY=`). `cp -n .env.example .env`.
- [ ] **Step 7: run → PASS** (`uv run pytest tests/unit -v`).
- [ ] **Step 8: `definitions.py` (D1)**:

```python
"""Точка входа code location. Основной путь — scaffold-строка; ветка python — fallback (decisions.md D1)."""
from importlib import import_module
import dagster as dg
from olist_ml.settings import settings

_COMMON = ("olist_ml.defs.resources", "olist_ml.defs.executor", "olist_ml.defs.ml",
           "olist_ml.defs.jobs", "olist_ml.defs.maintenance", "olist_ml.defs.automation")

def _module_defs(name: str) -> dg.Definitions:
    return dg.ComponentTree.from_module(defs_module=import_module(name),
                                        project_root=settings.PROJECT_ROOT).build_defs()

@dg.definitions
def defs() -> dg.Definitions:
    if settings.INTEGRATIONS_STYLE == "yaml":
        return dg.load_from_defs_folder(project_root=settings.PROJECT_ROOT)
    from olist_ml.integrations_python import build_defs        # ленивый импорт: dlt/dbt-декораторы
    return dg.Definitions.merge(*(_module_defs(m) for m in _COMMON), build_defs())
```

  (В S1 модули `ml/jobs/maintenance/automation` — пустые пакеты с `__init__.py`; `integrations_python` появляется в L-A/L-B, до этого python-режим покрыт тестом `xfail(strict=True)`.)
- [ ] **Step 9: ресурсы и executor** — `src/olist_ml/resources.py`:

```python
import dagster as dg, duckdb, mlflow
from olist_ml.settings import settings

class DuckDBResource(dg.ConfigurableResource):
    path: str
    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.path, read_only=read_only)

class MlflowResource(dg.ConfigurableResource):
    tracking_uri: str; experiment: str; artifacts_dir: str
    def setup(self) -> str:
        mlflow.set_tracking_uri(self.tracking_uri); mlflow.set_registry_uri(self.tracking_uri)
        exp = mlflow.get_experiment_by_name(self.experiment)
        exp_id = exp.experiment_id if exp else mlflow.create_experiment(self.experiment, artifact_location=self.artifacts_dir)
        mlflow.set_experiment(self.experiment)
        return exp_id
    def run_url(self, exp_id: str, run_id: str) -> str:
        return f"{settings.MLFLOW_UI_URL}/#/experiments/{exp_id}/runs/{run_id}"
```

  `src/olist_ml/defs/resources.py`:

```python
import dagster as dg
from dagster_dbt import DbtCliResource
from olist_ml.resources import DuckDBResource, MlflowResource
from olist_ml.settings import settings

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "duckdb": DuckDBResource(path=str(settings.abs(settings.DUCKDB_PATH))),
        "mlflow_res": MlflowResource(tracking_uri=settings.MLFLOW_TRACKING_URI, experiment=settings.MLFLOW_EXPERIMENT,
                                     artifacts_dir=settings.abs(settings.MLFLOW_ARTIFACTS_DIR).as_uri()),
        "dbt": DbtCliResource(project_dir=str(settings.abs(settings.DBT_PROJECT_DIR)), target=settings.DBT_TARGET),
    })
```

  `src/olist_ml/defs/executor.py`: `@dg.definitions def executor(): return dg.Definitions(executor=dg.in_process_executor)  # DuckDB: один писатель (D7)`.
- [ ] **Step 10: smoke** `tests/smoke/test_defs_load.py`:

```python
import subprocess, sys
from olist_ml.definitions import defs

def test_definitions_import():
    d = defs()
    assert d.executor is not None and {"duckdb", "mlflow_res", "dbt"} <= set(d.resources)

def test_dg_check_defs():
    r = subprocess.run(["uv", "run", "dg", "check", "defs"], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
```

- [ ] **Step 11: Makefile-ядро** (`-include .env` + `export`; `?=`-fallback'и = `.env.example`; цели `help install env dev dev-check test test-e2e test-all check lint fmt clean`; `dev: uv run dg dev --port $(DAGSTER_PORT)`; `check: uv run dg check defs && uv run ruff check . && cd dbt && uv run dbt parse`), `dagster.yaml` (`telemetry.enabled: false`, `freshness.enabled: true`, `python_logs.managed_python_loggers: [olist_ml]`), `scripts/check_ui.py` (POST `{"query":"{ repositoriesOrError { ... on RepositoryConnection { nodes { name assetNodes { assetKey { path } } jobs { name } } } ... on PythonError { message } } }"}` на `settings.DAGSTER_GRAPHQL_URL`, ждёт до 60 с, печатает счётчики, exit ≠ 0 при `PythonError`).
- [ ] **Step 12: run all → PASS**: `make check && make test`; `make dev &` → `make dev-check` (0 ассетов, без ошибок).
- [ ] **Step 13: docs + commit.** `docs/progress.md`: `## Этап 1` → `- [x] scaffold — <sha> — <дата>`; `docs/decisions.md`: D1, D7. `git commit -m "feat(scaffold): create-dagster project olist_ml, settings, Makefile, smoke"` → push.

### Task S2: копия dbt + сэмпл-seeds + ingest-компонент (v1) + `seed_job`

**Files:** Create `scripts/{sync_dbt_from_canonical.sh,make_seeds_sample.py,make_fixtures.py}`, `dbt/**` (копия), `dbt/macros/generate_schema_name.sql`, `dbt/macros/cross_db/date_parts.sql` (+`month_of`), `dbt/models/sources/sources.yml` (вариант демо), `dbt/profiles.yml`, `dbt/seeds/raw/*.csv`, `src/olist_ml/defs/ingest/defs.yaml`, `src/olist_ml/defs/jobs.py` (`seed_job`), `tests/fixtures/*.csv`, `tests/smoke/test_dbt_copy_in_sync.py`, `tests/unit/test_sampling.py`, `tests/integration/test_seed_job.py`.

**Interfaces:** Produces ключи `raw/<t>` для `t in settings.RAW_TABLES` (группа `raw`), таблицы `olist.raw.<t>` в DuckDB, `jobs.py::{RAW, seed_job}`; `scripts/make_seeds_sample.py::sample_connected(tables: dict[str, pd.DataFrame], n_orders: int, seed: int) -> dict[str, pd.DataFrame]`.

- [ ] **Step 1** `scripts/sync_dbt_from_canonical.sh`: `rsync -a --delete --exclude target --exclude dbt_packages --exclude logs --exclude '.user.yml' --exclude 'seeds/olist_*_dataset.csv' --exclude '*.duckdb' --exclude '.env*' --exclude .idea "${CANON:?путь к mlinside-hw-olist/dbt}/" dbt/` + печать `git status --short dbt`. Прогнать с `CANON=../../../mlinside-hw-olist/dbt`.
- [ ] **Step 2** Разрешённые отличия (наложение — часть того же скрипта, чтобы re-sync был идемпотентен): `profiles.yml` → только `duck` (`path: "{{ env_var('DUCKDB_PATH', 'data/olist.duckdb') }}"`, `schema: main`, `threads: 4`); `macros/generate_schema_name.sql`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
```

  `macros/cross_db/date_parts.sql` — добавить `month_of(col)` рядом с `hour_of`/`dow_of` (из `.claude/drafts/ml/macro_month_of.sql`); `dbt_project.yml`: `seeds: olist_dbt: raw: +schema: raw` (остальные `+schema: seeds`); `sources.yml` — у каждой таблицы удалить `meta.external_location`, добавить `meta: {dagster: {asset_key: [raw, <table>]}}`; на уровне source `database: olist`, `schema: raw`; у `orders` — `freshness: {warn_after: {count: "{{ env_var('DBT_FRESHNESS_WARN_DAYS','3650') | int }}", period: day}, error_after: {count: "{{ env_var('DBT_FRESHNESS_ERROR_DAYS','7300') | int }}", period: day}}`.
- [ ] **Step 3** Failing unit `tests/unit/test_sampling.py` (синтетические 20 заказов: `sample_connected(tables, n_orders=5, seed=1)` — все `order_items.order_id ⊂ orders.order_id`, `customers` покрывают `orders.customer_id`, `products/sellers` покрывают `order_items`, `geolocation` — zip-префиксы клиентов и продавцов; тот же seed → идентичный результат; `n_orders=len(orders)` → всё). → `make_seeds_sample.py` (pandas, `dtype=str`, `orders.sample(n, random_state=seed)` → связанные таблицы как в каноническом `make_sample.py`; вход — `data/sample/*.csv.gz` канона или `data/raw/*.csv`; выход `dbt/seeds/raw/<t>.csv`, `SEEDS_N_ORDERS`) и `make_fixtures.py` (то же в `tests/fixtures/`, `FIXTURES_N_ORDERS`). → PASS. `make data-sample` вызывает оба.
- [ ] **Step 4** `cd dbt && uv run dbt deps && uv run dbt seed && uv run dbt build` — зелёно; число моделей/тестов — в `docs/progress.md`.
- [ ] **Step 5** `src/olist_ml/defs/ingest/defs.yaml`:

```yaml
type: dagster_dbt.DbtProjectComponent
attributes:
  project:
    project_dir: "{{ project_root }}/dbt"
  select: "path:seeds/raw"
  cli_args: [build]          # для seeds build = seed (+ тесты сидов, если есть)
  translation:
    key: "raw/{{ node.name }}"
    group_name: raw
    description: "Сырая таблица Olist `{{ node.name }}` — загружена dbt seed из сэмпла (демо 1, v1)."
  prepare_if_dev: true
```

- [ ] **Step 6** `src/olist_ml/defs/jobs.py`: `RAW = dg.AssetSelection.groups("raw")`; `seed_job = dg.define_asset_job("seed_job", selection=RAW, description="Стадия 0 — ingest v1: dbt seed сэмплов в схему raw.")`; `@dg.definitions def jobs(): return dg.Definitions(jobs=[seed_job])`.
- [ ] **Step 7** Failing integration `tests/integration/test_seed_job.py`: `tmp_settings` (tmp `DUCKDB_PATH`, `DBT_PROJECT_DIR` = копия `dbt/` с `seeds/raw` из `tests/fixtures/`) → `defs().resolve_job_def("seed_job").execute_in_process()` → `success`; `duckdb` показывает 8 таблиц в `raw` с числом строк = строкам фикстур; повторный запуск → те же counts. → PASS.
- [ ] **Step 8** `make dev` → 8 серых узлов группы `raw`; Materialize `seed_job` → зелёные; `make dev-check`. `tests/smoke/test_dbt_copy_in_sync.py`: `diff -rq` копии и канона (путь канона из env `CANON`, тест пропускается, если канон недоступен) даёт только разрешённый список (`profiles.yml`, `dbt_project.yml`, `sources.yml`, `generate_schema_name.sql`, `date_parts.sql`, `mart_order_features.sql`, `_marts__models.yml`, `seeds/raw/*`).
- [ ] **Step 9** progress + decisions D2, D9. **Commit:** `feat(ingest): dbt seed sample as raw/* assets, seed_job, dbt copy sync` → push.

### Task S3: dbt-компонент, связный граф, `dbt_build_job`

**Files:** Create `src/olist_ml/defs/dbt/defs.yaml`; Modify `defs/jobs.py` (`DBT`, `dbt_build_job`); Create `tests/smoke/test_graph_connected.py`, `tests/integration/test_dbt_build_job.py`.

**Interfaces:** Produces ключи моделей = имя модели (`stg_orders`, `int_orders_enriched`, `mart_*`), группы по папке (`staging`/`intermediate`/`marts`), чеки = dbt-тесты; `jobs.py::DBT = dg.AssetSelection.kind("dbt") - RAW`, `dbt_build_job`.

- [ ] **Step 1** `src/olist_ml/defs/dbt/defs.yaml`:

```yaml
type: dagster_dbt.DbtProjectComponent
attributes:
  project:
    project_dir: "{{ project_root }}/dbt"
  exclude: "path:seeds/raw"
  cli_args: [build]
  translation:
    key: "{{ node.name }}"
    group_name: "{{ node.fqn[1] if node.fqn | length > 2 else 'dbt' }}"
    description: "{{ node.description or 'dbt-модель ' ~ node.name }}"
  prepare_if_dev: true
```

  Ключи source-узлов приходят из `meta.dagster.asset_key` в `sources.yml` (= `raw/<table>` из S2) — граф склеивается. Если компонент применяет `translation.key` и к source-узлам поверх meta — `key: "{{ 'raw/' ~ node.name if node.resource_type == 'source' else node.name }}"` (проверяется тестом шага 3).
- [ ] **Step 2** `jobs.py`: `DBT = dg.AssetSelection.kind("dbt") - RAW`; `dbt_build_job = dg.define_asset_job("dbt_build_job", selection=DBT, description="Демо 2 v1 — dbt build одной кнопкой: модели + тесты как asset checks.")`.
- [ ] **Step 3** Failing smoke `tests/smoke/test_graph_connected.py`: `g = defs().resolve_asset_graph()`; у каждого `stg_*` есть родитель с префиксом `raw`; BFS от `raw/orders` по `g.get(key).parent_keys | child_keys` достигает все ключи (один связный компонент); ключей ≥ 8 + 8 + 3 + 6. → PASS.
- [ ] **Step 4** Failing integration `tests/integration/test_dbt_build_job.py`: tmp DuckDB после `seed_job` → `dbt_build_job` → `success`, есть `AssetCheckEvaluation` с `passed=True`, `staging.stg_orders` и `marts.mart_daily_state_metrics` не пусты; второй запуск — те же row counts. → PASS.
- [ ] **Step 5** `make dev` → граф `raw → staging → intermediate → marts` слева направо; Materialize `dbt_build_job`; `make dev-check` (счётчик ассетов = `dg list defs --json`).
- [ ] **Step 6** progress + decisions. **Commit:** `feat(dbt): DbtProjectComponent over canonical Olist project, connected graph, dbt_build_job` → push.

### Task S4: `mart_order_features` + ML-ассеты + MLflow + `ml_job`/`full_pipeline_job` + расписание

**Files:** Create `dbt/models/marts/mart_order_features.sql` (из `.claude/drafts/ml/`, + описание и тесты `not_null/unique(order_id)`, `accepted_values(is_late_delivery: [0,1])` в `_marts__models.yml`), `src/olist_ml/defs/ml/{features.py,assets.py,checks.py}`, `src/olist_ml/defs/automation/schedules.py`; Modify `defs/jobs.py` (`ML`, `ml_job`, `full_pipeline_job`); Create `tests/unit/test_features.py`, `tests/unit/test_quality_gate.py`, `tests/integration/test_ml_pipeline.py`.

**Interfaces:** Produces ассеты `training_dataset` (DataFrame), `model` (dict `run_id, model_uri, exp_id, roc_auc_train, params`), `model_evaluation` (dict `roc_auc, pr_auc, accuracy, positive_rate, n_test`), `model_registered` (`MaterializeResult`: `model_version`, `reused_existing`, `model_uri models:/<name>@champion`); чеки `quality_gate` (blocking, на `model_evaluation`), `pr_auc_floor` (WARN), `roc_auc_within_bounds` (на `model`); `features.py::{split_by_hash(df, test_frac, seed) -> tuple[DataFrame, DataFrame], build_pipeline(model, numeric, categorical, **params) -> Pipeline, evaluate(pipe, X, y) -> dict, roc_png(y, proba) -> bytes, importance_png(pipe, X, y, max_rows, seed) -> bytes, md_table(metrics) -> str, dataset_fingerprint(df, params) -> str, gate(value, threshold) -> bool}`.

- [ ] **Step 1** `mart_order_features.sql` (черновик лейна ML сверен построчно с прототипом на 96 476 строках; `where is_delivered = 1`; только `ref()` на `int_orders_enriched`, `stg_order_items`, `stg_products`; макросы `safe_div`, `month_of`, `dbt.datediff`) → `uv run dbt build --select mart_order_features+` зелёный.
- [ ] **Step 2** Failing unit `test_features.py`: `split_by_hash` — детерминизм (два вызова равны), доля holdout `0.2 ± 0.02` на 10 000 синтетических id, независимость от порядка строк (перемешанный df → те же множества), вложенность (`test_frac=0.1 ⊂ 0.2`); `md_table` содержит `roc_auc`; `roc_png` начинается с `\x89PNG`; `dataset_fingerprint` не зависит от порядка строк и меняется при смене `params`. `test_quality_gate.py`: `gate(0.699, 0.70) is False`, `gate(0.70, 0.70) is True`. → `features.py` → PASS.
- [ ] **Step 3** `src/olist_ml/defs/ml/assets.py`:

```python
import base64, dagster as dg, mlflow, pandas as pd
from sklearn.metrics import roc_auc_score
from olist_ml.resources import DuckDBResource, MlflowResource
from olist_ml.settings import settings
from olist_ml.defs.ml import features as F

FEATURES = [*settings.ML_NUMERIC, *settings.ML_CATEGORICAL]

class TrainConfig(dg.Config):
    model: str = settings.ML_MODEL          # hgb | logreg — меняется в Launchpad одним движением
    test_frac: float = settings.ML_TEST_FRAC

def _mart(duckdb: DuckDBResource) -> pd.DataFrame:
    with duckdb.connect(read_only=True) as con:
        return con.execute("select * from marts.mart_order_features order by order_id").df()

@dg.asset(deps=[dg.AssetKey("mart_order_features")], group_name="ml", kinds={"pandas"},
          description="Обучающая выборка из витрины: детерминированный сплит по md5(order_id).")
def training_dataset(duckdb: DuckDBResource, config: TrainConfig) -> dg.Output[pd.DataFrame]:
    train, _ = F.split_by_hash(_mart(duckdb), config.test_frac, settings.RANDOM_STATE)
    return dg.Output(train, metadata={"rows_train": len(train),
                                      "positive_rate": round(float(train[settings.ML_TARGET].mean()), 4)})

@dg.asset(group_name="ml", kinds={"sklearn", "mlflow"},
          description="Обученная модель; метаданные ведут в MLflow, а не копируют его.")
def model(training_dataset: pd.DataFrame, mlflow_res: MlflowResource, config: TrainConfig) -> dg.Output[dict]:
    exp_id = mlflow_res.setup()
    X, y = training_dataset[FEATURES], training_dataset[settings.ML_TARGET]
    params = {"model": config.model, "random_state": settings.RANDOM_STATE, "test_frac": config.test_frac}
    pipe = F.build_pipeline(config.model, settings.ML_NUMERIC, settings.ML_CATEGORICAL, random_state=settings.RANDOM_STATE)
    with mlflow.start_run(tags={"dataset_fingerprint": F.dataset_fingerprint(training_dataset, params)}) as run:
        pipe.fit(X, y)
        roc = float(roc_auc_score(y, pipe.predict_proba(X)[:, 1]))
        mlflow.log_params(params); mlflow.log_metric("roc_auc_train", roc)
        info = mlflow.sklearn.log_model(pipe, name="model",
                                        serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE)
    return dg.Output({"run_id": run.info.run_id, "model_uri": info.model_uri, "exp_id": exp_id,
                      "roc_auc_train": roc, "params": params},
                     metadata={"mlflow_url": dg.MetadataValue.url(mlflow_res.run_url(exp_id, run.info.run_id)),
                               "roc_auc_train": round(roc, 4), **params})

@dg.asset(group_name="ml", kinds={"sklearn"},
          description="Оценка на отложенной выборке — отдельный ассет; quality gate — blocking check на нём.")
def model_evaluation(model: dict, duckdb: DuckDBResource, mlflow_res: MlflowResource, config: TrainConfig) -> dg.Output[dict]:
    mlflow_res.setup()
    _, test = F.split_by_hash(_mart(duckdb), config.test_frac, settings.RANDOM_STATE)
    pipe = mlflow.sklearn.load_model(model["model_uri"])
    X, y = test[FEATURES], test[settings.ML_TARGET]
    m = F.evaluate(pipe, X, y)                       # roc_auc, pr_auc, accuracy, positive_rate, proba
    pngs = {"roc_curve": F.roc_png(y, m["proba"]),
            "feature_importance": F.importance_png(pipe, X, y, settings.ML_IMPORTANCE_MAX_ROWS, settings.RANDOM_STATE)}
    with mlflow.start_run(run_id=model["run_id"]):
        mlflow.log_metrics({k: v for k, v in m.items() if isinstance(v, float)})
    scalars = {k: v for k, v in m.items() if k != "proba"} | {"n_test": len(test)}
    return dg.Output(scalars, metadata={
        "metrics": dg.MetadataValue.md(F.md_table(scalars)),
        **{k: dg.MetadataValue.md(f"![{k}](data:image/png;base64,{base64.b64encode(v).decode()})") for k, v in pngs.items()},
        "roc_auc": round(m["roc_auc"], 4), "pr_auc": round(m["pr_auc"], 4), "rows_test": len(test)})

@dg.asset(group_name="ml", kinds={"mlflow"},
          description="Регистрация версии в реестре MLflow; алиас champion; идемпотентно по dataset_fingerprint.")
def model_registered(model: dict, model_evaluation: dict, mlflow_res: MlflowResource) -> dg.MaterializeResult:
    mlflow_res.setup()
    client, name = mlflow.MlflowClient(), settings.MLFLOW_MODEL_NAME
    fp = client.get_run(model["run_id"]).data.tags["dataset_fingerprint"]
    existing = next((v for v in client.search_model_versions(f"name='{name}'")
                     if client.get_run(v.run_id).data.tags.get("dataset_fingerprint") == fp), None)
    version = existing or mlflow.register_model(model["model_uri"], name)
    client.set_registered_model_alias(name, "champion", version.version)
    return dg.MaterializeResult(metadata={
        "model_name": name, "model_version": int(version.version), "reused_existing": existing is not None,
        "model_uri": f"models:/{name}@champion",
        "mlflow_url": dg.MetadataValue.url(mlflow_res.run_url(model["exp_id"], model["run_id"]))})
```

- [ ] **Step 4** `src/olist_ml/defs/ml/checks.py`:

```python
import dagster as dg
from olist_ml.settings import settings
from olist_ml.defs.ml import features as F
from olist_ml.defs.ml.assets import model, model_evaluation

@dg.asset_check(asset=model_evaluation, blocking=True,
                description="Quality gate: ROC AUC на отложенной выборке не ниже порога из settings.")
def quality_gate(model_evaluation: dict) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(passed=F.gate(model_evaluation["roc_auc"], settings.ML_MIN_ROC_AUC),
                               severity=dg.AssetCheckSeverity.ERROR,
                               metadata={"roc_auc": model_evaluation["roc_auc"], "threshold": settings.ML_MIN_ROC_AUC})

@dg.asset_check(asset=model_evaluation, description="PR AUC не ниже порога (неблокирующий: дисбаланс 6.8 %).")
def pr_auc_floor(model_evaluation: dict) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(passed=F.gate(model_evaluation["pr_auc"], settings.ML_MIN_PR_AUC),
                               severity=dg.AssetCheckSeverity.WARN,
                               metadata={"pr_auc": model_evaluation["pr_auc"], "threshold": settings.ML_MIN_PR_AUC})

(roc_auc_within_bounds,) = dg.build_metadata_bounds_checks(
    assets=[model], metadata_key="roc_auc_train", min_value=settings.ML_MIN_ROC_AUC, max_value=1.0)
```

- [ ] **Step 5** `jobs.py`: `ML = dg.AssetSelection.groups("ml")`; `ml_job = define_asset_job("ml_job", selection=ML, description="Демо 3 — ML lifecycle; витрину не пересчитывает.")`; `full_pipeline_job = define_asset_job("full_pipeline_job", selection=RAW | DBT | ML, description="Всё одним запуском: ingest → dbt → ML. Порядок выводится из графа.")`; `automation/schedules.py`: `full_pipeline_schedule = dg.ScheduleDefinition(job=full_pipeline_job, cron_schedule=settings.FULL_PIPELINE_CRON, default_status=dg.DefaultScheduleStatus.STOPPED)`.
- [ ] **Step 6** Failing integration `tests/integration/test_ml_pipeline.py`: tmp DuckDB с фикстурами → `full_pipeline_job.execute_in_process()` → `success`; `model_registered` metadata `model_version == 1`; tmp MLflow содержит 1 run и 1 версию; второй запуск → `reused_existing is True`, версия 1; `monkeypatch.setenv("ML_MIN_ROC_AUC", "0.99")` → `quality_gate` failed и `model_registered` не материализован (blocking); `ML_MODEL=logreg` — pipeline тоже проходит. На фикстуре в 500 заказов gate снижается через `tmp_settings` до 0.5 (тест проверяет механику, не качество). → PASS.
- [ ] **Step 7** `make mlflow &`, `make dev` → Materialize `full_pipeline_job` от `raw/*` до `model_registered`; в UI — `mlflow_url`, ROC, таблица метрик; `make dev-check`.
- [ ] **Step 8** progress + decisions D5, D6. **Commit:** `feat(ml): mart_order_features, training→model→evaluation→registry with MLflow, quality gate, full_pipeline_job` → push.

### Task S5: CI

**Files:** Create `.github/workflows/ci.yml`; Modify `Makefile` (`ci: install check test`).

- [ ] **Step 1** `ci.yml`: `on: push (main) / pull_request / workflow_dispatch`; `permissions: contents: read`; `concurrency: ci-${{ github.ref }}`; job `ci` (`ubuntu-latest`, `actions/checkout@v4`, `astral-sh/setup-uv@v10.1.0` с `python-version: "3.12"`, `enable-cache: true`) — шаги строго `make install`, `make check`, `make test`; job `e2e` (`needs: ci`, `if: github.event_name != 'pull_request'`) — `make test-e2e`.
- [ ] **Step 2** Локально `make ci` зелёный; пуш → зелёный Actions в `dataengy/mlinside-demo`.
- [ ] **Step 3** progress. **Commit:** `ci: GitHub Actions on make targets (install/check/test, e2e on main)` → push.

### Task S6: полное покрытие тестами, e2e, идемпотентность

**Files:** Create `tests/e2e/test_full_pipeline.py` (`@pytest.mark.e2e`: `clean_all_job` → `full_pipeline_job` → чеки/метаданные → повтор → то же состояние), `tests/integration/test_idempotency.py` (seed×2, dbt×2, ml×2 — row counts и версия модели), `tests/integration/test_style_parity.py` (`INTEGRATIONS_STYLE=yaml` vs `python` — одинаковые множества ключей/групп/чеков/джоб; `xfail(strict=True)` до появления `integrations_python`), `tests/unit/test_telegram.py` (мок `httpx.Client.post`); Modify `Makefile` (`test`, `test-e2e`, `test-all`).

- [ ] Каждый тест: failing → run → implement → PASS → `git commit -m "test(...)"`. Финал: `make test-all` зелёный; `docs/progress.md` — все шаги этапа 1 отмечены; отчёт «что сделано / что не удалось / что проверить руками перед записью» в `docs/progress.md#Отчёт этапа 1`.

---

## 6. Этап 2 — лейны (параллельно, свои файлы, гейты по `docs/progress.md`)

Каждый лейн — отдельный субагент (`superpowers:subagent-driven-development`), стартует, когда в `docs/progress.md` появляется его гейт; пишет **только** в свои пути; общие файлы — под `agent-session-lock --object <file-slug>` аддитивным диффом; перед пушем `git pull --rebase`; при конфликте — стоп и сообщение пользователю. Строки прогресса — в `## Этап 2`.

| Лейн | Гейт | Владеет | Общие файлы (под lock) | Тесты |
|---|---|---|---|---|
| **L-A dlt из Kaggle (демо 1 v2)** | `dbt-seed-ingest` | `defs/ingest_dlt/**`, `integrations_python/ingest.py`, `scripts/download_data.py` | `settings.py` (Kaggle-поля), `Makefile` (`data`), `.env.example`, `definitions.py` (выбор ingest-модуля по `INGEST_MODE`) | unit `sample_connected`; integration dlt→tmp DuckDB на фикстурах (kagglehub замокан), replace×2 = одно состояние; smoke: при `INGEST_MODE=dlt` те же ключи `raw/*` |
| **L-B transform/dq + freshness (демо 2 v2)** | `dbt build` | `defs/dbt/freshness.py`, `integrations_python/dbt.py` | `jobs.py` (`transform_job`, `dq_job`), `dagster.yaml` | integration: `dq_job` без материализаций, `transform_job` без чеков; `source_freshness` при `DBT_FRESHNESS_WARN_DAYS=1` → WARN; `test_style_parity` |
| **L-C maintenance** | `dbt-seed-ingest` / `dbt build` / `MLflow` для каждой clean_* | `defs/maintenance/**` | `Makefile` (`clean-*` через `uv run dg launch --job <name>`; если флага `--job` нет — `uv run dagster job execute -m olist_ml.definitions -j <name>`) | integration: clean на пустом состоянии — успех; на заполненном — таблиц/эксперимента нет; e2e `clean_all → full_pipeline → то же` |
| **L-D ML-наглядность + автоматизация** | `MLflow` | `defs/automation/sensors.py::incoming_file_sensor` (курсор по `data/incoming/`), `AutomationCondition.eager()` на ML-ассетах (опц.) | `defs/ml/assets.py` (только если нужны доп. метаданные) | unit сенсора (курсор, `run_key`, `SkipReason`) через `build_sensor_context` |
| **L-E Docker + Grafana + Telegram (демо 4)** | `MLflow` (compose), `CI` (docker-test) | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `deploy/dagster.prod.yaml`, `observability/**`, `alerts/telegram.py`, `defs/automation/sensors.py::alert_on_run_failure` — стартует с черновиков `.claude/drafts/observability/` (compose прошёл `docker compose config` с обоими профилями; 10 YAML + dashboard JSON валидны; экспортер компилируется) и `.claude/drafts/ci/{Dockerfile,.dockerignore}` | `Makefile` (`docker-*`, профили всегда парой), `.env.example` (`POSTGRES_*`, `GRAFANA_*`, `TG_*`, `MLFLOW_SERVER_ALLOWED_HOSTS`, `EXPORTER_PORT=9101`) | unit: `format_run_failure`, `send_telegram` на моке httpx; ручная проверка: `docker pull` закреплённых образов (`grafana/grafana:12.3.1`, `prom/prometheus:v3.7.3`, `grafana/loki:3.5.9`, `grafana/alloy:v1.12.1`, `ghcr.io/mlflow/mlflow:v3.16.1`), `make docker-up-observability` → граф, `full_pipeline_job`, dashboard, сломанный чек → алерт в Telegram |
| **L-F docs** | `dbt build` (черновик runbook 1–2), `MLflow` (3), `тесты` (финал), L-E (4) | `docs/runbook.md`, `docs/deploy/**`, `docs/observability.md`, `data/README.md`, `README.md` | `docs/decisions.md` (аддитивно) | `make check` включает проверку ссылок (`scripts/check_md_refs.py`, опц.) |
| **L-H batch-инференс + `SIM_TODAY` (демо 3, слайд 37)** | `MLflow` | `defs/ml/inference.py` (`predictions`, `prediction_monitoring`, checks), `defs/ml/simtime.py` | `settings.py` (`SIM_TODAY`, `SIM_START`, `ML_MODEL_ALIAS`, пороги positive rate / PSI), `jobs.py` (`inference_job`), `defs/ingest*/` и `defs/ml/assets.py` (фильтр `purchase_date <= SIM_TODAY`, минимальный diff), `.env.example` | unit: `psi()`, фильтр по `SIM_TODAY`; integration: партиция `predictions` ×2 = одно состояние (без дублей), `model_version` в строках и метаданных, загрузка по алиасу из tmp MLflow; e2e: обучение на окне → бэкфилл 3 партиций → checks видны |
| **L-0 git-split** | ревью плана + предусловия §4 | `scripts/step1-restructure.sh` | — | dry-run на временном клоне выполнен |
| **L-G agentic BRD-watch** | нет | `~/.ai/...` через скиллы пользователя или `apply.sh` | — | pipe-тест хука `echo '{}' \| bash hook` → exit 0 |

**L-A (dlt).** Шаг 0 — практическая проверка «ловушки» компонента: материализовать `raw/*` дважды под `dg dev` без рестарта (черновик `mlinside-dagster-demo` описывает протухший `DltSource` при повторном extract в одном процессе; для компонента документацией не подтверждено и не опровергнуто); если воспроизводится → основной вариант ingest v2 = `@dlt_assets` из `integrations_python/ingest.py` (там источник создаётся на каждый запуск), запись в `decisions.md`. Черновики лейна (`.claude/drafts/ingest/`: `sampling.py` + 8 зелёных тестов, `kaggle_olist_source.py`, `loads.py`, `defs.yaml`) переносятся в `defs/ingest_dlt/`. `kaggle_olist.py`: `@dlt.source(name="kaggle_olist") def kaggle_olist(sample_frac=settings.SAMPLE_FRAC, seed=settings.RANDOM_STATE, dataset=settings.KAGGLE_DATASET, cache_dir=settings.abs(settings.KAGGLEHUB_CACHE), geo_max_rows=settings.GEO_MAX_ROWS)` → `path = kagglehub.dataset_download(dataset)` (`KAGGLEHUB_CACHE` в env) → 9 CSV (`encoding="utf-8-sig"`, `dtype=str`) в `dict[str, DataFrame]` → `sampled = sample_connected(tables, sample_frac, seed, geo_max_rows)` (одна общая выборка заказов для всех ресурсов) → `[dlt.resource(lambda t=t: sampled[t], name=t, write_disposition="replace") for t in settings.RAW_TABLES]`; `pipeline()` → `dlt.pipeline(pipeline_name="kaggle_olist", destination=dlt.destinations.duckdb(str(DUCKDB_PATH)), dataset_name=settings.RAW_SCHEMA, pipelines_dir=str(DATA_DIR/"dlt"))`; `defs/ingest_dlt/defs.yaml`: `type: dagster_dlt.DltLoadCollectionComponent`, `loads: [{source: .loads.kaggle_olist_source, pipeline: .loads.kaggle_olist_pipeline, translation: {key: "raw/{{ resource.name }}", group_name: raw, deps: []}}]` (если строка с `/` не даёт составной ключ — форма списка `["raw", "{{ resource.name }}"]`; проверяется smoke-тестом ключей). Проверено лейном на dlt 1.30.0: `pipeline.run` при `sample_frac=0.02` — 5.4 с, повтор 2.9 с с теми же row counts (orders 1885, order_items 2147, customers 1885, geolocation 276 321 — отсюда `geo_max_rows`). **Переключение v1/v2 (кандидат D14):** в `definitions.py` yaml-режим тоже собирается из списка модулей — `_COMMON + ("olist_ml.defs.ingest" if INGEST_MODE == "seed" else "olist_ml.defs.ingest_dlt") + ("olist_ml.defs.dbt",)` вместо `load_from_defs_folder` (иначе оба ingest-компонента загрузятся и дадут дубли `raw/*`); scaffold-строка остаётся в комментарии как «то, что было». `sample_frac` через `dg.Config` на уровне компонента невозможен — значение из settings при импорте; run-time конфиг — только в `@dlt_assets`-варианте (`integrations_python/ingest.py`) — в `decisions.md`.

**L-B (freshness).** `defs/dbt/freshness.py`:

```python
import json, dagster as dg
from dagster_dbt import DbtCliResource
from olist_ml.settings import settings

@dg.multi_asset_check(specs=[dg.AssetCheckSpec(name="source_freshness", asset=dg.AssetKey(["raw", t]))
                             for t in settings.RAW_TABLES],
                      description="dbt source freshness → статус на каждом raw/* (pass/warn/error).")
def source_freshness(dbt: DbtCliResource):
    inv = dbt.cli(["source", "freshness"], raise_on_error=False); inv.wait()
    results = {r["unique_id"].split(".")[-1]: r
               for r in json.loads((inv.target_path / "sources.json").read_text())["results"]}
    for t in settings.RAW_TABLES:
        r = results.get(t) or {}; status = r.get("status", "pass")        # без freshness в yml → pass
        yield dg.AssetCheckResult(asset_key=dg.AssetKey(["raw", t]), check_name="source_freshness",
                                  passed=status != "error",
                                  severity=dg.AssetCheckSeverity.WARN if status == "warn" else dg.AssetCheckSeverity.ERROR,
                                  metadata={"status": status, "max_loaded_at": str(r.get("max_loaded_at"))})
```

  `FreshnessPolicy.time_window(warn_window=timedelta(minutes=settings.FRESHNESS_WARN_MIN), fail_window=timedelta(minutes=settings.FRESHNESS_FAIL_MIN))` на `raw/*` и `mart_order_features` — через `post_processing` компонента, если 0.29.23 поддерживает `freshness_policy` в атрибутах (проверить первым шагом лейна), иначе `Definitions.map_asset_specs` одной строкой в `definitions.py`.

**L-H (batch-инференс + `SIM_TODAY`).** Обоснование и контекст — [`docs/overview.md` §6](../docs/overview.md) (контур B и §6.5); пункты 1 и 5 из §6.7. Остальные кандидаты §6.7 (поздние метки, challenger/champion, time-split, ClickHouse) — **не в скоупе**, кандидаты на после лекции.

- **`SIM_TODAY` — «виртуальное сегодня»** для исторического Olist (2016-09 … 2018-10): `settings.SIM_TODAY: date | None` (None = без ограничения, поведение этапа 1 не меняется) и `SIM_START`. Ingest и обучение видят только заказы с `purchase_date <= SIM_TODAY`; фильтр — в одном месте (`simtime.py::visible(df_or_sql)`), не размазан по ассетам. Сдвиг — через `.env`/Launchpad; расписание «+1 день» — опционально.
- **`predictions`** — `DailyPartitionsDefinition(start_date=settings.SIM_START, end_date=SIM_TODAY+1)` по дню покупки. Партиция = заказы дня из `mart_order_features` (признаки без таргета). Модель — `mlflow.pyfunc.load_model(f"models:/{settings.ML_MODEL_NAME}@{settings.ML_MODEL_ALIAS}")` в момент запуска; `deps=[model_registered]` только для связности графа. Колонки: `order_id, day, score, pred, model_version, scored_at`. Запись идемпотентна: `delete where day = ?` + `insert` в одной транзакции DuckDB. Метаданные: `model_version`, `n_rows`, `positive_rate`. Автоматизация — `AutomationCondition.eager()` (только последняя партиция); **обновление модели не пересчитывает историю** — бэкфилл из UI явно, запись в `decisions.md`.
- **`prediction_monitoring`** — asset checks на `predictions`: `positive_rate_within_bounds` (WARN), `score_psi_vs_train` (PSI распределения скоров партиции против holdout обучения, порог из settings, WARN). Эти checks подхватывает правило алертинга из L-E.
- **Сценарий демо** (в runbook через L-F): `SIM_TODAY=2017-12-31` → `full_pipeline_job` (обучение на 2017) → `SIM_TODAY=2018-03-31` → бэкфилл `predictions` 2018-01…03 → в февр.–марте растёт positive rate / PSI → checks жёлтые → алерт. Проверить на реальных данных, что сдвиг действительно даёт срабатывание; если нет — подобрать порог и записать в `decisions.md`, данные не подкручивать.

---

## 7. Оркестрация параллельных агентов

1. Оркестратор держит `PLAN.md`, `docs/progress.md`, `docs/decisions.md`; каждому лейну — свежий субагент с брифом: гейт, владение, интерфейсы из §5/§6, команды тестов, правило коммита («только свои пути, Conventional Commits, `git pull --rebase`, при конфликте — стоп»).
2. Лок: `JF=~/.ai/skills/_scripts/session/Justfile; just -f "$JF" agent-lock acquire --repo <mlinside-demo> --object settings-py --reason "L-A: Kaggle-поля"` → правка → коммит → `release`. `.ai/.locks/` в `.gitignore`. При `HELD-BY-OTHER` — ждать (poll 30 с, до 10 мин), затем сообщить оркестратору.
3. Гейт-поллинг: `grep -q '^- \[x\] dbt build' docs/progress.md` каждые 60 с (не по состоянию кода).
4. Ревью после каждого лейна — `superpowers:requesting-code-review` (свежий ревьюер, sonnet) по diff лейна; правит тот же лейн.
5. Модели/effort: agentic (L-G, ревью, docs) — sonnet/haiku, low; L-A/L-C — sonnet; L-B/L-D/L-E — opus; интеграция и конфликты — оркестратор.
6. Если параллельный запуск невозможен — этап 2 последовательно после этапа 1 тем же планом.

---

## 8. Runbook — что показываем (скелет `docs/runbook.md`; детали пишет L-F)

| Демо | Состояние «до» | Действия | Что показать в UI | «Если пошло не так» |
|---|---|---|---|---|
| 1 (~6 мин) | пустая папка | `uvx create-dagster project olist_ml` → `make dev` → пустой граф → `dg scaffold defs dagster_dbt.DbtProjectComponent ingest` и правка `defs.yaml` (v1) / показать `defs/ingest_dlt/defs.yaml` (v2) → reload → 8 узлов `raw/*` → Materialize `seed_job` | группа `raw`, row count в метаданных, `duckdb data/olist.duckdb "select count(*) from raw.orders"` | fallback-слайд A14; `make clean-ingest && make demo1` |
| 2 (~9 мин) | raw материализован | `dg scaffold defs dagster_dbt.DbtProjectComponent dbt` → `dg list defs` → граф связный → `dbt_build_job` → `transform_job` → `dq_job` → `make demo2-break` (дубль в `seed_state_region.csv`) → `dq_job`: красный чек, модель зелёная → `make demo2-fix` → `DBT_FRESHNESS_WARN_DAYS=30 make dq` → WARN | группы `staging/intermediate/marts`, вкладка Checks, статус Freshness | `make clean-dbt && make demo2` |
| 3 (~7 мин) | marts материализованы | `make mlflow` → `ml_job` → метаданные `model` (ссылка в MLflow), `model_evaluation` (ROC, таблица), `model_registered` (версия, алиас) → `full_pipeline_job` → `ML_MIN_ROC_AUC=0.99` или `ML_MODEL=logreg` в Launchpad → blocking check останавливает регистрацию | Asset → Metadata; MLflow: run + Registered model `champion` | `make clean-ml && make demo3` |
| 4 (~8 мин) | репо запушен | push → Actions зелёный → `make docker-up-observability` → Grafana dashboard → `make demo4-break` → алерт в Telegram (Grafana) и сообщение `run_failure_sensor` (уровень 0) | Grafana, Telegram | fallback A17; `docs/observability.md#troubleshooting` |

---

## 9. Вопросы к пользователю (блокируют шаг 1, не блокируют код этапа 1 в новом репо)

1. **`01-dbt` «как есть» — какое состояние фиксируем?** Сейчас: новая плоская структура только в индексе; README/Makefile/docs/.github удалены в дереве; в индексе полные seeds 120 МБ (`geolocation` 58 МБ > предупреждение GitHub 50 МБ); `olist.duckdb` 106 МБ не игнорируется (push заблокируется). Варианты: (a) закоммитить как есть (репо сразу 120 МБ и столько же при каждом обновлении сидов); (b) в `01-dbt/seeds/` — сэмпл (как в `02-dagster`), полные CSV — `data/raw` по `make data`; (c) git-lfs для `*.csv` (`/git-lfs-setup`). **Рекомендация — (b)** + `demo/**/*.duckdb` в `.gitignore`.
2. **Коммит в `MLInside-course` при грязном дереве** — разрешить путь-скоуп коммит `.gitmodules` + `demo`, или сначала закоммитить/stash всё?
3. **`gh auth refresh -h github.com`** и **`ssh -T git@github.com`** — выполнить до шага 1.4.
4. **Kaggle**: если анонимная загрузка не работает (§12), нужны `KAGGLE_USERNAME/KAGGLE_KEY` в `.env`.
5. **Telegram**: `TG_BOT_TOKEN`, `TG_CHAT_ID` для живой проверки алертов — через `/add-secret`.
6. **Расположение** `PROMPT.md`/`PLAN.md` в новом репо — `02-dagster/.claude/` (по дереву ТЗ) — подтвердить.
7. **Time-based holdout**: оставить хеш-сплит (0.78) с честной оговоркой в `decisions.md`, или сразу показывать time-split (0.71) как «правду MLE»?
8. **Экспортер метрик**: порт 9101 (в ТЗ не было) — оставить? Порог метрики модели в Grafana-правиле — дублировать в `rules.yml` вручную или рендерить `rules.yml` из `.env` маленьким скриптом (в духе `config/.env-render.sh`)?
9. **BRD-watch**: выполнить `REPO=/Users/user/gi/@dataengy/mlinside-hw-olist bash 02-dagster/.claude/drafts/agentic/apply.sh` вне этой сессии (sandbox не даёт писать в `~/.ai`/`~/.claude`), затем `/create-skill reread-brd-and-sync-plan` для промоута кандидата и `sync-project-agents --apply` — согласны?
10. **`.env.example` плоский** (`KEY=value`), без `config/*.template` и `${VAR:-fallback}` — так задаёт дерево ТЗ; это отступление от глобальной конвенции `~/.claude/CLAUDE.md` (`config/.env.config.template` + `.env-render.sh`). Подтвердить плоский вариант или добавить `config/` в структуру?

---

## 10. Риски

| Риск | Вероятность | Мера |
|---|---|---|
| dagster-dbt не даёт seed-узлу и source-узлу один ключ даже в разных компонентах | средняя | S2.7 — интеграционный тест первым; fallback: seeds как `@dbt_assets(select="path:seeds/raw")` в `integrations_python` |
| `dq_job` (только чеки) на компоненте с `build` материализует модели | низкая | Тест L-B; fallback — `@dbt_assets` с `dbt test` как основной для dq |
| Kaggle требует авторизации / нестабилен на записи | средняя | Кеш `kagglehub`; показ пользователю → fallback локальные CSV; демо 1 v1 не зависит от сети |
| ROC AUC ниже 0.70 на неудачном сиде при `SAMPLE_FRAC<0.2` | низкая при 0.2 (мин. 0.726) | `SAMPLE_FRAC=0.2`, порог 0.70; пересчёт порога скриптом лейна при смене признаков |
| `purchase_month` даёт половину качества (память об инцидентах 2017-11, 2018-03) | точно | Честно в `decisions.md` и в описании витрины; вариант без месяца — 0.687 |
| SQLite Dagster в compose — `database is locked` | высокая | Postgres сразу (D8) |
| Нет экспортера метрик Dagster → Prometheus «из коробки» | точно | Sidecar-экспортер (D10) |
| Размер `mlinside-demo` из-за seeds/duckdb в `01-dbt` | высокая при (a) §9 | Вопрос 1 §9; проверка блобов > 50 МБ в скрипте шага 1 |
| Конфликты параллельных агентов в общих файлах | средняя | Object-lock + аддитивные диффы + `git pull --rebase`; при конфликте — стоп |
| Вложенные сабмодули в `MLInside-course` | точно | README обоих репо; `git submodule update --init` без `--recursive`; `make submodules-full` |

---

## 11. Критерии приёмки

- Этап 1: `make install && make data-sample && make dev` из чистого клона поднимает UI с графом `raw → staging → intermediate → marts → mart_order_features → training_dataset → model → model_evaluation → model_registered` одним связным компонентом; `full_pipeline_job` зелёный; `make test-all` зелёный локально и в Actions; `docs/progress.md` заполнен; `docs/decisions.md` — D1, D2, D5, D6, D7, D9.
- Этап 2: `INGEST_MODE=dlt` даёт тот же граф с загрузкой из Kaggle; `seed_job`/`transform_job`/`dq_job` раздельны, падение теста — красный чек при зелёной модели; freshness виден отдельным статусом; `clean_all_job → full_pipeline_job` воспроизводит состояние; `docker compose --profile core --profile observability up` → граф, `full_pipeline_job`, dashboard, алерт в Telegram на сломанном чеке; `docs/runbook.md` (4 раздела с таймингом), `docs/deploy/` (Hetzner VM, Dagster+ Serverless/Hybrid, таблица «стоимость/сложность/что остаётся на мне», mermaid), `docs/observability.md`.

---

## 12. Результаты лейнов-разведки (2026-09-18; без записи в реальные репозитории; артефакты — `.claude/drafts/<lane>/`)

- **git-split — готово.** Split с `dev`: 4 коммита, 123 файла, 3.16 МБ, только старая структура `01-dbt`; новая плоская структура (50 файлов) — только в индексе; seeds 120 МБ в индексе (`geolocation` 58 МБ); `olist.duckdb` 106 МБ untracked и не игнорируется; блобов > 50 МБ в истории нет; `git bundle --all` 2.9 МБ. `gh auth` невалиден; ssh из sandbox не проверяется. Скрипт `step1-restructure.sh` (bash 3.2, `DRY_RUN=1`, `--verify`) — dry-run на клоне прошёл, gh-гейт сработал. Рекомендация лейна: не ставить `update=none` на вложенный сабмодуль, задокументировать дублирование.
- **ML — готово.** См. §0 и D6: HGB 0.782 / logreg 0.675 (frac 0.2), gate 0.70 от минимума по сидам, PR AUC ≥ 0.18, `SAMPLE_FRAC=0.2`, хеш-сплит md5, витрина `mart_order_features.sql` (22 признака, сверена с прототипом на 96 476 строках), нужен макрос `month_of()`, fingerprint 0.01 с, PNG 0.16 с. Файлы: `mart_order_features.sql`, `macro_month_of.sql`, `features_sql.py`, `prototype_train.py`, `artifacts/`, `REPORT.md`.
- **CI / Makefile / packaging — готово** (`.claude/drafts/ci/`): `Makefile` (парсится `make -n`), `.github/workflows/ci.yml` (валиден), `.env.example` (плоский), `src/olist_ml/settings.py` + `tests/unit/test_settings.py` (11/11 на pydantic-settings 2.15.0; `MLFLOW_TRACKING_URI` — `computed_field`, override через поле с `validation_alias`), `pyproject.toml`, `Dockerfile` + `.dockerignore` (не собирался — нет Docker в sandbox); `ruff check` и `ruff format --check` чисто. Правки при переносе: `httpx` — в основные зависимости (нужен `run_failure_sensor` в prod-образе); `dg launch --job` vs `--jobs` — сверить `dg launch --help` на реальном проекте; `uv.lock` появится в S1 (Dockerfile ждёт `--frozen`).
- **Observability — готово** (`.claude/drafts/observability/`): `docker-compose.yml` (профили `core`/`observability`, `docker compose config` пройден), `dagster.prod.yaml`, `exporter/{dagster_exporter.py,Dockerfile,requirements.txt}`, `alloy/config.alloy`, `prometheus/prometheus.yml`, `loki/loki-config.yaml`, `grafana/provisioning/{datasources,dashboards,alerting}` (Telegram contact point, 4 правила, policies), `docs-observability.md`, `decisions.md` (5 ADR), `.env.observability.example`, `makefile-snippet.mk`. Не проверено: реальный подъём стека, доставка в Telegram, отрисовка дашборда, существование тегов образов. Контракты с другими лейнами: `DAGSTER_WORKSPACE=/opt/dagster/app/workspace.yaml`, `APP_IMAGE=olist_ml:local`, `MLFLOW_EXPERIMENT=olist_ml`, `MLFLOW_METRICS=roc_auc`.
- **Ingest / Kaggle — готово** (`.claude/drafts/ingest/`): анонимный `kagglehub` работает (15.85 с холодный / 0.60 с из кеша, 120.3 МБ, хост `api.kaggle.com`), `sampling.py` + `test_sampling.py` (8/8), `kaggle_olist_source.py` (+ найденный и исправленный баг: ресурс возвращал невызванный callable), `loads.py`, `defs.yaml` (черновик компонента; `dg check defs` не гонялся — нет проекта), логи-доказательства. Вывод: начинать с `DltLoadCollectionComponent`, `sample_frac` только из settings при импорте; практический тест «ловушки» повторного extract — первым шагом L-A.
- **Agentic BRD-watch — собрано, установка за пользователем** (`.claude/drafts/agentic/`): `hooks/brd-watch-hook.sh` (протестирован на scratch-репо), `skills/reread-brd-and-sync-plan.candidate.md`, `agents/brd-watcher.md`, `apply.sh`. Реальный `add-session-hook` прогнан против scratch-репо: скелет/settings/yq-мерж прошли, запись `settings.local.json` упала только на ограничении heredoc в этой песочнице; JSON-мерж проверен напрямую. Проверка после установки: `echo '{}' | bash <repo>/scripts/session-hooks/brd-watch-hook.sh` → exit 0; правка PROMPT.md + повтор → предупреждение.

Самопроверка покрытия ТЗ: демо 1 v1/v2 — S2, L-A; демо 2 v1/v2 + freshness — S3, L-B; демо 3 — S4, L-D; демо 4 — S5, L-E, L-F; шаг 1 — §4, L-0; правило скаляров — S1 (settings + тест паритета с `.env.example`); идемпотентность — тесты ×2 в S2/S3/S4, S6; maintenance — L-C; четыре уровня тестов — S1–S6 и лейны; без Docker — Makefile S1; с Docker — L-E; docs — L-F + каждая задача; параллельная работа и блокировки — §7, D12; fallback pythonic для интеграций — D1, `integrations_python`, `test_style_parity`.
