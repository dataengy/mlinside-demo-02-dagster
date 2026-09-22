# Runbook — установка, запуск, сбои

> Операционный документ. Слайды показа — [`demo/`](../demo/README.md), план блоков — [`demo/`](../demo/README.md);
> решения — [`decisions.md`](decisions.md).
> Команды `just …` появляются по шагам [`../.claude/TODO.md`](../.claude/TODO.md); пометка **⚠ проверить** —
> ещё не подтверждено на установленных версиях.

## 1. Версии (зафиксированы на 2026-09-20)

| Компонент | Версия | Откуда |
|---|---|---|
| Python | 3.12 (через `uv python pin 3.12`) | uv 0.12.7 |
| dagster / dagster-webserver / dagster-dg-cli | 1.13.23 | pin в `pyproject.toml` (M0) |
| dagster-dbt / dagster-duckdb | 0.29.23 | требует `dbt-core >=1.7,<1.13` |
| dbt-core / dbt-duckdb | 1.12.x / 1.11.0 | канон `mlinside-hw-olist` — 1.11.x, тоже совместим |
| duckdb | 1.5.5 | |
| mlflow | 3.15–3.16 | alias API `set_registered_model_alias`, `models:/<name>@champion` (⚠ проверить на M3/M4) |
| scikit-learn / pandas | 1.9.x / 2.3.x | |
| just | 1.58 (`brew install just` / `uv tool install rust-just`) | ADR-15 |

Что из component schemas/команд проверяется на этих версиях **до** попадания в DEMO — decisions ADR-17 и план-аудит:
scaffold-команда и `defs.yaml` компонента; `prepare_if_dev` vs manifest при сборке; отдельный запуск checks
(`DBT_INDIRECT_SELECTION`) и blocking провала dbt-теста для ML-ветки; статусы UI после правки SQL (default
`code_version`); `FreshnessPolicy.time_window` + `freshness.enabled`; `dg launch --job`.

## 2. Установка

```bash
git clone git@github.com:dataengy/mlinside-demo-02-dagster.git && cd mlinside-demo-02-dagster
brew install just                # или uv tool install rust-just
just install                     # uv sync (+ dbt deps)
cp -n .env.example .env          # секретов для локального прогона не нужно
just demo-prepare                # raw-snapshot → DuckDB, baseline-версия модели без алиаса
```

Snapshot (`dbt/seeds/raw/*.csv`, ~8 МБ) хранится через git-lfs: при клоне нужен `git lfs install` (один раз);
если файлы пришли как LFS-указатели — `git lfs pull`.

## 3. Запуск и рецепты

| Рецепт | Что делает |
|---|---|
| `just dev` | `dg dev` на порту из `.env` (`DAGSTER_PORT=3000`) |
| `just mlflow` | MLflow UI на `MLFLOW_PORT=5001` (5000 на macOS занят AirPlay) |
| `just check` | `dg check defs` + `ruff check` + `dbt parse` — без сети, без UI |
| `just test` / `just test-e2e` / `just test-all` | smoke+unit+integration / e2e / всё |
| `just feature-mart` / `just dq` / `just train` / `just promote [version]` / `just score` | пять раздельных операций сюжета (`dg launch --job …`; promote с версией — через `--config-json`) |
| `just demote` | снять alias champion (S15: `predictions` завершается ошибкой «выполните promote») |
| `just measure-gate` | порог gate по 5 сидам на текущей витрине |
| `just demo-prepare` | подготовка raw и зависимостей; **не** обучает, **не** делает promotion |
| `just demo-break` / `just demo-fix` / `just demo-sql-change` | заготовленные патчи для блоков 2 и 5 DEMO |
| `just dbt-sync` | ⚠ временно отключено (`scripts/sync_dbt_from_canonical.sh` завершается с ошибкой) — `dbt/` упрощается до одной витрины `mart_order_features` + upstream, полный rsync из канона стёр бы урезанное дерево; см. `dbt/README.md` |
| `just seeds-sample` | snapshot raw (`SNAPSHOT_N_ORDERS`) → `dbt/seeds/raw`, фикстуры → `tests/fixtures/raw`; вход — полные CSV Kaggle |
| `just dbt-parse` | manifest.json (ADR-04b); после правки SQL — + Reload в UI |
| `just clean raw|derived|all` | очистка данных (ADR-10): raw — схема raw; derived — dbt-схемы, snapshot ML, MLflow (эксперимент + реестр); `just clean-build` — только кеши сборки |
| `just demo-recompute` | выборочный пересчёт: витрина + её ML-потребители (S17) |
| `just ci` | то же, что CI: install → check → test |

Проверка здоровья без UI: `uv run dg list defs` (все ключи и джобы), `uv run dg check defs`, GraphQL
`POST $DAGSTER_GRAPHQL_URL {"query":"{ repositoriesOrError { ... on RepositoryConnection { nodes { name } } ... on PythonError { message } } }"}`.

## 4. Scaffold (как проект создавался; для блока 1 DEMO)

```bash
uvx create-dagster@1.13.23 project olist_ml     # проверено (M0): pyproject с [tool.dg], definitions.py = load_from_defs_folder
cd olist_ml && uv python pin 3.12 && uv add dagster-dbt==0.29.23 dagster-duckdb==0.29.23 dbt-duckdb mlflow scikit-learn
uv run dg scaffold defs dagster_dbt.DbtProjectComponent dbt --project-path dbt   # проверено (M5): defs/dbt/defs.yaml
uv run dg dev
```

`manifest.json`: компоненты работают с `prepare_if_dev: false` (ADR-04b) — manifest собирает `just dbt-parse`
(входит в `install`/`check`/`dev`); после правки SQL — `just dbt-parse` + Reload definitions в UI. Сеть нужна
только `just install` (`uv sync` + `dbt deps`). Живой scaffold в кадре — только во временной папке.

## 5. Типовые сбои

| Симптом | Причина | Что делать |
|---|---|---|
| `dg dev`: definitions не загружаются, ошибка dbt manifest | не выполнен `dbt deps` / нет `profiles.yml` | `just install` (включает `dbt deps`); `DBT_PROFILES_DIR` не нужен — `profiles.yml` в `dbt/` |
| `database is locked` / `Could not set lock on file` DuckDB | второй писатель (открытый DBeaver, параллельный run) | закрыть клиенты; executor `in_process`, `max_concurrent_runs: 1` |
| MLflow UI 403 / не открывается | порт 5000 занят AirPlay; `MLFLOW_SERVER_ALLOWED_HOSTS` в compose | `MLFLOW_PORT=5001`; локально compose не используется |
| `predictions` падает «нет опубликованной модели» | alias `champion` не установлен | `just promote` (после успешного `train`) или alias в MLflow UI |
| `quality_gate` красный на честном пороге | порог из `.env` не перемерен на текущем snapshot | пересчитать по 5 сидам (M3), обновить `.env.example` |
| seeds — LFS-указатели вместо CSV | `git lfs` не установлен | `git lfs install && git lfs pull` |
| Freshness всегда UNKNOWN | демон freshness выключен | `dagster.yaml`: `freshness: {enabled: true}`; перезапуск `dg dev` |
| Telegram-алерт не приходит | `ALERTS_ENABLED=false` (dry-run) или пустые `TG_*` | заполнить `.env`, `just tg-test`; лог сенсора `alert_on_run_failure` (Automation) |
| Сенсор тикает SKIPPED «empty result» без лога после упавшего run | run без `remote_job_origin` (`dg launch`) отфильтрован по code location | `monitor_all_code_locations=True` в `@run_failure_sensor` |
| `$DAGSTER_HOME ".dagster_home" must be an absolute path` при `dg launch`/`dg dev` | в `.env` задан относительный `DAGSTER_HOME`: dagster CLI подгружает `.env` из cwd поверх окружения | убрать `DAGSTER_HOME` из `.env` (его задаёт Justfile абсолютным) |
| Сенсор падает «cursor that is not run-aware» | `get_event_records` с числовым курсором на SQLite-инстансе (run-sharded event log) | курсор по run'ам (`get_run_records(updated_after=…)` + `all_logs`), как в `alert_on_failed_check` |
| `dbt/dbt/target` появился, манифест не обновляется | относительный `DBT_TARGET_PATH` в `.env` берётся dbt от каталога проекта | Justfile экспортирует абсолютные `DBT_*`, `defs/env.py` абсолютизирует их при загрузке кода |
| CI: `failed to fetch some objects from …/info/lfs` | LFS-объекты seeds не загружены на GitHub | `git lfs push --all origin` (нужен push-доступ) |
| `just`: command not found | раннер не установлен | `brew install just` / `uv tool install rust-just` |

## 6. Расширенное демо (не MVP)

Compose core + Grafana/Prometheus/Loki, Evidently-отчёт, dlt из Kaggle — черновики в `.claude/drafts/`,
описание в `docs/deploy/` и `docs/observability.md` появится после MVP (TODO P1–P3).
