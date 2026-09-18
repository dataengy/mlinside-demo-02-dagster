# Промпт для Claude Code: mlinside-demo / 02-dagster

> Черновик. Запускать Claude Code из `/Users/user/gi/@dataengy/`.
> Этот файл — источник правды по задаче; при уточнениях правь его, а не переписывай в чате.

---

## Контекст

Я готовлю лекцию «Оркестрация ML-пайплайнов на Dagster» для курса MLInside (аудитория — MLE и MLOps). Нужен демо-проект на четыре демо, идущих по нарастающей: от пустой папки до наблюдаемого пайплайна с алертами.

Исходники:

- `/Users/user/gi/@dataengy/mlinside-hw-olist/dbt` — dbt-проект Olist, показанный на предыдущей лекции по dbt. Это **канонический dbt-проект**, его модели и тесты переиспользуем, а не переписываем.
- `/Users/user/gi/@dataengy/mlinside-dagster-demo` — мой черновой Dagster-репозиторий на синтетических данных. Забираем оттуда паттерны: translator, ML-ассеты с MLflow, runbook. Особо посмотри `dags/integrations_yaml` — YAML-подход к интеграциям, который я хочу сделать основным.
- `/Users/user/gi/@dataengy/mlinside-hw-olist/demo` — текущая папка демо (там уже лежит демо по dbt — проверь, как называется).
- `/Users/user/gi/@dataengy/MLInside-course/` — репозиторий курса, в него нужно подключить демо как корневую папку `demo`.

Перед началом: прочитай все четыре места, составь план в `PLAN.md` и **покажи его мне до реструктуризации git** — шаг 1 необратим.

### Приоритеты (в порядке убывания)

1. **Простота** — минимум файлов, слоёв и магии; всё, что можно объяснить одной фразой на лекции.
2. **Надёжность** — демо повторяется из чистого клона без ручных правок; зелёные тесты.
3. **Современность подходов** — компоненты, `dg`, `defs.yaml`, declarative automation, а не legacy API.
4. **Выразительность для демо** — граф в UI «читается» с первого взгляда, метаданные кликабельны, каждый шаг даёт видимый результат.

Соответствие слайдам приоритетом **не является** — слайды я поправлю под демо. Если ради простоты или выразительности стоит отклониться от сценариев ниже — отклоняйся и фиксируй в `docs/decisions.md`, что и почему изменилось.

---

## Четыре демо

### Демо 1 — инициализация проекта и ingest

Сценарий: пустая папка → `create-dagster` → `dg dev` → пустой граф в браузере → добавляем ingest → в графе появились raw-таблицы → Materialize → данные в DuckDB.

Ingest делается в **две версии**, обе остаются в репо — это и есть сюжет демо «от простого к настоящему»:

**v1 — `dbt seed` (этап 1 реализации).** Сэмплы CSV Olist лежат в `dbt/seeds/`, ingest = отдельная джоба `seed_job`, в графе seed-таблицы — отдельные узлы-родители моделей. Минимум движущихся частей, работает из чистого клона без сети.

**v2 — dlt из Kaggle (этап 2 реализации).** Для MLE-аудитории показательно, что пайплайн сам забирает исходные данные (`olistbr/brazilian-ecommerce`, https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):

- dlt-source `kaggle_olist`: `kagglehub.dataset_download(...)` скачивает датасет в кеш, dlt-resources читают CSV из кеша и грузят в DuckDB. Kaggle-токен — через `EnvVar` (`KAGGLE_USERNAME` / `KAGGLE_KEY`).
- Оформление — `dagster-dlt`: компонент `DltLoadCollectionComponent` в `defs.yaml`, если он покрывает кастомный source; иначе `@dlt_assets` + `DagsterDltResource`. Проверь по актуальной документации, запиши решение.
- Долгая загрузка на записи не проблема — вырежу на монтаже. Повторные запуски — из кеша `kagglehub`.
- **Сэмплирование** сразу после загрузки: `sample_frac` в `dg.Config` (по умолчанию ~0.1–0.2), связность по `order_id`, чтобы join'ы в dbt не рассыпались. Полный объём — только по явному конфигу.
- `kagglehub` должен работать без интерактивного логина при наличии env-переменных. В CI Kaggle **не дёргать** — только фикстуры.
- Ключи dlt-ассетов совпадают с ключами seed-таблиц v1 (или переключаются одним флагом в `settings`), чтобы dbt-sources не менялись при переходе v1 → v2.

Fallback для v2, если Kaggle усложняет демо (auth, нестабильность, обвязка дольше минуты объяснения): dlt из локальных CSV (`data/raw/`, `scripts/download_data.py` + `make data`). Прежде чем откатываться — покажи мне, что именно не работает.

### Демо 2 — dbt: загрузка и DQ как отдельные стадии

Сценарий: существующий dbt-проект Olist → компонент `DbtProjectComponent` в `defs.yaml` (одна команда) → `dg list defs` → граф моделей, `ref()` = ребро → ключ dbt-source = ключ ingest-ассета из демо 1, граф связный → запуск → тесты и freshness видны как отдельные узлы.

**v1 (этап 1 реализации)** — `dbt build` одной джобой `dbt_build_job`: модели как ассеты, тесты как asset checks. Достаточно для работающего end-to-end.

**v2 (этап 2 реализации)** — **две связанные, но раздельные стадии — трансформация и DQ**, чтобы по UI сразу было видно, где сломалось: в загрузке или в качестве данных.

- Стадия 1 — `dbt run` (модели как ассеты).
- Стадия 2 — `dbt test` (dbt-тесты как asset checks) — запускается **отдельно**, после моделей, и падение теста не выглядит как падение модели.
- Оформи это как две джобы (`transform_job`, `dq_job`) или как ассеты + отдельный запуск checks — что позволяет актуальный `dagster-dbt`. Проверь по документации, поддерживает ли компонент раздельный запуск `run` и `test`; если только `build` — сделай `@dbt_assets` рядом и объясни в `decisions.md`. Оба варианта (компонент и декоратор) должны быть в репо, потому что в лекции есть слайды на оба.
- Стадии идемпотентны и имеют свои джобы очистки (см. «Ключевые требования», п. 5–6).
- **dbt source freshness обязателен**: в `sources.yml` — `loaded_at_field` + `freshness: warn_after / error_after`; в Dagster — freshness-checks из dbt-конфига (`build_freshness_checks_from_dbt_assets` или актуальный аналог — проверь). Freshness-нарушение должно быть видно в UI как отдельный статус, не как ошибка модели.
- `seed_job` из демо 1 — отдельная стадия перед `transform_job`.
- `dbt_build_job` из v1 остаётся для сравнения — одна кнопка против трёх стадий.

dbt-проект внутри `02-dagster/dbt/` — копия из `mlinside-hw-olist/dbt` с `profiles.yml` на `dbt-duckdb`, чтобы `02-dagster` был самодостаточен при клоне `mlinside-demo`. Если найдёшь способ подключить без копии (submodule/symlink), который не ломает `dg dev` и Docker, — предложи, но по умолчанию копия.

### Демо 3 — ML и полный пайплайн от ingest до модели

Сценарий: витрина признаков из dbt → обучение → оценка с quality gate → регистрация в MLflow → один Materialize проводит данные от Kaggle до модели в реестре → результат виден «глазами».

- ML-задача простая и быстрая (<10 с обучение на сэмпле): классификация «заказ будет доставлен с опозданием» или регрессия review score. Витрина признаков — одна dbt-модель `mart_order_features` (или переиспользуй существующую mart).
- Ассеты: `training_dataset` → `model` → `model_evaluation` (+ `@asset_check(blocking=True)` как quality gate) → `model_registered`.
- **MLflow**: локальный backend по умолчанию (`sqlite:///mlflow.db` или `./mlruns`), `mlflow ui` через `make mlflow`. Обучающий ассет пишет `run_id`, кликабельный `mlflow_url` (`MetadataValue.url`), метрики в `MaterializeResult`. `dagster-mlflow` — проверь, актуален ли пакет; если он ops-ориентированный или legacy — свой `MlflowResource(dg.ConfigurableResource)` тонкой обёрткой над `mlflow` и запись в `decisions.md`.
- **Наглядность результатов ML — выбери самый простой вариант и обоснуй.** Кандидаты:
  - метаданные в самом Dagster UI: `MetadataValue.md` с таблицей метрик + `MetadataValue.png`/`image` с графиком (ROC, feature importance, распределение предсказаний) — ноль дополнительных инструментов, всё в том же окне, что и граф;
  - Jupyter/marimo-ноутбук на 3–5 ячеек, читающий DuckDB и MLflow — «взгляд аналитика», знакомо MLE;
  - DBeaver — только если нужен именно SQL по DuckDB; скорее всего избыточен.
  Моё предпочтение — **метаданные в Dagster UI как основной путь** + один короткий ноутбук как дополнение, если он влезает в 5 ячеек и не требует объяснений. Реши и запиши.
- `in_process_executor` из-за DuckDB (одна запись в файл) — задокументируй причину.
- Партиции — не обязательны в v1, оставь задел (TODO), в лекции есть слайд.
- Опционально, если время останется: сенсор с курсором на новый файл в `data/incoming/` или `AutomationCondition.eager()` на ML-ассетах — покажи, что пайплайн запускается сам.

### Демо 4 — CI/CD и Observability

Это **демо, а не только документация** — стек должен реально работать локально в Docker Compose.

Сценарий: пуш в ветку → GitHub Actions зелёный (`make check`, `make test`) → `docker compose up` поднимает Dagster + MLflow + Grafana-стек → dashboard с runs и метриками модели → ломаем asset check (или freshness) → алерт приходит в Telegram.

- CI — только `Makefile` + `.github/workflows/ci.yml`, где каждый шаг = вызов Make. `ubuntu-latest`, Python 3.12, кеш `uv`. Без логики в YAML.
- Observability — **Grafana-стек**: Grafana + Prometheus + Loki + Alloy (единый OTel-коллектор для логов и метрик; без Tempo/Mimir). Если Alloy избыточен — OTel Collector, но одним контейнером.
  - Логи dagster-webserver/daemon (структурированные) → Loki.
  - Метрики run duration / failure rate / asset check status → Prometheus (OTel exporter или Dagster GraphQL → exporter — что проще).
  - Метрики модели из MLflow как time-series в Grafana (scrape или прямой datasource — что проще).
  - Один dashboard JSON, provisioning из репо: runs по статусам, длительность материализаций, последние asset checks, метрика модели по времени.
  - **Алертинг — Grafana Alerting → Telegram contact point** (встроенный, без бриджей). Правила: run failed, asset check failed, freshness violated, model metric ниже порога. Contact point и alert rules — provisioning-файлами, чтобы всё поднималось из репо, а не кликами.
  - Дополнительно, как «уровень 0» без инфраструктуры: `@run_failure_sensor` → Telegram Bot API одним `httpx.post`, с unit-тестом на мок HTTP. Показать в лекции как альтернативу для тех, кому Grafana избыточна.
- Деплой в прод — **описать, не отлаживать**, в `docs/deploy/`, с mermaid-диаграммой каждый:
  1. **Hetzner VM** — один CX-сервер, тот же Docker Compose, Caddy для TLS, systemd, GitHub Actions job `deploy` по тегу через SSH (`appleboy/ssh-action` или `rsync` + `docker compose pull && up -d`), секреты GitHub Secrets → `.env`.
  2. **Dagster+** — Serverless (`dagster-cloud ci` из Actions) и Hybrid (agent на той же Hetzner VM). `dagster_cloud.yaml` + workflow. Отметить, что при Serverless MLflow и DuckDB выносятся (S3/GCS + отдельный MLflow), при Hybrid остаются на VM.
  Сравнительная таблица «стоимость / сложность / что остаётся на мне».

---

## Шаг 1. Реструктуризация git: `demo` → submodule

Папка `mlinside-hw-olist/demo` становится отдельным репозиторием `github.com/dataengy/mlinside-demo` и подключается как submodule в двух местах:

- `mlinside-hw-olist/demo` → submodule `dataengy/mlinside-demo`
- `MLInside-course/demo` → submodule `dataengy/mlinside-demo`

Требования:

1. `git status` во всех трёх репозиториях — не начинай при незакоммиченных изменениях, попроси меня закоммитить или stash.
2. Резервная копия `mlinside-hw-olist` (`cp -R` в `~/gi/_backup/<date>/`) до любых history-rewriting операций.
3. Историю папки `demo` сохрани: `git subtree split --prefix=demo -b demo-split` (или `git filter-repo --subdirectory-filter demo` в клоне — что установлено; `git-filter-repo` без спроса не ставь).
4. `gh repo create dataengy/mlinside-demo --public --source=<split-clone> --push` (если `gh` не авторизован — остановись и скажи).
5. В `mlinside-hw-olist`: `git rm -r demo`, `git submodule add git@github.com:dataengy/mlinside-demo.git demo`, коммит.
6. В `MLInside-course`: `git submodule add git@github.com:dataengy/mlinside-demo.git demo`, коммит.
7. Пуши в `mlinside-hw-olist` и `MLInside-course` **не делай** — покажи diff и дождись подтверждения.

Структура `mlinside-demo`:

```
mlinside-demo/
  README.md              # индекс демо, ссылки на лекции
  01-dbt/                # существующее демо по dbt (перенести как есть)
  02-dagster/            # новое, см. ниже
```

---

## Шаг 2. Проект `02-dagster`

### Стек

- Python 3.12, `uv`, `pyproject.toml` с `[tool.dg]`.
- Актуальные версии: `dagster`, `dagster-webserver`, `dagster-dbt`, `dagster-dlt`, `dagster-mlflow` (см. оговорку выше), `dbt-core` + `dbt-duckdb`, `dlt[duckdb]`, `mlflow`, `duckdb`, `scikit-learn`, `pandas`/`polars`, `kagglehub`, `pydantic-settings`.
- **Перед написанием кода сверь API по актуальной документации** (Context7 / docs.dagster.io): компоненты (`defs.yaml`), `dg` CLI, `DbtProjectComponent`, `DltLoadCollectionComponent`, freshness checks из dbt, `AutomationCondition`, `dg.Config`, `MaterializeResult`, asset checks. Не используй `@op`/`@job`-стиль там, где есть asset-эквивалент; `@job` допустим только как именованная выборка ассетов для расписаний и раздельных стадий из демо 2.

### Структура — строго дефолтная от `create-dagster project`

Создай через `uvx create-dagster project olist_ml` (или актуальную команду) и **ничего не переименовывай** — пути должны совпадать с scaffold'ом, чтобы в лекции не объяснять нестандартные места:

```
02-dagster/
  .claude/PROMPT.md           # этот файл
  pyproject.toml
  src/olist_ml/
    definitions.py            # не трогаем руками
    settings.py               # ЕДИНСТВЕННОЕ место констант, см. правило ниже
    resources.py              # DuckDB, MLflow, dlt — из settings
    defs/
      ingest/                 # Демо 1
        defs.yaml | loads.py
      dbt/                    # Демо 2
        defs.yaml
        assets.py             # @dbt_assets-вариант, если нужен
        translator.py         # только если компонент не покрывает маппинг ключей
      ml/                     # Демо 3
        assets.py
        checks.py
      jobs.py                 # seed_job / transform_job / dq_job / dbt_build_job / full_pipeline_job
      maintenance/            # clean_ingest_job / clean_dbt_job / clean_ml_job / clean_all_job
      automation/             # расписания, сенсоры, run_failure_sensor → Telegram
  dbt/                        # копия mlinside-hw-olist/dbt, profiles.yml на dbt-duckdb
  data/raw/                   # .gitignore; только data/README.md в репо
  tests/
    fixtures/                 # sub-sample CSV ≤ 1000 строк на таблицу, связный по order_id
    smoke/  unit/  integration/  e2e/
  scripts/
  docs/
    runbook.md                # пошаговый сценарий по каждому демо: цель, предусловия, команды, что показать в UI, ожидаемый результат, «если пошло не так»
    decisions.md              # ADR-стиль, 3–5 строк каждое
    progress.md               # статус шагов этапа 1 — сигнал для агента этапа 2, см. «План реализации»
    deploy/
    observability.md
  observability/              # grafana/provisioning, prometheus.yml, alloy config, dashboards/*.json
  Makefile
  Dockerfile
  docker-compose.yml
  .github/workflows/ci.yml
  .env.example
```

### Правило: никаких скаляров в коде

Все хосты, порты, пути, URL, имена схем и таблиц, пороги качества, `sample_frac`, имена MLflow-экспериментов, лимиты — только через `settings.py` (`pydantic-settings`, читает `.env`) и оттуда в ресурсы/конфиги. Никаких литералов в ассетах, ресурсах, тестах и Makefile-целях, которые дублируют `.env`.

Плохо:

```python
ui_url = f"http://127.0.0.1:{settings.DAGSTER_PORT}"
```

Хорошо:

```python
ui_url = settings.DAGSTER_UI_URL          # собран в settings из DAGSTER_HOST / DAGSTER_PORT / DAGSTER_SCHEME
mlflow_uri = settings.MLFLOW_TRACKING_URI
duckdb_path = settings.DUCKDB_PATH
```

Составные значения (URL из host+port+scheme, пути из base_dir+имя) собираются **один раз в `settings.py`** как `computed_field`/property, а не в местах использования. `.env.example` содержит все переменные с комментарием, `Makefile` читает тот же `.env`. Тесты берут значения из `settings` с override через фикстуру/`monkeypatch`, не из литералов.

### Ключевые требования к коду

1. **Ключи ассетов сшиваются.** Ключ каждого ingest-ассета (`raw/olist_orders` и т.д.) равен ключу dbt-source через `meta.dagster.asset_key` в `sources.yml`. `dg list defs` — граф должен быть **одним связным компонентом** от Kaggle до `model_registered`.
2. Компоненты (`defs.yaml`) — основной способ подключения интеграций; Python-декораторы — только там, где компонент не покрывает нужное, с записью в `decisions.md`.
3. Всё внешнее — через `dg.EnvVar` / `settings`, `.env.example` полный.
4. Джобы из демо 2 и `full_pipeline_job` (ingest → run → test → ml) — в `jobs.py`, с расписанием на `full_pipeline_job` как пример.
5. **Идемпотентность всей обработки данных** — если это не усложняет заметно. Правило: повторный запуск любой джобы на тех же входах даёт то же состояние (те же таблицы, те же row counts, без дублей). Конкретно:
   - seed / dlt: `write_disposition="replace"` (или `merge` по первичному ключу), никаких append;
   - dbt: `table` или `incremental` с `unique_key`; `--full-refresh` доступен через конфиг джобы;
   - ML: `training_dataset` детерминирован (фиксированный `random_state` из `settings`, стабильная сортировка перед сплитом); MLflow-run на каждый запуск создаётся — это нормально, но регистрация в реестре не плодит версии при неизменной модели (проверка по хешу датасета/параметров в тегах run'а — если это дёшево; иначе задокументируй, что регистрация append-only, и почему);
   - тест `test_idempotency` на каждом уровне: запуск дважды → сравнение состояния.
   Где идемпотентность дорогая (например, `merge` в dlt на DuckDB) — выбирай `replace` и пиши в `decisions.md`.
6. **Джобы очистки входных данных для каждого этапа** — в `defs/maintenance/`, чтобы демо можно было прогнать «с нуля» одной командой:
   - `clean_ingest_job` — дропает raw-таблицы / seed-таблицы в DuckDB, чистит `data/raw/` и кеш `kagglehub` (по флагу);
   - `clean_dbt_job` — дропает схемы staging/marts, чистит `target/`;
   - `clean_ml_job` — удаляет эксперимент и зарегистрированную модель в MLflow, чистит артефакты;
   - `clean_all_job` — все три по порядку.
   Это единственное законное место для `@op`/`@job`-стиля (они ничего не материализуют). Каждая джоба идемпотентна: повторный запуск на пустом состоянии — успех, не ошибка. Make-цели `make clean-ingest / clean-dbt / clean-ml / clean-all` вызывают их через `dg launch`.

### Тесты — обязательны, проект не сдан без зелёного `make test`

| Уровень | Что проверяет | Инструмент |
|---|---|---|
| smoke | `dg check defs`, `dg list defs`, импорт `definitions`, `dbt parse`, наличие всех ожидаемых ключей ассетов и джоб | pytest + subprocess |
| unit | ассеты как функции: сэмплирование (связность по `order_id`), feature engineering, обучение на фикстуре, quality gate, логика сенсора (курсор, `run_key`, `SkipReason`), Telegram-алерт на мок HTTP | pytest, `dg.build_asset_context`, `build_sensor_context` |
| integration | seed/dlt → DuckDB на фикстурах; `dbt run` и `dbt test` раздельно на временной DuckDB; freshness-check на «протухшем» источнике; MLflow-ресурс пишет run в tmp backend; связность графа; **идемпотентность каждого этапа** (дважды → одно состояние); джобы очистки на пустом и на заполненном состоянии | pytest + `tmp_path` |
| e2e | `full_pipeline_job` на фикстурах от ingest до `model_registered`; проверка asset checks и метаданных; `clean_all_job` → `full_pipeline_job` → тот же результат | pytest, marker `e2e` |

- Никаких сетевых вызовов в тестах (Kaggle, Telegram — моки).
- `make test` = smoke + unit + integration; `make test-e2e` отдельно; `make test-all` — всё.
- Тесты проходят без Docker.

---

## Шаг 3. Запуск: без Docker и с Docker

### Без Docker (основной путь, для записи демо)

```
make install       # uv sync
make data-sample   # sub-sample для локального прогона
make mlflow        # mlflow ui в фоне
make dev           # dg dev
make demo1 / demo2 / demo3 / demo4   # команды и подсказки под каждое демо из runbook
make test / test-e2e / test-all
make check         # dg check defs + ruff + dbt parse
make clean
```

Всё поднимается на macOS из чистого клона за `make install && make data-sample && make dev`.

### С Docker (для демо 4 и прод-подобного стенда)

- `Dockerfile` (multi-stage, uv), `docker-compose.yml`: `dagster-webserver`, `dagster-daemon`, `mlflow`, `grafana`, `prometheus`, `loki`, `alloy`; общая volume для DuckDB/mlruns. Профили compose: `core` (Dagster + MLflow) и `observability`, чтобы демо 1–3 можно было гонять без Grafana.
- **Storage Dagster — SQLite или Postgres, реши по правилу:**
  - если SQLite на общей volume между webserver и daemon стабилен (нет `database is locked`, сенсоры тикают, run history видна) и **заметно упрощает** compose — оставляй SQLite; тогда `docs/deploy/` показывает, что для прода меняется только `dagster.yaml` + один сервис;
  - если упрощение незначительное или SQLite нестабилен при двух процессах — Postgres сразу, чтобы dev и prod отличались только `.env`.
  - Решение — в `decisions.md`.
- `make docker-up / docker-down / docker-test / docker-up-observability`.
- `dagster.yaml` для prod-конфигурации — отдельно от dev.
- Проверь: `docker compose up` → граф виден, `full_pipeline_job` проходит, dashboard в Grafana показывает run, алерт-правило срабатывает на сломанном check (Telegram-токен — из `.env`, в CI не нужен).

---

## План реализации: два этапа

### Этап 1 — минимальный рабочий end-to-end (основной агент)

Шаги строго слева направо; каждый следующий начинается только после того, как предыдущий отмечен в `docs/progress.md`:

```
scaffold → dbt-seed-ingest → dbt build → MLflow (train/eval/register) → CI → полное покрытие тестами, всё зелёное
```

Правила этапа 1:

- **Web-GUI на каждом шаге.** Как только шаг собран — ещё до тестов — запусти `make dev` в фоне и убедись, что `dg dev` поднимается без ошибок, ассеты/джобы шага видны в UI, Materialize проходит. Ошибки загрузки definitions или красные узлы — фиксить до перехода дальше. Проверяй через `dg list defs` + `dg check defs` + запрос к GraphQL/HTTP webserver'а (порт из `settings`), не только визуально.
- **Коммит + пуш после каждого шага** и после каждой заметной доработки или прохождения тестов. Light trunk-based: работа в `main` репозитория `mlinside-demo`, мелкие коммиты, Conventional Commits, пуш сразу. Пуш в `mlinside-hw-olist` и `MLInside-course` — по-прежнему только после моего подтверждения.
- После каждого шага — строка в `docs/progress.md`: `- [x] <шаг> — <commit sha> — <дата>`. Это контракт с агентом этапа 2.
- Тесты для шага пишутся в том же шаге, не откладываются на «потом в конце»; но финальный шаг этапа 1 — сквозной прогон `make test-all` и добивание покрытия.

### Этап 2 — всё остальное (параллельный агент)

Запускается **параллельно** с этапом 1, в **той же ветке `main`, без worktrees**. Содержимое этапа 2:

- dlt из Kaggle (демо 1 v2) + сэмплирование;
- раздельные `transform_job` / `dq_job` + dbt source freshness (демо 2 v2);
- идемпотентность и джобы очистки (`defs/maintenance/`), если основной агент не сделал их в своём шаге;
- наглядность ML (метаданные/ноутбук), сенсор/`AutomationCondition`;
- Docker Compose, Grafana-стек, Telegram-алертинг (демо 4);
- `docs/deploy/`, `docs/observability.md`, `docs/runbook.md` для демо 4.

Правила этапа 2:

- **Ожидание зависимостей по `docs/progress.md`.** Каждая задача этапа 2 объявляет, какой шаг этапа 1 ей нужен (например, «dlt-ingest — после `dbt-seed-ingest`», «Compose — после `MLflow`», «раздельные джобы — после `dbt build`»). Пока шаг не отмечен — задача не стартует; агент поллит файл, не гадает по состоянию кода.
- **Блокировки объектов вместо worktrees.** Перед правкой файла, который может трогать основной агент, — взять блокировку; механизм — из моих скиллов: посмотри `~/.ai/skills` и `~/.claude/skills`, найди скилл про параллельную работу агентов / блокировки объектов / lock-файлы и следуй ему. Если подходящего скилла не найдёшь — скажи мне и предложи минимальную конвенцию (например, `.locks/<path>.lock` с owner + timestamp, `git pull --rebase` перед каждым коммитом, коммит только своих файлов по явному списку). Не изобретай сложнее необходимого.
- **Разграничение владения.** Пока этап 1 не закрыт, агент этапа 2 создаёт **новые** файлы и папки (`defs/ingest_dlt/`, `defs/maintenance/`, `observability/`, `docker-compose.yml`, `docs/deploy/`) и не правит файлы этапа 1 (`defs/dbt/`, `defs/ml/`, `jobs.py`, `settings.py`, `Makefile`, `ci.yml`) без блокировки. Если этапу 2 нужно добавить переменную в `settings.py` или цель в `Makefile` — берёт блокировку, делает минимальный аддитивный diff, коммитит, отпускает.
- Свои коммиты и пуши — по тем же правилам, что у этапа 1; перед пушем — `git pull --rebase`, при конфликте — не резолвить молча, а остановиться и написать мне.
- Свои строки в `docs/progress.md` — в отдельном разделе «Этап 2».

Если параллельный запуск окажется технически невозможен (нет способа запустить второго агента или скилл блокировок не работает) — скажи, и делаем этап 2 последовательно после этапа 1, тем же промптом.

---

## Правила работы

- Нестандартные решения — в `docs/decisions.md`.
- `docs/runbook.md` — для каждого из четырёх демо отдельный раздел с таймингом.
- Останавливайся и спрашивай: перед history-rewriting git-операциями, перед `git push` в существующие репозитории (`mlinside-hw-olist`, `MLInside-course`), перед установкой системных пакетов, если API Dagster расходится с описанным здесь, перед откатом v2-ingest на локальные CSV, при конфликтах слияния между агентами.
- В конце каждого этапа: `make test-all` зелёный, отчёт «что сделано / что не удалось / что проверить руками перед записью». После этапа 2 дополнительно: `docker compose up` работает, алерт в Telegram приходит.
