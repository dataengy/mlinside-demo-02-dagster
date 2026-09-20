# DEMO-plan — план показа по блокам (≤30 минут)

> Слайдовая версия для докладчика — [`DEMO.md`](DEMO.md); этот файл — планирование блоков («до», действия, UI,
> fallback). Ревизия v3, 2026-09-20. Рассказчик — DE; аудитория — MLE/MLOps. Тезис: **Dagster соединяет
> dbt-модели и ML-артефакты в один asset graph — видно, из чего получена модель, что проверено и какой версией
> рассчитаны предсказания.** Решения — [`docs/decisions.md`](docs/decisions.md); операционные детали и сбои —
> [`docs/runbook.md`](docs/runbook.md); контракты — [`docs/contracts/`](docs/contracts/).
>
> Пометка **⚠ проверить** = команда/поведение ещё не подтверждены на установленных версиях (dagster 1.13.23,
> dagster-dbt 0.29.23, mlflow 3.15); снимается по мере шагов M0–M5. Рецепты `just …` появляются по шагам TODO;
> ни одна показываемая команда не должна существовать только в слайдах.

## Тайминг

| # | Блок | Мин | Накопл. |
|---|---|---|---|
| 0 | Подготовка (до записи) | — | — |
| 1 | Старт: проект, `dg dev`, подключение dbt | 3 | 3 |
| 2 | dbt как ассеты и checks, freshness | 10 | 13 |
| 3 | Feature contract → обучение → gate → registry | 7 | 20 |
| 4 | Promotion → batch inference | 4 | 24 |
| 5 | Выборочный пересчёт | 3 | 27 |
| 6 | Dev-prod, CI/CD, observability | 3 | 30 |

Резерв — ~0.7 мин (Evidently вынесен в appendix). Если не помещается — кандидаты на срез (решает автор, не
агент): freshness-пример в блоке 2 (−1), негативный dbt-сценарий как заранее снятый скриншот (−1.5).

---

## 0. Подготовка (вне кадра)

**Цель:** чистое, детерминированное состояние «до»; реестр MLflow не пуст (baseline-версия без алиаса).

```bash
just clean all           # дропает raw/derived, MLflow-эксперимент и реестр
just demo-prepare        # raw-snapshot → DuckDB, dbt deps, baseline-версия модели БЕЗ алиаса
just mlflow              # MLflow UI на :5001 в фоне
just dev                 # Dagster UI на :3000
```

Проверить: `just check` зелёный; в UI граф без материализаций (кроме `raw/*`); MLflow → Models → одна версия,
alias пустой. Заготовить патчи `just demo-break` / `just demo-fix` (блок 2) и `just demo-sql-change` (блок 5).
Второй терминал и MLflow UI открыты заранее. **Ingestion в кадре не показываем** — только реплика ниже.

---

## 1. Старт — 3 мин

**Состояние «до».** Пустая временная папка для scaffold (рабочий репо поверх себя не пересоздаём) + уже
готовый репозиторий в соседнем окне.

**Реплики.** «Предположим, дата-инженеры уже доставили данные в хранилище либо они доступны в корпоративном
warehouse/lakehouse. Для автономности здесь локальный DuckDB со snapshot Olist. Сегодня нас интересует не
доставка CSV, а путь от raw-таблиц через dbt до модели и predictions». Оговорка: «это граница именно этого
демо, а не утверждение, что MLE не занимаются ingestion». Про dlt/Airbyte — «отдельный вебинар».

**Действия.**
1. `uvx create-dagster@1.13.23 project olist_demo && cd olist_demo && uv run dg dev` — пустой граф (⚠ проверить
   точную форму команды на 1.13.23). 30 с — и переходим в готовый репозиторий.
2. `just --list` — единый интерфейс; `just dev`.
3. Показать `src/olist_ml/defs/dbt/defs.yaml` (`DbtProjectComponent`: `project_dir`, `select`, `translation`) и
   сказать про `manifest.json`: в dev его собирает `prepare_if_dev` при загрузке definitions, в prod — при сборке
   артефакта (`packaged_project_dir` / `prepare_project_cli_args`; ⚠ проверить).

**Что видно в UI.** Граф `raw/* → staging → intermediate → mart_order_features → ML-ветка → predictions`, одним
связным компонентом. Группы `raw / staging / intermediate / marts / ml`.

**Fallback.** Scaffold не поднимается — показать заранее снятый скриншот пустого графа и идти в готовый репо.

---

## 2. dbt как ассеты и checks — 10 мин

**Состояние «до».** `raw/*` материализованы (`demo-prepare`), модели нет.

**2.1 Модели = ассеты (3 мин).** Открыть `stg_orders` → description, group, owner/tags из dbt yml; вкладка
Lineage — `source()`/`ref()` стали рёбрами; ключ `raw/orders` совпадает с ключом dbt source
(`meta.dagster.asset_key`). Реплика про Cosmos: «в Airflow 3 + Cosmos dbt-модели тоже становятся задачами и
lineage виден, но здесь граф продолжается **через границу dbt → Python** без склейки вручную».
Действие: `just feature-mart` (или Materialize selection `+mart_order_features`) → зелёные модели.

**2.2 Тесты = asset checks (4 мин).** Вкладка Checks у `mart_order_features`: dbt-тесты как checks, они
**не** запускались вместе с материализацией — это отдельная операция. `just dq` → checks зелёные.
Негативный сценарий: `just demo-break` (правит seed/SQL так, что ломается контракт витрины) → `just feature-mart`
→ модель **зелёная**; `just dq` → check **красный**; попытка `just train` → ML-ветка не выполняется (blocking;
⚠ проверить механику на M2). Реплика: «зелёная модель и красный check — разные состояния; падение проверки не
выглядит падением загрузки». `just demo-fix` → `just feature-mart` → `just dq` → зелёный (витрину надо пересобрать — в DuckDB остаются дубли).

**2.3 Freshness (2 мин).** У `mart_order_features` статус freshness (`FreshnessPolicy.time_window`, ⚠
проверить текст статуса): «это asset freshness — когда ассет последний раз материализован. Свежесть бизнес-данных
внутри — другое; `dbt source freshness` есть в коде, в кадре не показываем. Недавняя материализация ≠ свежие
данные». Реплика про Cosmos: «freshness как статус ассета, а не как ещё один DAG».

**Что видно в UI.** Lineage; Checks (зелёный/красный); Freshness-статус.

**Fallback.** `demo-break` не даёт красного check — показать скриншот; blocking не срабатывает — реплика «gate
на ML-ветке остановит её на следующем блоке».

---

## 3. Feature contract → обучение → gate → registry — 7 мин

**Состояние «до».** Витрина материализована, checks зелёные; реестр — baseline v1 без алиаса.

**3.1 Контракт (2 мин).** Открыть [`docs/contracts/features.md`](docs/contracts/features.md) или yml
витрины: «только то, что известно в момент оформления». Единственная ML-остановка: «`delivery_delay_days` даёт
почти идеальный AUC — это не хорошая модель, а утечка будущего; поля нет в момент scoring». Три объекта: витрина
(признаки) / `training_dataset` (строки с известным target) / `scoring_input` (target ещё неизвестен).

**3.2 Обучение как чёрный ящик (3 мин).** `just train` → `training_dataset` (metadata: rows train/holdout,
positive rate, fingerprint) → `model` (ссылка на MLflow run) → `model_evaluation` (ROC AUC на holdout) →
`quality_gate` зелёный → `model_registered` (metadata: `model_version`, `reused_existing`). Реплика: «внутри
обычный sklearn Pipeline — вы напишете модель лучше меня. Для нас важно, откуда пришли признаки, что прошло
проверку, что попало в реестр».

**3.3 Gate падает (2 мин).** Launchpad `train_job` → run config `quality_gate: {min_roc_auc: 0.99}` (⚠ имя
поля — после M3) → `model_evaluation` зелёный, `quality_gate` **красный**, `model_registered` **не запущен**
(blocking). В MLflow: новой версии нет, alias не тронут.

**Что видно.** Metadata ассетов; MLflow run и версия; красный gate.

**Fallback.** Повышенный порог не роняет gate (метрика выше 0.99 невозможна на честных данных, но если) —
`min_roc_auc: 1.01`.

---

## 4. Promotion → batch inference — 4 мин

**Состояние «до».** В реестре ≥2 версии (baseline + кандидат из 3.2), алиаса нет.

1. `just promote` (или `promote_job` в UI с версией) → MLflow Models: alias `champion` на версии из 3.2.
   Реплика: «регистрация и promotion — разные шаги; после failed gate предыдущий champion не меняется; «последняя
   версия = production» — нет».
2. `just score` → `scoring_input` → `predictions`; metadata: rows, mean score, predicted-positive rate,
   **`model_version`**, `batch_id`. Реплика: «training и scoring не имеют прямой runtime-зависимости —
   модель передаётся через alias `champion`; feature schema и preprocessing остаются общим контрактом. Сейчас мы
   видим, что scoring работает и какой версией получены предсказания; реальное качество станет известно позже,
   когда придут labels». Повторный `just score` — дублей нет.
3. (опц., 20 с) `just demote` → `just score` → понятная ошибка «нет опубликованной модели — выполните promote».

**Fallback.** Alias не переключился — `mlflow` UI вручную: Models → версия → alias `champion`.

---

## 5. Выборочный пересчёт — 3 мин

**Состояние «до».** Всё материализовано и зелёное.

1. `just demo-sql-change` — правка SQL `mart_order_features` (например, новый признак или изменённая формула
   `freight_share`). Reload definitions (кнопка в UI; manifest пересобирается через `prepare_if_dev`).
2. Открыть `mart_order_features` — статус «code version changed» (default `code_version = sha1(raw_sql)` в
   dagster-dbt 0.29.23; ⚠ проверить текст статуса и что downstream **не** помечен транзитивно).
3. Выбрать в UI витрину + `training_dataset … model_registered` (или `predictions`) → Materialize selection.
4. По run events: `raw/*` и неизменившийся staging **не запускались**; выполнились витрина и выбранные потребители.

**Не обещаем**: что после reload все downstream станут Unsynced; что статический job сам выберет только Unsynced;
что пересчитается ровно N узлов. Реплика про Cosmos: «в task-модели пересчитывается DAG или его сабсет по
таскам; здесь — выбранные ассеты по факту изменения кода/данных, и Python-потребители в той же выборке».

**Fallback.** Статус не отобразился — материализовать выборку вручную и показать run events (что запускалось).

---

## 6. Dev-prod, CI/CD, observability — 3 мин

**Dev vs prod (0.8 мин).** По осям overview §4.1: изоляция данных и реестра; окно обучения; проверка кандидата и
promotion; независимые расписания train и scoring; хранилище состояния Dagster; executor. Реплика: «в разработке
проверяем весь путь на небольшом snapshot и пишем в отдельный эксперимент; в проде используем утверждённые данные,
проверяем кандидата и отдельно допускаем его к применению; инференс использует опубликованную версию — обучение
при этом не запускается». Оговорка: dev-прогон отвечает «пайплайн работает», а не «модель хорошая».

**CI/CD (1 мин).** GitHub Actions → зелёный run: `just install / check / test` (без сети за данными — shipped
fixtures, без секретов); e2e — отдельный job. Показать `ci.yml`: шаги = вызовы `just`, логики в YAML нет.
Деплой и Dagster+ — в docs, не в кадре.

**Observability (1.2 мин).** Dagster UI → Runs (статусы), Asset checks (последние результаты), Freshness. Один
канал алерта: сломать check/run (`just demo-break && just dq`) → сообщение в Telegram от `run_failure_sensor`
(⚠ проверить живую доставку на M7). Реплика: «Grafana/Prometheus/Loki и Evidently-отчёты — расширенное демо и
docs; здесь — один экран и один канал».

**Fallback.** Telegram не пришёл — показать dry-run лог сенсора; CI не зелёный — заранее снятый скриншот run.

---

## Appendix (не в кадре)

- **Ingestion-варианты.** dlt из Kaggle (`.claude/drafts/ingest/`, критерии ADR-03); `raw/*` как внешние
  assets/sources, если данные приходят от другой команды; паритет контракта — [`docs/contracts/raw.md`](docs/contracts/raw.md).
- **Evidently** — один HTML-отчёт по `predictions` как расширенное демо (ADR-16).
- **Compose core + Grafana-стек** — `.claude/drafts/observability/`, `docs/deploy/` (после MVP).
- **Прод-контуры** — overview §6: партиции `predictions`, backfill, `SIM_TODAY`, мониторинг, поздние метки,
  challenger/champion, temporal split, online serving, feature store, Kafka.
- **`dbt source freshness`** — в коде, без экранного времени.
