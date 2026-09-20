# DEMO — слайды докладчика (≤30 минут)

> «Виртуальные слайды»: ⏱ тайминг, заголовок, **главный поинт**, текст докладчика буллетами, команды (bash +
> `just`-fallback), 📚 материалы для погружения. План блоков со состоянием «до», действиями, UI и fallback —
> [`DEMO-plan.md`](DEMO-plan.md). Теория по обучению — [`docs/theory/training-in-demo.md`](docs/theory/training-in-demo.md).
> Пометка **⚠ проверить** — команда/поведение ещё не подтверждены на dagster 1.13.23 / dagster-dbt 0.29.23 /
> mlflow 3.15; снимаются по шагам M0–M5. Ни одна команда не должна существовать только в слайдах.
>
> Условные обозначения: **тезис**, _термин_, <u>акцент, который надо произнести</u>.

Тайминг: старт 3 · dbt 10 · contract/train/gate/registry 7 · promote/inference 4 · выборочный пересчёт 3 ·
dev-prod/CI/observability 3 = **30**. Резерв ≈ 0.7 мин.

---

## S0. Подготовка (вне кадра) · ⏱ — (0)

**Главный поинт:** детерминированное состояние «до»; в кадре ничего не устанавливаем.

- `just clean all && just demo-prepare` — raw-snapshot в DuckDB, `dbt deps`, baseline-версия в реестре **без
  алиаса** (вариант — ADR-07a).
- Открыты: Dagster UI (`:3000`), MLflow UI (`:5001`), второй терминал, временная папка для scaffold.
- Заготовлены патчи `just demo-break` / `just demo-fix` (S7) и `just demo-sql-change` (S17).
- Проверка: `just check` зелёный; в UI граф без материализаций кроме `raw/*`; MLflow → Models → одна версия, alias пуст.

```bash
just clean all && just demo-prepare && just mlflow && just dev
```

📚 [`docs/runbook.md`](docs/runbook.md) §2–3 · [`docs/decisions.md`](docs/decisions.md) ADR-03, ADR-07a

---

## S1. Откуда данные — и почему не об этом · ⏱ 0.5 (0.5)

**Главный поинт:** ingestion — _предпосылка_, а не блок демо.

- «Предположим, дата-инженеры уже доставили данные в хранилище либо они доступны в корпоративном
  warehouse/lakehouse. Для автономности здесь локальный DuckDB со snapshot Olist».
- «Сегодня нас интересует <u>не доставка CSV</u>, а путь от raw-таблиц через dbt до модели и predictions».
- Оговорка: это граница именно этого демо, а не утверждение, что MLE не занимаются ingestion.
  - dlt / Airbyte — «будет отдельный вебинар».
- `raw/*` — восемь таблиц Olist: orders, order_items, order_payments, order_reviews, customers, sellers,
  products, geolocation; в хранилище как приехали (`VARCHAR`), касты — в staging.

📚 [`docs/contracts/raw.md`](docs/contracts/raw.md) · [Olist на Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## S2. Пустой проект за 30 секунд · ⏱ 1 (1.5)

**Главный поинт:** `create-dagster` + `dg dev` = пустой asset graph; дальше — только «подключить существующее».

- Во временной папке (рабочий репо поверх себя не пересоздаём):

```bash
cd "$(mktemp -d)" && uvx create-dagster@1.13.23 project olist_demo && cd olist_demo && uv run dg dev   # ⚠ проверить форму команды
```

- В UI — пустой граф. «Здесь ещё ничего нет — и это правильно: Dagster ничего не вычисляет сам».
- Переходим в готовый репозиторий: `just --list` — <u>один интерфейс</u> для человека, CI и Docker.

```bash
just --list && just dev
```

📚 [docs.dagster.io — Getting started / create-dagster](https://docs.dagster.io/getting-started) · ADR-15 (Justfile)

---

## S3. Подключаем dbt-проект одним компонентом · ⏱ 1.5 (3)

**Главный поинт:** _компонент_ `DbtProjectComponent` в `defs.yaml` — и весь dbt-проект становится частью графа.

- Показать `src/olist_ml/defs/dbt/defs.yaml`: `project_dir`, `select: "+mart_order_features"`, `translation`
  (группы, описания, owners).
  - «Это YAML, а не Python: конфигурация интеграции, которую можно ревьюить как данные».
- Роль **`manifest.json`**: dbt-граф → Dagster-граф. Его собирает `just dbt-parse` (входит в `install`/`check`/
  `dev`), компоненты читают готовый файл (`prepare_if_dev: false`, ADR-04b) — так же, как prod-артефакт;
  «магии при загрузке» нет, и сеть после `install` не нужна.
- Как это создавалось: `uv run dg scaffold defs dagster_dbt.DbtProjectComponent dbt` ⚠ проверить.
- Реплика Cosmos: «в Airflow 3 + Cosmos dbt-модели тоже раскладываются в задачи по manifest; разница проявится
  дальше — граф продолжится в Python без склейки».

📚 [docs.dagster.io — dbt integration](https://docs.dagster.io/integrations/libraries/dbt) · [Components](https://docs.dagster.io/guides/build/components) · ADR-02, ADR-04

---

## S4. Модель dbt = ассет; `ref()` = ребро · ⏱ 2 (5)

**Главный поинт:** dbt <u>не стал одним непрозрачным таском</u> — каждая модель отдельный ассет с lineage.

- Открыть `stg_orders`: description, group `staging`, owner/tags — всё из dbt yml, ничего не дублируется.
- Вкладка Lineage: `source()` и `ref()` превратились в рёбра; `raw/orders` — тот же ключ, что у dbt source
  (`meta.dagster.asset_key`) — <u>граф связный от raw до predictions</u>.
- Материализуем витрину: только модели, без тестов.

```bash
just feature-mart          # fallback: uv run dg launch --job feature_mart_job
```

- Группы `raw → staging → intermediate → marts` слева направо; зелёные узлы; row count в метаданных.

📚 [dbt sources/meta](https://docs.getdbt.com/reference/resource-properties/meta) · ADR-05 · [`docs/contracts/raw.md`](docs/contracts/raw.md)

---

## S5. Тесты dbt = asset checks · ⏱ 2 (7)

**Главный поинт:** _материализация_ и _проверка_ — <u>две разные операции</u> с разными статусами.

- Вкладка Checks у `mart_order_features`: `unique(order_id)`, `not_null`, `accepted_values(is_late_delivery)` —
  это dbt-тесты, показанные как asset checks.
- Они **не запускались** вместе с `feature-mart` — запускаем отдельно:

```bash
just dq                    # fallback: uv run dg launch --job dq_job   (только checks; DBT_INDIRECT_SELECTION=empty выставляет dagster-dbt)
```

- Зелёные checks. «В `dbt build` тест и модель живут в одном прогоне; здесь я могу проверить данные, не трогая
  модель, и наоборот».
- Факт для вопросов: dbt-тест с `severity: error` становится **blocking**-check'ом (dagster-dbt 0.29.23),
  `warn` — non-blocking; предупреждение никогда не изображается блокирующим.

📚 [docs.dagster.io — Asset checks](https://docs.dagster.io/guides/test/asset-checks) · [dbt tests](https://docs.getdbt.com/docs/build/data-tests) · ADR-04

---

## S6. Зелёная модель, красный check · ⏱ 3 (10)

**Главный поинт:** по UI видно, <u>где сломалось — в загрузке или в качестве данных</u>.

- Ломаем контракт витрины заготовленным патчем (дубль/NULL в ключевом поле):

```bash
just demo-break && just feature-mart     # модель зелёная
just dq                                  # check красный
```

- Открыть витрину: материализация зелёная, check красный — <u>два разных состояния одного ассета</u>.
- Пробуем обучение — ML-ветка не выполняется (blocking; механика — ADR-04a, ⚠ проверить на M2 текст статуса
  «skipped/blocked»):

```bash
just train                               # ожидаем: ML-ассеты не материализованы
```

- «Падение проверки не выглядит падением загрузки — и не даёт обучиться на сломанных данных».
- Чиним, **пересобираем витрину** (в DuckDB всё ещё дубли) и подтверждаем:

```bash
just demo-fix && just feature-mart && just dq
```

- Факт (M2): run `dq_job` при провале dbt-теста завершается **FAILURE** (dbt build возвращает ненулевой код),
  а модель и её материализация остаются зелёными — это и есть «две разные операции» в Runs.

- Реплика Cosmos: «там dbt-тест — задача в DAG; здесь — статус ассета, который можно спросить из любого места графа».

📚 ADR-04, ADR-04a (варианты гейта) · [`docs/contracts/features.md`](docs/contracts/features.md)

---

## S7. Свежесть: ассета ≠ данных · ⏱ 2 (12)

**Главный поинт:** _asset freshness_ — когда ассет материализован; _data freshness_ — насколько свежи данные внутри. <u>Недавняя материализация ≠ свежие данные.</u>

- У `mart_order_features` — статус freshness `HEALTHY` (`FreshnessPolicy.time_window`, окна 10/30 мин из `.env`,
  демон `freshness.enabled: true`; проверено на M5). Витрина собрана в S4 (~5-я минута) → к этому слайду
  (~12-я) статус может сам стать `WARNING` — это и есть демонстрация.
- «Если витрину не пересчитывали дольше окна — статус деградирует, даже если данные внутри нормальные. Это
  вопрос оркестрации».
- «Свежесть бизнес-данных — `dbt source freshness` по `order_purchase_timestamp`; она в коде, но экранного
  времени не получает: Olist заканчивается в 2018-м».
- Реплика Cosmos: «freshness здесь — статус ассета, а не ещё один DAG».

📚 [docs.dagster.io — Freshness](https://docs.dagster.io/guides/labs/observe/freshness) · [dbt source freshness](https://docs.getdbt.com/docs/build/sources#source-data-freshness) · ADR-17

---

## S8. Резерв dbt-блока · ⏱ 1 (13)

**Главный поинт:** буфер на вопросы из зала по dbt-блоку; если вопросов нет — переходим дальше.

- Кандидаты на короткий ответ: «почему копия dbt-проекта, а не submodule» (ADR-04: цикл сабмодулей, `dg dev`
  и Docker), «сколько моделей в графе» (8 stg + 3 int + 1 mart через `select +mart_order_features`).

📚 ADR-04, ADR-09

---

## S9. Три набора строк одной витрины · ⏱ 1.5 (14.5)

**Главный поинт:** _feature mart_, _training dataset_, _scoring input_ — <u>разные объекты</u>, а не одна таблица.

- Задача: «на основании информации, доступной при оформлении заказа, оценить риск, что заказ будет доставлен
  позднее обещанного срока».
- Витрина — признаки; `training_dataset` — исторические строки с известным target; `scoring_input` — строки, у
  которых target ещё неизвестен (заказ в пути).
- Target `is_late_delivery` у недоставленного — `NULL` («ещё не знаем»), не 0.
- Открыть [`docs/contracts/features.md`](docs/contracts/features.md) или yml витрины: 8–10 признаков и правило
  «известно в момент оформления».

📚 [`docs/theory/training-in-demo.md`](docs/theory/training-in-demo.md) §1–2 · ADR-14

---

## S10. Единственная ML-остановка: утечка · ⏱ 1 (15.5)

**Главный поинт:** `delivery_delay_days` даёт почти идеальную метрику — <u>это не хорошая модель, а утечка будущего</u>.

- Поля нет в момент реального scoring → оно не входит в feature contract. То же — отзывы, финальный статус,
  агрегаты «за весь период».
- «Утечка исключается на уровне витрины и контракта, а не в коде модели. Это работа DE».
- Конкретный AUC не обещаем до воспроизведения на текущем коде.

📚 теория §2 · ADR-06

---

## S11. Обучение как чёрный ящик · ⏱ 2.5 (18)

**Главный поинт:** нас интересует <u>откуда пришли признаки, что прошло проверку, что попало в реестр</u> — не устройство алгоритма.

```bash
just train                 # fallback: uv run dg launch --job train_job
```

- По ассетам: `training_dataset` (rows train/holdout, positive rate, `dataset_fingerprint`) → `model` (ссылка на
  MLflow run) → `model_evaluation` (ROC AUC на holdout) → `quality_gate` зелёный → `model_registered`
  (`model_version`, `reused_existing`).
- «Внутри обычный sklearn Pipeline — вы напишете модель лучше меня».
- Технические факты, если спросят: детерминированный сплит по `md5(order_id)`; snapshot train/holdout —
  evaluation не перечитывает витрину; preprocessing внутри Pipeline, `fit` только на train.
- Кликнуть `mlflow_url` → run в MLflow: параметры, метрики, артефакт.

📚 теория §3–5 · [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html) · ADR-06, ADR-07

---

## S12. Gate падает — регистрация не происходит · ⏱ 2 (20)

**Главный поинт:** _blocking check_ по ROC AUC — <u>одна метрика, одна причина остановки</u>.

- Launchpad `train_job` → run config (op asset-check'а называется `<asset>_<check>`):

```yaml
ops:
  model_evaluation_quality_gate:
    config: {min_roc_auc: 0.99}
```

  → Launch. Порог по умолчанию — `ML_MIN_ROC_AUC=0.61` (измерен на snapshot: LogReg по 5 сидам 0.63–0.69).
- `model_evaluation` зелёный, `quality_gate` **красный**, `model_registered` не запущен.
- MLflow: новой версии нет, alias не тронут.
- «Порог мы подняли честно — через конфигурацию, а не подсунули модель похуже. Порог по умолчанию измерен на
  этом snapshot по пяти сидам».
- Fallback: `min_roc_auc: 1.01`.

📚 теория §5 · [Asset checks — blocking](https://docs.dagster.io/guides/test/asset-checks) · ADR-06

---

## S13. Регистрация ≠ promotion · ⏱ 1.5 (21.5)

**Главный поинт:** версия в реестре — ещё не production; <u>`champion` переводится явным действием</u>.

- MLflow → Models: baseline v1 (из подготовки) и кандидат v2 (из S11), alias пустой.

```bash
just promote               # последняя версия; `just promote 1` — явная; fallback: alias в MLflow UI
```

- Alias `champion` → v2 (metadata op: `previous_version`, `version`). «Откат = `just promote 1`; переобучать
  ничего не нужно».
- «После failed gate (S12) версии не появилось, champion не изменился — <u>"последняя версия = production"</u> здесь
  невозможно».

📚 [MLflow Model Registry — aliases](https://mlflow.org/docs/latest/model-registry.html) · теория §6 · ADR-07

---

## S14. Batch inference опубликованной версией · ⏱ 2 (23.5)

**Главный поинт:** scoring <u>не зависит от run обучения</u>; модель приходит через alias, версия записана в каждую строку.

```bash
just score                 # fallback: uv run dg launch --job score_job
```

- `scoring_input` (таблица `ml.scoring_input`: заказы с ещё неизвестным target — на snapshot 138) →
  `predictions` (`ml.predictions`); метаданные: rows, mean score, predicted-positive rate, **`model_version`**,
  `batch_id`, `rows_total_in_table`.
- «Training и scoring не имеют прямой runtime-зависимости: модель передаётся через alias `champion`; feature
  schema и preprocessing остаются общим контрактом».
- «Сейчас мы видим, что scoring работает и какой версией получены предсказания. Реальное качество станет
  известно позже, когда придут labels» — mean score ≠ drift, ≠ качество.
- Повторный `just score` — дублей нет (идемпотентность по `batch_id`).

📚 теория §7 · [`docs/contracts/features.md`](docs/contracts/features.md) §4 · ADR-13

---

## S15. Без champion — понятная остановка · ⏱ 0.5 (24)

**Главный поинт:** отсутствие опубликованной модели — <u>ожидаемое состояние</u>, а не traceback.

```bash
just demote && just score   # ожидаем: «нет опубликованной модели — выполните promote»
just promote
```

- Опционально, если есть 30 секунд; иначе — одной репликой.

📚 ADR-13

---

## S16. Меняем SQL витрины · ⏱ 1 (25)

**Главный поинт:** Dagster знает, что <u>код ассета изменился</u>, ещё до пересчёта.

```bash
just demo-sql-change       # правка mart_order_features.sql (новый признак / формула freight_share)
```

- `just dbt-parse` (manifest) → Reload definitions в UI (ADR-04b).
- Открыть `mart_order_features` — статус **Unsynced / STALE**, причина «has a new code version» (в dagster-dbt
  0.29.23 `code_version` по умолчанию = `sha1(raw_sql)`; своей реализации не нужно — проверено на M5).
- «Downstream ML-ассеты <u>не помечены транзитивно</u> (они `FRESH`) — и это правильно: их код не менялся,
  изменились бы данные после пересчёта витрины».

📚 [docs.dagster.io — code versions / stale assets](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching) · ADR-17

---

## S17. Пересчитываем только нужное · ⏱ 2 (27)

**Главный поинт:** <u>выбранная витрина + её ML-потребители</u> — без raw и без неизменившегося staging.

- В UI выбрать `mart_order_features` + `training_dataset … model_registered` → Materialize selection; из
  терминала то же самое:

```bash
just demo-recompute        # dg launch --assets "mart_order_features,training_dataset,model,model_evaluation,model_registered"
```

- По run events (проверено на M5): степы `dbt_feature_branch → training_dataset → model → model_evaluation →
  quality_gate → model_registered`; `raw/*` и staging не запускались; после run все ассеты `FRESH`.
- Не обещаем: что после reload все downstream станут Unsynced; что статический job сам выберет только Unsynced;
  что пересчитается ровно N узлов.
- Реплика Cosmos: «в task-модели пересчитывается DAG или сабсет по таскам; здесь — выбранные ассеты по факту
  изменения, и Python-потребители в той же выборке, через границу dbt → Python».

📚 ADR-17 · [Asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax)

---

## S18. Dev vs prod — через ML-процесс · ⏱ 0.8 (27.8)

**Главный поинт:** оси отличий — <u>данные, окно, promotion, расписания, состояние, executor</u>, а не загрузчики.

- «В разработке проверяем весь путь на небольшом snapshot и пишем в отдельный эксперимент. В проде используем
  утверждённые данные и ресурсы, проверяем кандидата и отдельно допускаем его к применению. Инференс использует
  опубликованную версию — обучение при этом не запускается».
- Оговорка: dev-прогон отвечает «пайплайн работает», а не «модель хорошая».

📚 [`docs/overview.md`](docs/overview.md) §4.1

---

## S19. CI: что именно зелёное · ⏱ 1 (28.8)

**Главный поинт:** CI = <u>те же `just`-рецепты</u>, без сети за данными и без секретов.

- GitHub Actions → зелёный run: `just install / check / test`; e2e — отдельный job.
- `ci.yml`: шаги = вызовы раннера, логики в YAML нет; shipped fixtures вместо сети.
- Деплой и Dagster+ — в docs, не в кадре.

📚 ADR-11 · `.github/workflows/ci.yml` (после M6)

---

## S20. Observability: один экран, один канал · ⏱ 1.2 (30)

**Главный поинт:** статус run'ов и checks — в UI; <u>один канал алерта</u> без дополнительной инфраструктуры.

- Dagster UI → Runs (статусы), Asset checks (последние результаты), Freshness.
- Сломать check и получить сообщение в Telegram от `run_failure_sensor` (⚠ проверить живую доставку на M7):

```bash
just demo-break && just feature-mart && just dq   # → Telegram
just demo-fix && just feature-mart && just dq
```

- «Grafana/Prometheus/Loki и drift-отчёты (Evidently) — расширенное демо и docs; здесь — один экран и один канал».

📚 ADR-18, ADR-12, ADR-16 · [Sensors — run failure sensor](https://docs.dagster.io/guides/automate/sensors/run-failure-sensors)

---

## S21. Итог · ⏱ — (30)

**Главный поинт:** **Dagster соединяет dbt-модели и ML-артефакты в один asset graph: видно, из чего получена модель, что проверено и какой версией рассчитаны предсказания.**

- Что показали: модели = ассеты; тесты = checks; blocking gate; register ≠ promote; inference по alias с
  `model_version`; выборочный пересчёт; freshness как статус.
- Чего намеренно не показали: ingestion, внутренности ML, Docker, полный observability-стек — appendix.

📚 [`docs/overview.md`](docs/overview.md) §6 (как в проде) · [`DEMO-plan.md`](DEMO-plan.md) appendix

---

## S22. Appendix (не в кадре)

- **Ingestion-варианты:** dlt из Kaggle (`.claude/drafts/ingest/`, критерии ADR-03); `raw/*` как внешние
  assets/sources от другой команды; паритет контракта — [`docs/contracts/raw.md`](docs/contracts/raw.md).
- **Evidently** — один HTML-отчёт по `predictions` как расширенное демо (ADR-16).
- **Compose core + Grafana-стек** — `.claude/drafts/observability/`, `docs/deploy/` (после MVP).
- **Прод-контуры** — overview §6: партиции `predictions`, backfill, `SIM_TODAY`, мониторинг, поздние метки,
  challenger/champion, temporal split, online serving, feature store, Kafka.
- **Открытые решения** — ADR-04a (как провал dbt-теста гейтит ML), ADR-07a (baseline в `demo-prepare`).
