# Демо 2 — показ: dbt в asset graph (≈ 12.5 мин)

[README](README.md) · [2-1-prepare](2-1-prepare.md) · **2-2-run** · [2-3-results](2-3-results.md) · следующее → [3-2-run](3-2-run.md)

**Тезис демо:**

- dbt-проект подключается к графу <u>одним компонентом</u>, без ручной склейки;
- каждая модель dbt — отдельный ассет с lineage, `ref()` / `source()` — рёбра;
- тест dbt — asset check: материализация и проверка — <u>две разные операции</u> с разными статусами;
- провал blocking-check'а витрины не даёт ML обучиться на сломанных данных;
- свежесть ассета ≠ свежесть данных.

## Навигация

| # | Слайд | Краткое описание слайда | ⏱ | Накопл. |
|---|---|---|---|---|
| [D2-1](#d2-1) | Откуда данные | ingestion — предпосылка; raw-snapshot Olist в DuckDB | 0.5 | 0.5 |
| [D2-2](#d2-2) | Из пустого проекта в готовый | тот же `dg`-проект, что в демо 1, но с dbt, MLflow и джобами | 0.5 | 1 |
| [D2-3](#d2-3) | dbt одним компонентом | [`DbtProjectComponent`](../src/olist_ml/defs/dbt/defs.yaml) + [`manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json) → dbt-граф в Dagster | 1.5 | 2.5 |
| [D2-4](#d2-4) | Модель dbt = ассет | lineage от raw до витрины; материализуем витрину | 2 | 4.5 |
| [D2-5](#d2-5) | Тесты dbt = asset checks | checks запускаются отдельно от моделей | 2 | 6.5 |
| [D2-6](#d2-6) | Зелёная модель, красный check | ломаем контракт: модель зелёная, check красный, ML не стартует | 3 | 9.5 |
| [D2-7](#d2-7) | Свежесть ассета ≠ данных | freshness-статус витрины по окну | 2 | 11.5 |
| [D2-8](#d2-8) | Резерв dbt-блока | буфер на вопросы | 1 | 12.5 |

- Команды — из корня репозитория, без `uv run` (direnv). UI: [http://localhost:3000](http://localhost:3000),
  location `olist_ml`.
- Повторяющийся `dbt parse` записан функцией (её же подставляет прогон [`tests/steps.py`](tests/steps.py)):

```bash
dbt_parse() { dbt parse --quiet --project-dir "$PWD/dbt" --profiles-dir "$PWD/dbt" --target-path "$PWD/dbt/target"; }
```

- Проверки шагов: [`just check-demo-2-step-N-input`](steps.just) / [`-result`](steps.just). Прогон всех шагов:
  [`just demo-2-run`](Justfile) / [`just demo-2-run just`](Justfile).

---

<a id="d2-1"></a>
## D2-1. Откуда данные и почему не об этом · ⏱ 0.5 (0.5)

- **Главный поинт:** ingestion здесь _предпосылка_, а не блок демо
  ([ADR-03](../docs/decisions.md#adr-03-ingestion--предпосылка-не-демо-dbt-seed-shipped-snapshot--demo-prepare-dlt--stretch)).
- **Дано / на входе:**
  - состояние «до» ([P2-1](2-1-prepare.md#p2-1)): `raw/*` в DuckDB;
  - проверка: [`just check-demo-2-step-1-input`](steps.just).
- **Шаги:**
  1. «Предположим, дата-инженеры уже доставили данные в хранилище либо они доступны в корпоративном
     warehouse/lakehouse»;
     - для автономности используется локальный DuckDB со snapshot Olist;
  2. «Сегодня нас интересует <u>не доставка CSV</u>, а путь от raw-таблиц через dbt до модели и predictions»;
  3. оговорка: это граница именно этого демо, а не утверждение, что MLE не занимаются ingestion;
     - dlt / Airbyte — «будет отдельный вебинар» ([черновики ingest](../.claude/drafts/ingest));
  4. `raw/*` — восемь таблиц Olist ([контракт raw](../docs/contracts/raw.md)):
     - orders, order_items, order_payments, order_reviews, customers, sellers, products, geolocation;
     - в хранилище как приехали (`VARCHAR`), касты делаются в staging.
- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/raw](http://localhost:3000/locations/olist_ml/asset-groups/raw) —
    8 raw-ассетов, материализованы в подготовке.
- **Результат / на выходе:**
  - граница демо обозначена;
  - проверка: [`just check-demo-2-step-1-result`](steps.just).

📚 [`docs/contracts/raw.md`](../docs/contracts/raw.md) · [Olist на Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

<a id="d2-2"></a>
## D2-2. Из пустого проекта в готовый · ⏱ 0.5 (1)

- **Главный поинт:** это тот же `dg`-проект, что в [демо 1](1-2-run.md), только в нём уже есть dbt, MLflow и джобы.
- **Дано / на входе:**
  - dbt manifest собран ([P2-2](2-1-prepare.md#p2-2));
  - проверка: [`just check-demo-2-step-2-input`](steps.just).
- **Шаги:**
  1. «В демо 1 мы начали с пустого `create-dagster`. Здесь такой же проект, но наполненный»;
  2. показать определения и рецепты: [`Justfile`](../Justfile) — <u>один интерфейс</u> для человека, CI и Docker
     ([ADR-15](../docs/decisions.md#adr-15-task-runner--justfile-единственный-интерфейс)).

```bash
dg list defs                          # ассеты, checks, джобы, сенсоры проекта
```

```bash
just --list                           # все рецепты проекта: install, dev, train, score, demo-*…
```

- **UI:**
  - [http://localhost:3000/assets](http://localhost:3000/assets) — каталог: raw, staging, intermediate, marts, ml, inference.
- **Результат / на выходе:**
  - зритель видит масштаб: десятки ассетов вместо четырёх;
  - проверка: [`just check-demo-2-step-2-result`](steps.just).

---

<a id="d2-3"></a>
## D2-3. dbt-проект одним компонентом · ⏱ 1.5 (2.5)

- **Главный поинт:** _компонент_ `DbtProjectComponent` в [`defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml)
  делает весь dbt-проект частью графа.
- **Дано / на входе:**
  - [`src/olist_ml/defs/dbt/defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml), [`dbt/target/manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json);
  - проверка: [`just check-demo-2-step-3-input`](steps.just).
- **Шаги:**
  1. показать [`defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml):
     - `project_dir`, `select: "+mart_order_features"`, `translation` (группы, описания, owners);
     - «это YAML, а не Python: конфигурацию интеграции можно ревьюить как данные»;
  2. роль **[`manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json)** ([ADR-04b](../docs/decisions.md#adr-04b-prepare_if_dev-false--manifest-собирает-just-dbt-parse-devcheckci-офлайн)):
     - dbt-граф превращается в Dagster-граф;
     - manifest собирает `dbt parse` (входит в `install` / `check` / `dev`);
     - компоненты читают готовый файл (`prepare_if_dev: false`), как prod-артефакт; сеть после `install` не нужна;
  3. как это создавалось: `dg scaffold defs …` → [`defs/dbt/defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml), дальше вручную `select`, `exclude`,
     `translation`, `op.name`, `prepare_if_dev: false`;
  4. реплика про Cosmos: «в Airflow 3 + Cosmos dbt-модели тоже раскладываются в задачи по manifest. Разница
     проявится дальше: граф продолжится в Python без склейки».

```bash
dg scaffold defs dagster_dbt.DbtProjectComponent dbt --project-path dbt   # не выполнять: только показать
```

- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/staging](http://localhost:3000/locations/olist_ml/asset-groups/staging) —
    staging-модели как ассеты.
- **Результат / на выходе:**
  - понятно, откуда dbt-ассеты: один компонент + manifest;
  - проверка: [`just check-demo-2-step-3-result`](steps.just).

📚 [dbt integration](https://docs.dagster.io/integrations/libraries/dbt) · [Components](https://docs.dagster.io/guides/build/components) · [ADR-02](../docs/decisions.md#adr-02-раскладка-create-dagster--yaml-компоненты-python-fallback-отложен) · [ADR-04](../docs/decisions.md#adr-04-dbt--копия-канона-в-графе-только-mart_order_features)

---

<a id="d2-4"></a>
## D2-4. Модель dbt = ассет; `ref()` = ребро · ⏱ 2 (4.5)

- **Главный поинт:** dbt <u>не стал одним непрозрачным таском</u>. Каждая модель — отдельный ассет с lineage.
- **Дано / на входе:**
  - raw-snapshot загружен;
  - проверка: [`just check-demo-2-step-4-input`](steps.just) (снимок счётчика материализаций витрины).
- **Шаги:**
  1. открыть [`stg_orders`](http://localhost:3000/assets/stg_orders): description, group `staging`, owner/tags —
     всё из dbt yml, ничего не дублируется;
  2. Lineage: `source()` и `ref()` стали рёбрами; `raw/orders` — тот же ключ, что у dbt source
     (`meta.dagster.asset_key`, [ADR-05](../docs/decisions.md#adr-05-сшивка-графа-ключ-dbt-source--ключ-ingest-ассета)); <u>граф связный от raw до predictions</u>;
  3. материализовать витрину (только модели, без тестов) — **Materialize** `+mart_order_features`;
     - то же командой: `dg launch --job feature_mart_job` ([`jobs.py`](../src/olist_ml/defs/jobs.py));
     - результат: [Runs](http://localhost:3000/runs), [витрина](http://localhost:3000/assets/mart_order_features);
  4. `mart_order_features` — <u>тот же ключ, что был parquet-ассетом в [демо 1](1-2-run.md#d1-2)</u>.

```bash
dg launch --job feature_mart_job
```

```bash
just feature-mart                     # dbt-модели до mart_order_features, без тестов
```

- **UI:**
  - [http://localhost:3000/assets/stg_orders](http://localhost:3000/assets/stg_orders) — описание, группа, owner из dbt yml;
  - [http://localhost:3000/assets/mart_order_features?view=lineage](http://localhost:3000/assets/mart_order_features?view=lineage) —
    upstream до `raw/*`, downstream в `ml`;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — run `feature_mart_job` **Success**;
  - [http://localhost:3000/locations/olist_ml/asset-groups/marts](http://localhost:3000/locations/olist_ml/asset-groups/marts) —
    зелёная витрина, row count в metadata.
- **Результат / на выходе:**
  - `marts.mart_order_features` пересобрана, группы `raw → staging → intermediate → marts` зелёные;
  - проверка: [`just check-demo-2-step-4-result`](steps.just).

📚 [dbt sources/meta](https://docs.getdbt.com/reference/resource-properties/meta) · [ADR-05](../docs/decisions.md#adr-05-сшивка-графа-ключ-dbt-source--ключ-ingest-ассета)

---

<a id="d2-5"></a>
## D2-5. Тесты dbt = asset checks · ⏱ 2 (6.5)

- **Главный поинт:** _материализация_ и _проверка_ — <u>две разные операции</u> с разными статусами.
- **Дано / на входе:**
  - витрина материализована (D2-4), checks не запускались;
  - проверка: [`just check-demo-2-step-5-input`](steps.just).
- **Шаги:**
  1. вкладка [Checks у `mart_order_features`](http://localhost:3000/assets/mart_order_features?view=checks):
     `unique(order_id)`, `not_null`, `accepted_values(is_late_delivery)` — dbt-тесты
     ([yml витрины](../dbt/models/marts/_mart_order_features.yml)) как asset checks;
  2. они **не запускались** вместе с `feature-mart` — запускаем отдельно (`dq_job`);
     - результат: [checks витрины](http://localhost:3000/assets/mart_order_features?view=checks) — **Passed**;
  3. «В `dbt build` тест и модель живут в одном прогоне. Здесь я могу проверить данные, не трогая модель, и
     наоборот»;
  4. факт для вопросов: `severity: error` → **blocking**-check; `warn` → non-blocking
     ([ADR-04](../docs/decisions.md#adr-04-dbt--копия-канона-в-графе-только-mart_order_features)).

```bash
dg launch --job dq_job
```

```bash
just dq                               # только dbt-checks витрины (без пересборки моделей)
```

- **UI:**
  - [http://localhost:3000/assets/mart_order_features?view=checks](http://localhost:3000/assets/mart_order_features?view=checks) —
    до запуска «not evaluated», после — **Passed**.
- **Результат / на выходе:**
  - checks витрины зелёные, материализация не менялась;
  - проверка: [`just check-demo-2-step-5-result`](steps.just).

📚 [Asset checks](https://docs.dagster.io/guides/test/asset-checks) · [dbt tests](https://docs.getdbt.com/docs/build/data-tests)

---

<a id="d2-6"></a>
## D2-6. Зелёная модель, красный check · ⏱ 3 (9.5)

- **Главный поинт:** по UI видно, <u>где сломалось: в загрузке или в качестве данных</u>.
- **Дано / на входе:**
  - витрина и checks зелёные (D2-5); заготовка [`demo_patch.py break`](../scripts/demo_patch.py) — дубли `order_id`;
  - проверка: [`just check-demo-2-step-6-input`](steps.just) (снимок числа красных run'ов).
- **Шаги:**
  1. сломать контракт витрины → пересобрать витрину → запустить checks:
     - модель **зелёная**, check `unique(order_id)` **красный** — <u>два состояния одного ассета</u>;
     - результат: [checks витрины](http://localhost:3000/assets/mart_order_features?view=checks), [Runs](http://localhost:3000/runs);
  2. попробовать обучение — ML-ветка не выполняется:
     - `train_job` включает blocking dbt-checks витрины в тот же run
       ([ADR-04a](../docs/decisions.md#adr-04a-как-провал-dbt-теста-останавливает-ml-ветку--варианты)); провал `unique(order_id)` пропускает ML-ассеты;
     - «падение проверки не выглядит падением загрузки и не даёт обучиться на сломанных данных»;
  3. починить и **пересобрать витрину**: в DuckDB всё ещё лежат дубли;
  4. реплика про Cosmos: «там dbt-тест — задача в DAG; здесь статус ассета, который можно спросить из любого
     места графа».

```bash
python scripts/demo_patch.py break && dbt_parse
dg launch --job feature_mart_job                  # модель зелёная
dg launch --job dq_job || true                    # check красный — run падает ожидаемо
dg launch --job train_job || true                 # ML не стартует — run падает ожидаемо
python scripts/demo_patch.py fix && dbt_parse
dg launch --job feature_mart_job && dg launch --job dq_job
```

```bash
just demo-break                       # патч: дубли order_id в витрине + dbt-parse
just feature-mart                     # пересобрать витрину — зелёная
just dq || true                       # checks витрины — unique(order_id) красный
just train || true                    # обучение не стартует: blocking-check витрины провален
just demo-fix                         # вернуть SQL витрины к эталону + dbt-parse
just feature-mart && just dq          # пересобрать витрину (убрать дубли) и перепроверить
```

- **UI:**
  - [http://localhost:3000/assets/mart_order_features?view=checks](http://localhost:3000/assets/mart_order_features?view=checks) —
    `unique…order_id` **Failed** при зелёной материализации; после fix — **Passed**;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — `dq_job` **Failure**, `train_job`: ML-шаги не выполнены.
- **Fallback:**
  - `demo-break` не даёт красного check — заранее снятый скриншот красного `unique(order_id)`;
  - blocking не остановил `train_job` — реплика «gate на ML-ветке остановит её в [демо 4](4-2-run.md#d4-4)».
- **Результат / на выходе:**
  - был красный `dq_job` и остановленный `train_job`; витрина снова эталонная, checks зелёные;
  - проверка: [`just check-demo-2-step-6-result`](steps.just).

📚 [ADR-04a](../docs/decisions.md#adr-04a-как-провал-dbt-теста-останавливает-ml-ветку--варианты) · [`docs/contracts/features.md`](../docs/contracts/features.md)

---

<a id="d2-7"></a>
## D2-7. Свежесть: ассета ≠ данных · ⏱ 2 (11.5)

- **Главный поинт:** _asset freshness_ — когда ассет материализован; _data freshness_ — насколько свежи данные
  внутри. <u>Недавняя материализация ≠ свежие данные.</u>
- **Дано / на входе:**
  - витрина материализована; `FreshnessPolicy.time_window`, окна `FRESHNESS_WARN_MIN` / `FRESHNESS_FAIL_MIN`
    ([`.env.example`](../.env.example)), `freshness.enabled: true` в [`dagster.yaml`](../dagster.yaml);
  - проверка: [`just check-demo-2-step-7-input`](steps.just).
- **Шаги:**
  1. статус freshness у [`mart_order_features`](http://localhost:3000/assets/mart_order_features) — `HEALTHY`;
     к этому слайду может сам стать `WARNING` — это и есть демонстрация;
  2. «Если витрину не пересчитывали дольше окна, статус деградирует, даже если данные нормальные. Это вопрос
     оркестрации»;
  3. «Свежесть бизнес-данных — `dbt source freshness` по `order_purchase_timestamp`; в кадре не показываем: Olist
     заканчивается в 2018-м»;
  4. реплика про Cosmos: «freshness — статус ассета, а не ещё один DAG».
- **UI:**
  - [http://localhost:3000/assets/mart_order_features](http://localhost:3000/assets/mart_order_features) — блок
    Freshness: статус и окно.
- **Результат / на выходе:**
  - разница asset vs data freshness проговорена;
  - проверка: [`just check-demo-2-step-7-result`](steps.just) (покажет возраст витрины в минутах).

📚 [Freshness](https://docs.dagster.io/guides/labs/observe/freshness) · [dbt source freshness](https://docs.getdbt.com/docs/build/sources#source-data-freshness) · [ADR-17](../docs/decisions.md#adr-17-выборочный-пересчёт-и-freshness--центральный-dagster-сценарий)

---

<a id="d2-8"></a>
## D2-8. Резерв dbt-блока · ⏱ 1 (12.5)

- **Главный поинт:** буфер на вопросы из зала по dbt.
- **Дано / на входе:**
  - dbt-блок пройден;
  - проверка: [`just check-demo-2-step-8-input`](steps.just).
- **Шаги:**
  1. «почему копия dbt-проекта, а не submodule» — [ADR-04](../docs/decisions.md#adr-04-dbt--копия-канона-в-графе-только-mart_order_features): цикл сабмодулей,
     `dg dev` и Docker;
  2. «сколько моделей в графе» — 8 stg + 3 int + 1 mart через `select +mart_order_features`;
  3. «куда делись вопросы про CI и алерты» → [демо 3](3-2-run.md).
- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/marts](http://localhost:3000/locations/olist_ml/asset-groups/marts) —
    вернуть кадр на витрину.
- **Результат / на выходе:**
  - вопросы закрыты или время сэкономлено → [2-3-results.md](2-3-results.md);
  - проверка: [`just check-demo-2-step-8-result`](steps.just).

📚 [ADR-04](../docs/decisions.md#adr-04-dbt--копия-канона-в-графе-только-mart_order_features) · [ADR-09](../docs/decisions.md#adr-09-никаких-скаляров-в-коде-плоский-envexample)
