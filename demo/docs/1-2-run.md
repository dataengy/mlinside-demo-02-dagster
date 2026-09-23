# Демо 1 — показ: Dagster с нуля (≈ 13 мин)

[README](README.md) · [1-1-prepare](1-1-prepare.md) · **1-2-run** · [1-3-results](1-3-results.md)

**Тезис демо:**

- в Dagster пайплайн описывается не задачами, а <u>данными, которые он производит</u> (ассетами);
- граф строится из аргументов функций, DAG вручную не рисуется;
- метаданные, проверки и пересчёт привязаны к ассету.

## Навигация

| # | Слайд | Краткое описание слайда | ⏱ | Накопл. |
|---|---|---|---|---|
| [D1-1](#d1-1) | Каркас и интерфейс | `create-dagster` + `dg dev`: пустой проект и пустой граф за минуту | 2 | 2 |
| [D1-2](#d1-2) | Четыре ассета → граф | pandas/sklearn-функции; рёбра графа — аргументы функций | 3 | 5 |
| [D1-3](#d1-3) | Materialize all | материализация = сохранённое значение ассета; run из 4 шагов | 2 | 7 |
| [D1-4](#d1-4) | Metadata | метрики в metadata ассета, график `roc_auc` по запускам | 2 | 9 |
| [D1-5](#d1-5) | Asset checks | `no_leakage` и blocking `quality_gate`: зелёный и красный через config | 2 | 11 |
| [D1-6](#d1-6) | Выборочный пересчёт | изменили код модели → Unsynced → пересчёт только `model+` | 1.5 | 12.5 |
| [D1-7](#d1-7) | Мостик ко второму демо | витрина-файл станет dbt-проектом | 0.5 | 13 |

- Сырые команды пишутся без `uv run`, окружение активирует direnv. D1-1 выполняется из корня репозитория,
  остальные шаги — в `demo/olist_ml/`.
- `just`-рецепты ([`Justfile`](Justfile)) работают из любого каталога репозитория.
- UI: [http://localhost:3000](http://localhost:3000), code location `olist_ml`.
- Проверки шагов: [`just check-demo-1-step-N-input`](steps.just) / [`-result`](steps.just). Прогон всех
  шагов: [`just demo-1-run`](Justfile) (сырые команды) и [`just demo-1-run just`](Justfile) (just-блоки).

---

<a id="d1-1"></a>
## D1-1. Каркас и интерфейс · ⏱ 2 (2)

- **Главный поинт:** `create-dagster` + `dg dev` дают пустой asset graph за минуту. Dagster <u>сам ничего не
  вычисляет</u>, пока ассеты не описаны.
- **Дано / на входе:**
  - `demo/olist_ml/` отсутствует, витрина выгружена ([P1-2](1-1-prepare.md#p1-2));
  - кеш uv прогрет, порт 3000 свободен;
  - проверка: [`just check-demo-1-step-1-input`](steps.just).
- **Шаги:**
  1. создать проект; `--uv-sync` сразу делает `uv sync` без интерактивного вопроса;
  2. подключить `.envrc` из [`materials/1/envrc`](materials/1/envrc): `DAGSTER_HOME` проекта + `.venv`;
     - без `DAGSTER_HOME` история материализаций пропадёт после перезапуска `dg dev`;
  3. запустить UI (`dg dev`, терминал занят);
  4. показать структуру проекта ([документация](https://docs.dagster.io/getting-started)):
     - [`src/olist_ml/definitions.py`](../src/olist_ml/definitions.py) — точка входа (здесь и ниже — аналог в основном проекте);
     - [`src/olist_ml/defs/`](../src/olist_ml/defs/jobs.py) — всё, что лежит здесь, <u>загружается автоматически</u>;
     - [`pyproject.toml`](../pyproject.toml) → `[tool.dg]`.

```bash
cd demo
uvx create-dagster@1.13.23 project olist_ml --uv-sync
cd olist_ml
cp ../materials/1/envrc .envrc && direnv allow
dg dev                                            # UI на :3000, терминал занят
```

```bash
just demo-1-scaffold                  # create-dagster … --uv-sync в demo/olist_ml
just demo-1-deps                      # .envrc + зависимости + features.py + parquet (заодно шаг D1-2)
just demo-1-dev                       # dg dev проекта демо 1 на :3000 с его DAGSTER_HOME
```

- **UI:**
  - [http://localhost:3000/assets](http://localhost:3000/assets) — каталог ассетов **пуст**: «мы ещё ничего не объявили»;
  - [http://localhost:3000/locations](http://localhost:3000/locations) — code location `olist_ml` загружен без ошибок.
- **Результат / на выходе:**
  - проект `demo/olist_ml/` с `.venv` и `.envrc`, `dg dev` работает, граф пуст;
  - проверка: [`just check-demo-1-step-1-result`](steps.just).

📚 [create-dagster / Getting started](https://docs.dagster.io/getting-started) · [dg CLI](https://docs.dagster.io/api/clis/dg-cli)

---

<a id="d1-2"></a>
## D1-2. Четыре ассета → граф · ⏱ 3 (5)

- **Главный поинт:** рёбра графа — <u>аргументы функций</u>. DAG вручную никто не рисует.
- **Дано / на входе:**
  - пустой проект, `dg dev` запущен (D1-1);
  - [`demo/data/mart_order_features.parquet`](data/README.md), [`materials/1/ml_v1.py`](materials/1/ml_v1.py),
    [`src/olist_ml/ml/features.py`](../src/olist_ml/ml/features.py);
  - проверка: [`just check-demo-1-step-2-input`](steps.just).
- **Шаги** (терминал 2, в `demo/olist_ml/`):
  1. добавить зависимости: `pandas`, `pyarrow`, `scikit-learn`;
  2. скопировать признаки из основного репо: [`features.py`](../src/olist_ml/ml/features.py);
     - feature contract, сплит по `md5(order_id)`, `build_pipeline`, `evaluate`;
     - «ML-код не знает про Dagster, его можно тестировать отдельно»;
  3. положить витрину в `data/` проекта;
  4. положить ассеты [v1](materials/1/ml_v1.py) в [`defs/ml.py`](materials/1/ml_v1.py) и показать код (≈ 45 строк):
     - `mart_order_features()` читает parquet; аргументов нет, поэтому это корень графа;
     - `training_dataset(mart_order_features)` — строки с известным target, train/holdout;
     - `model(training_dataset)` — sklearn Pipeline (preprocessing + LogisticRegression);
     - `model_evaluation(model, training_dataset)` — метрики на holdout;
  5. `dg list defs` → Reload в UI.

```bash
uv add pandas pyarrow scikit-learn
cp ../../src/olist_ml/ml/features.py src/olist_ml/features.py
mkdir -p data && cp ../data/mart_order_features.parquet data/
cp ../materials/1/ml_v1.py src/olist_ml/defs/ml.py
dg list defs                          # 4 ассета: mart_order_features, training_dataset, model, model_evaluation
```

```bash
just demo-1-deps                      # зависимости + features.py + parquet + .envrc в demo/olist_ml
just demo-1-ml v1                     # defs/ml.py ← materials/1/ml_v1.py (4 ассета)
```

- **UI:**
  - [http://localhost:3000/locations](http://localhost:3000/locations) — **Reload** code location `olist_ml`;
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    lineage группы `ml`: четыре узла слева направо, у `model_evaluation` два входа.
- **Результат / на выходе:**
  - граф из 4 ассетов, ни одной материализации;
  - проверка: [`just check-demo-1-step-2-result`](steps.just).

📚 [Assets](https://docs.dagster.io/guides/build/assets) · [Passing data between assets](https://docs.dagster.io/guides/build/assets/passing-data-between-assets)

---

<a id="d1-3"></a>
## D1-3. Materialize all · ⏱ 2 (7)

- **Главный поинт:** _материализация_ — вычисленное и сохранённое значение ассета, а не просто «задача
  выполнилась».
- **Дано / на входе:**
  - граф v1 без материализаций (D1-2);
  - проверка: [`just check-demo-1-step-3-input`](steps.just) (заодно сохраняет снимок счётчиков).
- **Шаги:**
  1. UI: Lineage группы → **Materialize all**;
     - то же командой: `dg launch --assets '*'`;
     - результат: [Runs](http://localhost:3000/runs), последний run **Success**;
  2. открыть run: 4 шага в порядке графа, у каждого свои логи и время;
  3. открыть ассет [`model`](http://localhost:3000/assets/model):
     - последняя материализация, ссылка на run;
     - значение сохранено [IO manager'ом](https://docs.dagster.io/guides/build/io-managers) (pickle в
       `$DAGSTER_HOME/storage`);
     - «следующий ассет берёт его отсюда, а не из памяти процесса»;
  4. реплика про Airflow: «в task-модели XCom — побочный канал; здесь передача данных и есть ребро графа».

```bash
dg launch --assets '*'
```

```bash
just demo-1-launch                    # dg launch --assets '*' в demo/olist_ml с его DAGSTER_HOME
```

- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    кнопка **Materialize all**, узлы становятся зелёными;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — последний run, 4 шага, **Success**;
  - [http://localhost:3000/assets/model](http://localhost:3000/assets/model) — последняя материализация и run.
- **Результат / на выходе:**
  - все 4 ассета материализованы, значения лежат в `demo/olist_ml/.dagster_home/storage/`;
  - проверка: [`just check-demo-1-step-3-result`](steps.just).

📚 [IO managers](https://docs.dagster.io/guides/build/io-managers)

---

<a id="d1-4"></a>
## D1-4. Metadata: метрики без отдельного трекера · ⏱ 2 (9)

- **Главный поинт:** числа, которые ассет вернул в `metadata`, <u>хранятся у каждой материализации</u>, и UI
  сам строит по ним график.
- **Дано / на входе:**
  - v1 материализован (D1-3);
  - проверка: [`just check-demo-1-step-4-input`](steps.just).
- **Шаги:**
  1. заменить [`defs/ml.py`](materials/1/ml_v1.py) на [v2](materials/1/ml_v2_metadata.py) и показать diff: функции возвращают
     `dg.Output(value, metadata={…})`:
     - `mart_order_features`: `rows`, `path`;
     - `training_dataset`: `rows_train`, `rows_holdout`, `positive_rate`, список `features`;
     - `model_evaluation`: `roc_auc`, `pr_auc`, `accuracy`, `rows_holdout`;
  2. Reload → **Materialize all два раза**, чтобы на графике было ≥ 2 точки;
     - то же командой: `dg launch --assets '*'` (дважды);
     - результат: [metadata `model_evaluation`](http://localhost:3000/assets/model_evaluation?view=events);
  3. «Для MLE это привычный run-трекинг, только привязанный к ассету. В [демо 4](4-2-run.md#d4-3) рядом появится
     MLflow, и в metadata будет ссылка на run».

```bash
cp ../materials/1/ml_v2_metadata.py src/olist_ml/defs/ml.py
dg launch --assets '*'
dg launch --assets '*'
```

```bash
just demo-1-ml v2_metadata            # defs/ml.py ← ml_v2_metadata.py (dg.Output + metadata)
just demo-1-launch                    # материализовать все ассеты (1-й раз)
just demo-1-launch                    # 2-й раз — вторая точка на графике
```

- **UI:**
  - [http://localhost:3000/locations](http://localhost:3000/locations) — **Reload**;
  - [http://localhost:3000/assets/training_dataset](http://localhost:3000/assets/training_dataset) — metadata:
    строки, positive rate, список признаков;
  - [http://localhost:3000/assets/model_evaluation?view=events](http://localhost:3000/assets/model_evaluation?view=events) —
    история материализаций и график `roc_auc` (⚠ проверить: в 1.13 график может быть на вкладке **Plots**).
- **Результат / на выходе:**
  - у ассетов есть metadata, у `model_evaluation` виден график метрик по запускам;
  - проверка: [`just check-demo-1-step-4-result`](steps.just).

📚 [Asset metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags)

---

<a id="d1-5"></a>
## D1-5. Asset checks: зелёный и красный · ⏱ 2 (11)

- **Главный поинт:** _проверка_ — отдельная сущность со своим статусом. <u>Ассет может быть материализован, а
  проверка — провалена</u>.
- **Дано / на входе:**
  - v2 материализован (D1-4), [`materials/1/gate_fail.json`](materials/1/gate_fail.json) — порог 0.99;
  - проверка: [`just check-demo-1-step-5-input`](steps.just).
- **Шаги:**
  1. заменить [`defs/ml.py`](materials/1/ml_v1.py) на [v3](materials/1/ml_v3_checks.py); добавились два check'а:
     - `no_leakage` на `training_dataset`: target и поля «из будущего» (`delivery_delay_days`, дата доставки,
       `review_score`) не входят в признаки;
     - `quality_gate` на `model_evaluation`: ROC AUC на holdout ≥ `min_roc_auc` (0.61), `blocking=True`;
  2. **Materialize all** — оба check'а зелёные;
     - то же командой: `dg launch --assets '*'`;
     - результат: [checks `training_dataset`](http://localhost:3000/assets/training_dataset?view=checks),
       [checks `model_evaluation`](http://localhost:3000/assets/model_evaluation?view=checks);
  3. красный gate без подмены модели, только порогом: Launchpad → config (op check'а называется
     `<asset>_<check>`):

     ```yaml
     ops:
       model_evaluation_quality_gate:
         config: {min_roc_auc: 0.99}
     ```

     - то же командой: `dg launch --assets model_evaluation --config-json …` (ниже);
     - результат: [Runs](http://localhost:3000/runs) — run **Failure**, `quality_gate` красный;
  4. «`blocking` значит, что всё ниже по графу не запустится. Здесь ниже ничего нет. В
     [демо 4](4-2-run.md#d4-4) ниже будет регистрация модели, и gate её остановит».

```bash
cp ../materials/1/ml_v3_checks.py src/olist_ml/defs/ml.py
dg list defs                          # + training_dataset:no_leakage, model_evaluation:quality_gate
dg launch --assets '*'                # оба check'а зелёные
dg launch --assets model_evaluation --config-json "$(cat ../materials/1/gate_fail.json)" || true   # gate красный — run падает ожидаемо
```

```bash
just demo-1-ml v3_checks              # defs/ml.py ← ml_v3_checks.py (no_leakage + blocking quality_gate)
just demo-1-launch                    # все ассеты + checks: зелёные
just demo-1-gate-fail                 # model_evaluation с порогом 0.99 → quality_gate красный
```

- **UI:**
  - [http://localhost:3000/assets/training_dataset?view=checks](http://localhost:3000/assets/training_dataset?view=checks) —
    `no_leakage` **Passed**;
  - [http://localhost:3000/assets/model_evaluation?view=checks](http://localhost:3000/assets/model_evaluation?view=checks) —
    `quality_gate`: история passed → failed, metadata `roc_auc_holdout` и `threshold`;
  - [http://localhost:3000/locations/olist_ml/jobs/__ASSET_JOB/playground](http://localhost:3000/locations/olist_ml/jobs/__ASSET_JOB/playground) —
    Launchpad с config (⚠ проверить: из графа открывается через **Materialize ▾ → Open launchpad**);
  - [http://localhost:3000/runs](http://localhost:3000/runs) — последний run **Failure**, упал шаг
    `model_evaluation_quality_gate`.
- **Результат / на выходе:**
  - у `quality_gate` в истории есть и зелёный, и красный результат; материализация `model_evaluation` зелёная;
  - проверка: [`just check-demo-1-step-5-result`](steps.just).

📚 [Asset checks](https://docs.dagster.io/guides/test/asset-checks)

---

<a id="d1-6"></a>
## D1-6. Выборочный пересчёт · ⏱ 1.5 (12.5)

- **Главный поинт:** изменился код модели, значит пересчитываем <u>только модель и то, что ниже</u>. Витрину и
  датасет не трогаем.
- **Дано / на входе:**
  - v3 материализован, у `model` `code_version="v1"`;
  - проверка: [`just check-demo-1-step-6-input`](steps.just) (снимок счётчиков материализаций).
- **Шаги:**
  1. заменить [`defs/ml.py`](materials/1/ml_v1.py) на [v4](materials/1/ml_v4_model_change.py) (diff: `clf__C=0.1`, `code_version="v2"`);
  2. Reload → `model` получает статус **Unsynced**, причина «new code version»;
  3. выделить `model` → **Materialize selected** с downstream (`model+`);
     - то же командой: `dg launch --assets 'model+'`;
     - результат: [Runs](http://localhost:3000/runs) — только `model → model_evaluation → quality_gate`;
  4. на [графике `roc_auc`](http://localhost:3000/assets/model_evaluation?view=events) новая точка, её можно
     сравнить с прошлой версией.

```bash
cp ../materials/1/ml_v4_model_change.py src/olist_ml/defs/ml.py
dg launch --assets 'model+'
```

```bash
just demo-1-ml v4_model_change        # defs/ml.py ← ml_v4_model_change.py (C=0.1, code_version v2)
just demo-1-launch 'model+'           # пересчитать model и всё ниже по графу
```

- **UI:**
  - [http://localhost:3000/locations](http://localhost:3000/locations) — **Reload**;
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    у `model` метка **Unsynced**, витрина и датасет без метки;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — последний run без `mart_order_features` и
    `training_dataset`;
  - [http://localhost:3000/assets/model_evaluation?view=events](http://localhost:3000/assets/model_evaluation?view=events) —
    новая точка `roc_auc`.
- **Результат / на выходе:**
  - у `model` новая материализация, у `training_dataset` и витрины — нет;
  - проверка: [`just check-demo-1-step-6-result`](steps.just).

📚 [Asset versioning / stale](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching) · [Asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax)

---

<a id="d1-7"></a>
## D1-7. Мостик ко второму демо · ⏱ 0.5 (13)

- **Главный поинт:** `mart_order_features` у нас пока просто файл.
- **Дано / на входе:**
  - D1-6 завершён;
  - проверка: [`just check-demo-1-step-7-input`](steps.just).
- **Шаги:**
  1. «В следующей части на месте этого узла будет dbt-проект: SQL-модели и тесты встанут в этот же граф одним
     компонентом, под тем же ключом `mart_order_features`» → [демо 2](2-2-run.md);
  2. «ML-ассеты станут взрослее: MLflow, реестр, promotion, batch inference» → [демо 4](4-2-run.md);
  3. остановить `dg dev` (Ctrl+C): порт 3000 нужен демо 2.
- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    финальный граф демо 1, последний кадр.
- **Результат / на выходе:**
  - демо 1 завершено, порт 3000 свободен → [1-3-results.md](1-3-results.md), затем [2-1-prepare.md](2-1-prepare.md);
  - проверка: [`just check-demo-1-step-7-result`](steps.just) (включает чеклист результата демо).
