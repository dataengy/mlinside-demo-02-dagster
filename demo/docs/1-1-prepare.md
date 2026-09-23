# Демо 1 — подготовка (вне кадра)

[README](README.md) · **1-1-prepare** · [1-2-run](1-2-run.md) · [1-3-results](1-3-results.md) · следующее демо → [2-1-prepare](2-1-prepare.md)

## Цель
Показать модель ассетов Dagster на пустом проекте.
- Зритель должен увидеть, что граф строится из данных,
  - а не из задач, и что метаданные, проверки и пересчёт привязаны к ассету.

## Краткое содержание

- Создаём пустой проект [`create-dagster`](https://docs.dagster.io/getting-started) в `demo/olist_ml` и
  запускаем UI.
- Добавляем четыре ML-ассета на pandas/sklearn, граф строится из аргументов функций.
- Materialize all → metadata с метриками → asset checks (зелёный и красный gate) → выборочный пересчёт `model+`.
- Мостик к [демо 2](2-2-run.md): витрина из файла станет dbt-проектом.
- ≈ 13 мин, слайды D1-1…D1-7 в [1-2-run.md](1-2-run.md).

## Навигация

- [P1-1. Собрать витрину в основном репо](#p1-1)
- [P1-2. Выгрузить витрину в parquet, сбросить проект](#p1-2)
- [P1-3. Прогреть кеш uv (генеральная репетиция)](#p1-3)
- [P1-4. Рабочее место](#p1-4)
- [P1-5. Чеклист готовности](#p1-5)
- [Откуда что берётся](#sources)

---

<a id="sources"></a>
## Откуда что берётся

| Что | Источник | Куда попадает в кадре |
|---|---|---|
| Проект | [`uvx create-dagster`](https://docs.dagster.io/getting-started) | `demo/olist_ml/` (в [`.gitignore`](../.gitignore), создаётся в кадре) |
| Витрина `mart_order_features` | `marts.mart_order_features` в [`data/olist.duckdb`](../data/README.md) | [`materials/1/export_mart.py`](materials/1/export_mart.py) → [`demo/data/mart_order_features.parquet`](data/README.md) → `demo/olist_ml/data/` |
| Признаки, сплит, pipeline, метрики | [`src/olist_ml/ml/features.py`](../src/olist_ml/ml/features.py) (чистые функции, без Dagster и I/O) | [`demo/olist_ml/src/olist_ml/features.py`](../src/olist_ml/ml/features.py) |
| Код ассетов по шагам | [`ml_v1.py`](materials/1/ml_v1.py) → [`ml_v2_metadata.py`](materials/1/ml_v2_metadata.py) → [`ml_v3_checks.py`](materials/1/ml_v3_checks.py) → [`ml_v4_model_change.py`](materials/1/ml_v4_model_change.py) | [`demo/olist_ml/src/olist_ml/defs/ml.py`](materials/1/ml_v1.py) (автозагрузка `defs/`) |
| Окружение проекта | [`materials/1/envrc`](materials/1/envrc) | `demo/olist_ml/.envrc` + `direnv allow` |
| Config красного gate | [`materials/1/gate_fail.json`](materials/1/gate_fail.json) | `dg launch --config-json` / Launchpad |
| Проверки шагов | [`tests/steps.py`](tests/steps.py), рецепты — [`steps.just`](steps.just) | `just check-demo-1-step-N-{input,result}` |

- **Почему parquet, а не CSV:**
  - типы сохраняются (bool target, числа, timestamp), в кадре не нужен код для парсинга;
  - один файл ≈ 4.9k строк;
  - основное демо хранит train/holdout в том же формате ([`data/README.md`](../data/README.md)).
- **Почему не [`src/olist_ml/defs/ml/assets.py`](../src/olist_ml/defs/ml/assets.py):**
  - там DuckDB-ресурс, MLflow, `TrainConfig`, `automation_condition` — это материал [демо 4](4-2-run.md);
  - в демо 1 четыре функции, данные передаёт встроенный [IO manager](https://docs.dagster.io/guides/build/io-managers).

---

<a id="p1-1"></a>
## P1-1. Собрать витрину в основном репо

- **Дано / на входе:**
  - `just install` выполнен ([`Justfile`](../Justfile)), direnv активирует `.venv` корня ([`.envrc`](../.envrc)).
- **Шаги:**
  1. подготовить raw-snapshot и baseline (то же, что в [2-1-prepare](2-1-prepare.md#p2-1));
  2. материализовать витрину `mart_order_features` ([`dbt/models/marts/mart_order_features.sql`](../dbt/models/marts/mart_order_features.sql)).

```bash
dg launch --job demo_prepare_job      # raw-snapshot → DuckDB, витрина, baseline-версия
dg launch --job feature_mart_job      # dbt-модели до mart_order_features
```

```bash
just demo-prepare                     # состояние «до» основного демо: raw, витрина, baseline без алиаса
just feature-mart                     # материализовать витрину признаков (dbt, без тестов)
```

- **Результат / на выходе:**
  - в [`data/olist.duckdb`](../data/README.md) есть `marts.mart_order_features`.

<a id="p1-2"></a>
## P1-2. Выгрузить витрину в parquet, сбросить проект

- **Дано / на входе:**
  - витрина собрана (P1-1).
- **Шаги:**
  1. удалить `demo/olist_ml/` от прошлой репетиции;
  2. выгрузить витрину скриптом [`export_mart.py`](materials/1/export_mart.py) в [`demo/data/`](data/README.md).

```bash
rm -rf demo/olist_ml
python demo/materials/1/export_mart.py demo/data/mart_order_features.parquet   # → «…: 4918 rows»
```

```bash
just demo-1-prepare                   # demo-1-reset + выгрузка витрины DuckDB → demo/data/*.parquet
```

- **Результат / на выходе:**
  - [`demo/data/mart_order_features.parquet`](data/README.md) ≈ 4.9k строк, `demo/olist_ml/` отсутствует.

<a id="p1-3"></a>
## P1-3. Прогреть кеш uv (генеральная репетиция)

- **Дано / на входе:**
  - P1-2 выполнен, есть сеть.
- **Шаги:**
  1. прогнать все шаги [1-2-run.md](1-2-run.md) автоматически: команды из md + проверки входа/результата;
     - `create-dagster` и `uv add` скачают пакеты в кеш uv, в кадре это займёт секунды;
  2. проверить итог по [чеклисту результата](1-3-results.md#r1-2);
  3. сбросить проект.

```bash
python demo/tests/steps.py run 1 --mode bash          # все шаги D1-1…D1-7: сырые команды + проверки
python demo/tests/steps.py complete 1
rm -rf demo/olist_ml
```

```bash
just demo-1-test                      # смоук материалов: ml_v1…v4 материализуются in-process на parquet
just demo-1-run                       # все шаги D1-1…D1-7 сырыми командами из 1-2-run.md + проверки
just demo-1-run just                  # то же, но через just-блоки (проверка fallback-рецептов)
just demo-1-check-complete            # чеклист результата (1-3-results.md)
just demo-1-reset                     # удалить demo/olist_ml перед записью
```

- **Результат / на выходе:**
  - кеш uv прогрет, все шаги зелёные, `demo/olist_ml/` снова отсутствует.

<a id="p1-4"></a>
## P1-4. Рабочее место

- **Дано / на входе:**
  - P1-3 выполнен.
- **Шаги:**
  1. терминал 1 в `demo/` (для `create-dagster` и `dg dev`);
  2. терминал 2 пригодится после D1-1, в `demo/olist_ml/`;
  3. IDE: открыт [`demo/materials/1/`](materials/1/ml_v1.py) — показывать код и diff между версиями;
  4. браузер: вкладка [http://localhost:3000](http://localhost:3000), пока пустая;
  5. основное демо (`just dev`) **не запущено**: порт 3000 нужен демо 1;
  6. шрифт терминала и IDE увеличен, тема светлая, лишние вкладки закрыты;
  7. скриншоты на случай провала сети: пустой граф, граф из 4 ассетов, график `roc_auc`, красный gate.
- **Результат / на выходе:**
  - рабочее место готово к [D1-1](1-2-run.md#d1-1).

<a id="p1-5"></a>
## P1-5. Чеклист готовности

```bash
just demo-1-check-ready               # автопроверка чеклиста ниже + смоук материалов (demo/tests/)
```

- [ ] `demo/data/mart_order_features.parquet` ≈ 4.9k строк, колонки feature contract на месте — *авто*
- [ ] `demo/olist_ml/` **отсутствует** — *авто*
- [ ] [`demo/materials/1/`](materials/1/ml_v1.py): [`ml_v1…v4`](materials/1/ml_v1.py), [`envrc`](materials/1/envrc), [`export_mart.py`](materials/1/export_mart.py), [`gate_fail.json`](materials/1/gate_fail.json) — *авто*
- [ ] `uv`, `uvx`, `direnv` в PATH; `just` — предупреждение — *авто*
- [ ] Порт 3000 свободен — *авто*
- [ ] Смоук [`tests/test_materials_1.py`](tests/test_materials_1.py) зелёный — *авто*
- [ ] Кеш uv прогрет (P1-3) — вручную
- [ ] Хук direnv в shell работает (`direnv status`) — вручную
- [ ] Рабочее место (P1-4) и скриншоты-fallback — вручную
