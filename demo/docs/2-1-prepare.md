# Демо 2 — подготовка (вне кадра): dbt

[README](README.md) · **2-1-prepare** · [2-2-run](2-2-run.md) · [2-3-results](2-3-results.md) · предыдущее → [1-3-results](1-3-results.md) · следующее → [3-1-prepare](3-1-prepare.md)

## Цель

Показать, что dbt-проект становится частью того же asset graph, что и в [демо 1](1-2-run.md). Каждая модель
dbt — отдельный ассет с lineage, тесты dbt — asset checks со своим статусом, а свежесть — статус ассета, а не
ещё один DAG.

## Краткое содержание

- Граница демо: ingestion — предпосылка; raw-snapshot Olist уже в DuckDB.
- Из пустого `dg`-проекта — в готовый: dbt подключается одним компонентом
  [`DbtProjectComponent`](../src/olist_ml/defs/dbt/defs.yaml).
- Модель dbt = ассет, `ref()` = ребро; витрина `mart_order_features` — тот же ключ, что в демо 1.
- Тесты dbt = asset checks: зелёная модель и красный check, ML не обучается на сломанных данных.
- Freshness: свежесть ассета ≠ свежесть данных.
- ≈ 12.5 мин, слайды D2-1…D2-8 в [2-2-run.md](2-2-run.md).

## Навигация

- [P2-1. Сброс и состояние «до»](#p2-1)
- [P2-2. Поднять Dagster UI](#p2-2)
- [P2-3. Заготовки: патчи](#p2-3)
- [P2-4. Рабочее место](#p2-4)
- [P2-5. Чеклист готовности](#p2-5)

- Все команды — из **корня репозитория**, direnv активирует `.venv` основного проекта ([`.envrc`](../.envrc)).
- `dg launch` / `dg dev` берут абсолютные пути dbt и DuckDB из [`defs/env.py`](../src/olist_ml/defs/env.py);
  `just`-рецепты дополнительно экспортируют `DBT_*` и `DUCKDB_PATH` (шапка [`Justfile`](../Justfile)).

---

<a id="p2-1"></a>
## P2-1. Сброс и состояние «до»

- **Дано / на входе:**
  - `just install` выполнен (зависимости, `dbt deps`, manifest); демо 1 остановлено (порт 3000 свободен).
- **Шаги:**
  1. очистить данные: raw, derived, реестр ([ADR-10](../docs/decisions.md#adr-10-три-уровня-очистки-raw--derived--all));
  2. `demo_prepare_job` ([`jobs.py`](../src/olist_ml/defs/jobs.py)): raw-snapshot → DuckDB → витрина + checks →
     baseline-версия в реестре **без алиаса**
     ([ADR-07a](../docs/decisions.md#adr-07a-baseline-версия-к-блоку-scoring--варианты)).

```bash
dg launch --job clean_all_job
dg launch --job demo_prepare_job
```

```bash
just demo-2-prepare                   # = just clean all + just demo-prepare
```

- **Результат / на выходе:**
  - `raw.*` (8 таблиц) и `marts.mart_order_features` в [`data/olist.duckdb`](../data/README.md).

<a id="p2-2"></a>
## P2-2. Поднять Dagster UI

- **Дано / на входе:**
  - P2-1 выполнен.
- **Шаги:**
  1. собрать dbt manifest ([ADR-04b](../docs/decisions.md#adr-04b-prepare_if_dev-false--manifest-собирает-just-dbt-parse-devcheckci-офлайн));
  2. скопировать [`dagster.yaml`](../dagster.yaml) в `DAGSTER_HOME`;
  3. запустить `dg dev` на порту 3000 (отдельный терминал).

```bash
dbt parse --quiet --project-dir "$PWD/dbt" --profiles-dir "$PWD/dbt" --target-path "$PWD/dbt/target"
mkdir -p "$DAGSTER_HOME" && cp -n dagster.yaml "$DAGSTER_HOME/dagster.yaml"
dg dev --port 3000
```

```bash
just dev                              # dbt-parse + dagster.yaml в DAGSTER_HOME + dg dev на :3000
```

- **UI:**
  - [http://localhost:3000/locations](http://localhost:3000/locations) — code location `olist_ml` без ошибок;
  - [http://localhost:3000/assets](http://localhost:3000/assets) — raw, staging, intermediate, marts, ml.
- **Результат / на выходе:**
  - Dagster UI слушает порт 3000.

<a id="p2-3"></a>
## P2-3. Заготовки: патчи

- **Дано / на входе:**
  - [`scripts/demo_patch.py`](../scripts/demo_patch.py): `break` (дубли `order_id`, [D2-6](2-2-run.md#d2-6)),
    `fix` (вернуть витрину к эталону из overlay).
- **Шаги:**
  1. убедиться, что [`dbt/models`](../dbt/models/marts/mart_order_features.sql) чистые (нет остатков патчей);
  2. если грязные — `fix` + пересобрать manifest.

```bash
git status --porcelain dbt/models     # пусто
python scripts/demo_patch.py fix      # только если не пусто
```

```bash
just demo-fix                         # если dbt/models грязные: вернуть витрину к эталону + dbt-parse
```

- **Результат / на выходе:**
  - `dbt/models` совпадают с эталоном.

<a id="p2-4"></a>
## P2-4. Рабочее место

- **Шаги:**
  1. вкладка [http://localhost:3000/locations/olist_ml/asset-groups/marts](http://localhost:3000/locations/olist_ml/asset-groups/marts) — стартовый кадр;
  2. IDE: [`src/olist_ml/defs/dbt/defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml),
     [`dbt/models/marts/_mart_order_features.yml`](../dbt/models/marts/_mart_order_features.yml),
     [`docs/contracts/raw.md`](../docs/contracts/raw.md);
  3. окна freshness в `.env` (`FRESHNESS_WARN_MIN` / `FRESHNESS_FAIL_MIN`, см. [`.env.example`](../.env.example))
     согласованы с таймингом [D2-7](2-2-run.md#d2-7);
  4. терминал свободен для команд показа.
- **Результат / на выходе:**
  - рабочее место готово к [D2-1](2-2-run.md#d2-1).

<a id="p2-5"></a>
## P2-5. Чеклист готовности

```bash
python demo/tests/steps.py ready 2
```

```bash
just demo-2-check-ready               # just check (dbt parse, dg check defs, ruff) + автопроверка чеклиста ниже
```

- [ ] `just check` зелёный — *авто*
- [ ] `dg`, `dbt`, `mlflow` в PATH; `just`, `.env` — предупреждения — *авто*
- [ ] [`data/olist.duckdb`](../data/README.md) и `data/mlflow.db` на месте, raw = 8 таблиц — *авто*
- [ ] [`dbt/target/manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json) собран — *авто*
- [ ] `dbt/models` без остатков demo-патчей — *авто*
- [ ] Dagster UI слушает :3000 — *авто*
- [ ] Рабочее место (P2-4) — вручную
