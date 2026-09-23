# Демо 4 — подготовка (вне кадра): ML

[README](README.md) · **4-1-prepare** · [4-2-run](4-2-run.md) · [4-3-results](4-3-results.md) · предыдущее → [3-3-results](3-3-results.md)

## Цель

Показать ML-ветку в том же asset graph:
- из чего получена модель,
- что проверено перед регистрацией,
- какой версией рассчитаны предсказания
- — и что изменение витрины пересчитывает только её ML-потребителей.

## Краткое содержание

- Витрина, обучающая выборка и scoring input — три разных набора строк; утечка исключается контрактом витрины.
- Обучение как чёрный ящик: `training_dataset → model → model_evaluation → quality_gate → model_registered`
  ([`assets.py`](../src/olist_ml/defs/ml/assets.py), [`checks.py`](../src/olist_ml/defs/ml/checks.py)), MLflow.
- Blocking gate останавливает регистрацию; регистрация ≠ promotion (alias `champion`,
  [`promotion.py`](../src/olist_ml/defs/promotion.py)).
- Batch inference опубликованной версией ([`inference.py`](../src/olist_ml/defs/ml/inference.py)), `model_version`
  в каждой строке.
- Правка SQL витрины → выборочный пересчёт через границу dbt → Python. Dev vs prod.
- ≈ 14.5 мин, слайды D4-1…D4-10 в [4-2-run.md](4-2-run.md). Итог всех демо — [9-3-all-results.md](9-3-all-results.md#r9-1).

## Навигация

- [P4-1. Состояние «до»](#p4-1)
- [P4-2. Поднять Dagster и MLflow UI](#p4-2)
- [P4-3. Заготовки: run config и патчи](#p4-3)
- [P4-4. Рабочее место](#p4-4)
- [P4-5. Чеклист готовности](#p4-5)

---

<a id="p4-1"></a>
## P4-1. Состояние «до»

- **Дано / на входе:**
  - `just install` выполнен; демо 2–3 показаны или нет — неважно.
- **Шаги:**
  1. сброс и `demo_prepare_job`: raw → витрина + checks → baseline-версия **без алиаса**
     ([ADR-07a](../docs/decisions.md#adr-07a-baseline-версия-к-блоку-scoring--варианты));
  2. витрина пересобрана, checks зелёные.

```bash
dg launch --job clean_all_job && dg launch --job demo_prepare_job
dg launch --job feature_mart_job && dg launch --job dq_job
```

```bash
just demo-4-prepare                   # demo-2-prepare + feature-mart + dq
```

- **Результат / на выходе:**
  - MLflow: модель `olist_late_delivery`, одна версия, alias пуст; `ml.predictions` ещё нет.

<a id="p4-2"></a>
## P4-2. Поднять Dagster и MLflow UI

- **Шаги:**
  1. терминал A — MLflow UI на :5001 (backend — `data/mlflow.db`);
  2. терминал B — `dg dev` на :3000.

```bash
mlflow ui --host 127.0.0.1 --port 5001 --backend-store-uri "sqlite:///$PWD/data/mlflow.db"
dg dev --port 3000
```

```bash
just mlflow                           # MLflow UI на :5001
just dev                              # dbt-parse + dagster.yaml в DAGSTER_HOME + dg dev на :3000
```

- **UI:**
  - [http://127.0.0.1:5001/#/models/olist_late_delivery](http://127.0.0.1:5001/#/models/olist_late_delivery) —
    одна версия, alias пуст;
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) — ML-ветка.
- **Результат / на выходе:**
  - порты 3000 и 5001 слушают.

<a id="p4-3"></a>
## P4-3. Заготовки: run config и патчи

- **Шаги:**
  1. [`materials/2/gate_fail.json`](materials/2/gate_fail.json) — порог gate 0.99 ([D4-4](4-2-run.md#d4-4));
  2. [`materials/2/promote_v1.json`](materials/2/promote_v1.json) — откат `champion` на v1 ([D4-5](4-2-run.md#d4-5));
  3. [`demo_patch.py sql-change`](../scripts/demo_patch.py) — правка SQL витрины ([D4-8](4-2-run.md#d4-8));
  4. `dbt/models` чистые.

```bash
git status --porcelain dbt/models     # пусто
```

```bash
just demo-fix                         # если dbt/models грязные: вернуть витрину к эталону + dbt-parse
```

- **Результат / на выходе:**
  - config'и и патч под рукой.

<a id="p4-4"></a>
## P4-4. Рабочее место

- **Шаги:**
  1. IDE: [`docs/contracts/features.md`](../docs/contracts/features.md),
     [`src/olist_ml/ml/features.py`](../src/olist_ml/ml/features.py),
     [`docs/theory/training-in-demo.md`](../docs/theory/training-in-demo.md) (теория и проверочные вопросы);
  2. браузер: Dagster группа `ml`, MLflow Models, [эксперименты](http://127.0.0.1:5001/#/experiments).
- **Результат / на выходе:**
  - рабочее место готово к [D4-1](4-2-run.md#d4-1).

<a id="p4-5"></a>
## P4-5. Чеклист готовности

```bash
python demo/tests/steps.py ready 4
```

```bash
just demo-4-check-ready               # just check + автопроверка чеклиста ниже
```

- [ ] Базовые пункты (инструменты, данные, manifest, `dbt/models`, :3000) — *авто*
- [ ] Витрина в DuckDB, последний `dq_job` SUCCESS — *авто*
- [ ] Реестр: одна baseline-версия, alias пуст — *авто*
- [ ] MLflow UI слушает :5001 — *авто*
- [ ] `ml.predictions` ещё нет — *авто, предупреждение*
- [ ] Рабочее место (P4-4) — вручную
