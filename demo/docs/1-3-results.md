# Демо 1 — результаты

[README](README.md) · [1-1-prepare](1-1-prepare.md) · [1-2-run](1-2-run.md) · **1-3-results** · следующее демо → [2-1-prepare](2-1-prepare.md)

## Навигация

- [R1-1. Что должно получиться](#r1-1)
- [R1-2. Чеклист результата](#r1-2)
- [R1-3. Что зритель унёс](#r1-3)
- [R1-4. Типовые сбои и fallback](#r1-4)
- [R1-5. Сброс после показа](#r1-5)

---

<a id="r1-1"></a>
## R1-1. Что должно получиться

- **Дано / на входе:**
  - пройдены [D1-1…D1-7](1-2-run.md).
- **Результат / на выходе:**
  - проект `demo/olist_ml/`:
    - `.venv`, `.envrc` (копия [`materials/1/envrc`](materials/1/envrc)), [`pyproject.toml`](../pyproject.toml) с `pandas`,
      `pyarrow`, `scikit-learn`;
    - [`src/olist_ml/features.py`](../src/olist_ml/ml/features.py) — байт в байт копия [`src/olist_ml/ml/features.py`](../src/olist_ml/ml/features.py);
    - [`src/olist_ml/defs/ml.py`](materials/1/ml_v4_model_change.py) — финальная версия [`ml_v4_model_change.py`](materials/1/ml_v4_model_change.py);
    - `data/mart_order_features.parquet` ([откуда](data/README.md));
  - история в `demo/olist_ml/.dagster_home`:
    - все 4 ассета материализованы;
    - у `model` материализаций **больше**, чем у `training_dataset` ([D1-6](1-2-run.md#d1-6));
    - `no_leakage` зелёный; у `quality_gate` есть и зелёный, и красный результат ([D1-5](1-2-run.md#d1-5)).
- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    4 зелёных узла, у двух — значки checks;
  - [http://localhost:3000/assets/model_evaluation?view=checks](http://localhost:3000/assets/model_evaluation?view=checks) —
    история `quality_gate`: passed / failed / passed;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — ≥ 5 запусков, среди них один **Failure** (gate).

<a id="r1-2"></a>
## R1-2. Чеклист результата

```bash
python demo/tests/steps.py complete 1
```

```bash
just demo-1-check-complete            # автопроверка всех пунктов ниже по файлам и DAGSTER_HOME проекта
```

- [ ] Проект создан: [`pyproject.toml`](../pyproject.toml), `.venv`, [`.envrc`](materials/1/envrc) — *авто*
- [ ] [`features.py`](../src/olist_ml/ml/features.py) скопирован без изменений — *авто*
- [ ] [`defs/ml.py`](materials/1/ml_v4_model_change.py) = [`ml_v4_model_change.py`](materials/1/ml_v4_model_change.py) — *авто*
- [ ] Материализации: все 4 ассета; `model` > `training_dataset` — *авто*
- [ ] `quality_gate` был и зелёным, и красным — *авто*
- [ ] Главные поинты D1-1…D1-6 произнесены — вручную
- [ ] Уложились в ≈ 13 мин — вручную

<a id="r1-3"></a>
## R1-3. Что зритель унёс

- Ассет — это данные, а не задача: граф строится из аргументов функций, значение сохраняет
  [IO manager](https://docs.dagster.io/guides/build/io-managers).
- Метаданные ассета заменяют простой run-трекинг: история и график без отдельного сервиса.
- Check — отдельный статус: материализация зелёная, проверка красная; `blocking` останавливает downstream.
- Изменение кода видно до пересчёта (**Unsynced**), пересчитывается только нужная часть графа.
- Мостик: «витрина-файл» → dbt-проект в [демо 2](2-2-run.md).

<a id="r1-4"></a>
## R1-4. Типовые сбои и fallback

- **`create-dagster` долго качает пакеты:**
  - кеш не прогрет — см. [P1-3](1-1-prepare.md#p1-3);
  - в кадре: скриншот пустого графа, `just demo-1-scaffold` за кадром.
- **`ModuleNotFoundError: olist_ml.features`:**
  - не скопирован [`features.py`](../src/olist_ml/ml/features.py): `just demo-1-deps` или
    `cp ../../src/olist_ml/ml/features.py src/olist_ml/features.py`.
- **`FileNotFoundError: …/data/mart_order_features.parquet`:**
  - нет копии в проекте: `just demo-1-deps`; нет выгрузки: `just demo-1-prepare` ([P1-2](1-1-prepare.md#p1-2)).
- **После перезапуска `dg dev` пропала история:**
  - не подхватился `.envrc` (`DAGSTER_HOME`): `direnv allow` или `just demo-1-dev`.
- **Порт 3000 занят:**
  - запущено демо 2 — остановить его; временно `DAGSTER_PORT=3001 just demo-1-dev` (тогда URL на `:3001`).
- **Нет графика метрик:**
  - нужно ≥ 2 материализаций с metadata: повторить `just demo-1-launch`.
- **Код ассетов сломан в кадре:**
  - вернуть эталон: `just demo-1-ml v1 | v2_metadata | v3_checks | v4_model_change`
    ([materials/1](materials/1/ml_v1.py)).
- **Какой шаг сломан — непонятно:**
  - `just check-demo-1-step-N-input` / `-result` ([`steps.just`](steps.just)) покажут, что не так.

<a id="r1-5"></a>
## R1-5. Сброс после показа

- **Дано / на входе:**
  - демо 1 показано, результат проверен.
- **Шаги:**
  1. остановить `dg dev`;
  2. удалить проект (его можно пересоздать).

```bash
rm -rf demo/olist_ml
```

```bash
just demo-1-reset                     # удалить demo/olist_ml; parquet в demo/data/ остаётся
```

- **Результат / на выходе:**
  - состояние как после [P1-2](1-1-prepare.md#p1-2), можно репетировать заново.
