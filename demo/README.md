# demo — четыре демо к лекции «Оркестрация ML-пайплайнов на Dagster»

**Тезис:** Dagster соединяет dbt-модели и ML-артефакты в один asset graph. По нему видно, из чего получена
модель, что проверено и какой версией рассчитаны предсказания.

| | Демо 1 — Dagster с нуля | Демо 2 — dbt | Демо 3 — CI + Observability | Демо 4 — ML |
|---|---|---|---|---|
| Тема | модель ассетов | dbt-проект в asset graph | CI и алерты | обучение, реестр, inference |
| Где работаем | `demo/olist_ml/` (создаётся в кадре) | корень репозитория | корень репозитория | корень репозитория |
| Показываем | ассеты, lineage, metadata, checks, пересчёт | dbt = ассеты + checks, freshness | CI = рецепты, 2 сенсора → Telegram | gate → registry → promote → score, пересчёт dbt → ML |
| Слайды | D1-1…D1-7 | D2-1…D2-8 | D3-1…D3-3 | D4-1…D4-10 |
| Тайминг | ≈ 13 мин | ≈ 12.5 мин | ≈ 3 мин | ≈ 14.5 мин |

- Порядок показа: 1 → 2 → 3 → 4. Демо 3 идёт сразу после dbt: алерты срабатывают на тех же checks витрины.
  Итог всех демо — в [9-3-all-results.md](docs/9-3-all-results.md#r9-1).

## Навигация

- **Демо 1 — Dagster с нуля:** [1-1-prepare](docs/1-1-prepare.md) · [1-2-run](docs/1-2-run.md) · [1-3-results](docs/1-3-results.md)
- **Демо 2 — dbt:** [2-1-prepare](docs/2-1-prepare.md) · [2-2-run](docs/2-2-run.md) · [2-3-results](docs/2-3-results.md)
- **Демо 3 — CI + Observability:** [3-1-prepare](docs/3-1-prepare.md) · [3-2-run](docs/3-2-run.md) · [3-3-results](docs/3-3-results.md)
- **Демо 4 — ML:** [4-1-prepare](docs/4-1-prepare.md) · [4-2-run](docs/4-2-run.md) · [4-3-results](docs/4-3-results.md)
- **Итог всех демо:** [9-3-all-results](docs/9-3-all-results.md)
- **Задание для Claude Code:** [TODO.md](TODO.md)

## Структура каталога

```
demo/
├── README.md  TODO.md
├── Justfile                # рецепты demo-*; импортируется в ./Justfile
├── steps.just              # check-demo-N-step-X-{input,result} (генерируется из tests/steps.py)
├── {N}-1-prepare.md  {N}-2-run.md  {N}-3-results.md   # N = 1…4
├── 9-3-all-results.md      # итог всех демо, общий чеклист, тайминг, appendix
├── data/                   # parquet витрины для демо 1 (gitignored, кроме README)
├── materials/1/            # демо 1: export_mart.py, envrc, ml_v1…v4, gate_fail.json
├── materials/2/            # демо 4: gate_fail.json, promote_v1.json
├── tests/
│   ├── steps.py            # проверки шагов, чеклисты ready/complete, прогон команд из md
│   └── test_materials_1.py # смоук ml_v1…v4 in-process
├── .state/                 # снимки входа шагов для проверки результата (gitignored)
└── olist_ml/               # проект демо 1, создаётся в кадре (gitignored)
```

Файлы: [`Justfile`](Justfile) · [`steps.just`](steps.just) · [`data/README.md`](data/README.md) ·
[`materials/1/`](materials/1/ml_v1.py) · [`materials/2/`](materials/2/gate_fail.json) ·
[`tests/steps.py`](tests/steps.py) · [`tests/test_materials_1.py`](tests/test_materials_1.py).

## Формат документов

- [`{N}-1-prepare.md`](docs/1-1-prepare.md): цель и краткое содержание демо → оглавление → шаги подготовки → чеклист готовности.
- [`{N}-2-run.md`](docs/1-2-run.md): тезис демо (пункты) → таблица слайдов с кратким описанием → слайды:
  - главный поинт → **Дано / на входе** (+ `just check-demo-N-step-X-input`) → нумерованные **Шаги**
    (подпункты — буллеты; для UI-действий «Materialize…» — эквивалентная `dg`-команда и ссылка на результат)
    → блок `bash` → блок `just` → **UI** → **Fallback** → **Результат / на выходе** (+ `…-result`).
- [`{N}-3-results.md`](docs/1-3-results.md): что должно получиться → чеклист результата → что зритель унёс → сбои → сброс.

## Команды

- **Сырые команды** — без `uv run`: окружение активирует direnv ([`../.envrc`](../.envrc); для демо 1 —
  [`materials/1/envrc`](materials/1/envrc)).
- **`just`-рецепты** — отдельным блоком под сырыми командами, у каждого есть комментарий.

```bash
just --list | grep demo-                  # все рецепты демо
just demo-steps                           # список шагов всех демо

just check-demo-4-step-3-input            # вход шага D4-3 (сохраняет снимок в demo/.state/)
just check-demo-4-step-3-result           # результат шага D4-3 (сравнивает со снимком)
just demo-run-step 4 3                    # шаг D4-3: вход → команды из 4-2-run.md → результат
just demo-4-run                           # все шаги демо 4 сырыми командами
just demo-4-run just                      # все шаги демо 4 через just-блоки

just demo-N-check-ready                   # чеклист готовности демо N (N = 1…4)
just demo-N-check-complete                # чеклист результата демо N
```

- Прогон пропускает команды UI (`dg dev`, `mlflow ui`, `just dev`, `just mlflow`, `just demo-1-dev`) и строки с
  `# не выполнять`; ожидаемо падающие команды в md помечены `|| true`.

## UI

| Что | URL | Комментарий |
|---|---|---|
| Dagster | [http://localhost:3000](http://localhost:3000) | code location во всех демо — `olist_ml` |
| Группа ассетов | [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) | lineage группы (`ml`, `staging`, `marts`, `raw`…) |
| Ассет | [http://localhost:3000/assets/model_evaluation](http://localhost:3000/assets/model_evaluation) | вкладки `?view=events` / `checks` / `lineage` ⚠ проверить |
| Runs | [http://localhost:3000/runs](http://localhost:3000/runs) | история запусков |
| Code locations | [http://localhost:3000/locations](http://localhost:3000/locations) | **Reload** после правки кода |
| Automation | [http://localhost:3000/automation](http://localhost:3000/automation) | сенсоры алертов (демо 3) |
| Launchpad | [http://localhost:3000/locations/olist_ml/jobs/train_job/playground](http://localhost:3000/locations/olist_ml/jobs/train_job/playground) | run config (порог gate) |
| MLflow | [http://127.0.0.1:5001/#/models/olist_late_delivery](http://127.0.0.1:5001/#/models/olist_late_delivery) | версии и alias `champion` (демо 4) |

- **⚠ проверить** — URL или поведение не подтверждены на dagster 1.13.23; аудит — [TODO.md §5](TODO.md#t5).
- Решения — [`docs/decisions.md`](../docs/decisions.md); теория обучения —
  [`docs/theory/training-in-demo.md`](../docs/theory/training-in-demo.md); runbook — [`docs/runbook.md`](../docs/runbook.md).
