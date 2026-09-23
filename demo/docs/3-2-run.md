# Демо 3 — показ: CI + Observability (≈ 3 мин)

[README](README.md) · [3-1-prepare](3-1-prepare.md) · **3-2-run** · [3-3-results](3-3-results.md) · следующее → [4-2-run](4-2-run.md)

**Тезис демо:**

- CI — это <u>те же `just`-рецепты</u>, без сети за данными и без секретов;
- статусы run'ов, checks и freshness видны <u>на одном экране</u> Dagster UI;
- один канал алертов без дополнительной инфраструктуры: и упавший run, и тихо проваленный warn-check.

## Навигация

| # | Слайд | Краткое описание слайда | ⏱ | Накопл. |
|---|---|---|---|---|
| [D3-1](#d3-1) | CI: что именно зелёное | GitHub Actions = install → check → test; локально — `just ci` | 1 | 1 |
| [D3-2](#d3-2) | Алерт: run упал | blocking-check роняет `dq_job` → `alert_on_run_failure` → Telegram | 1 | 2 |
| [D3-3](#d3-3) | Алерт: run зелёный, check жёлтый | warn-check провален при зелёном run → `alert_on_failed_check` | 1 | 3 |

- Команды — из корня репозитория, без `uv run`. UI: [http://localhost:3000](http://localhost:3000).
- `dbt_parse` — функция из [2-2-run.md](2-2-run.md) (прогон [`tests/steps.py`](tests/steps.py) подставляет её сам).
- Проверки шагов: [`just check-demo-3-step-N-input`](steps.just) / [`-result`](steps.just). Прогон всех шагов:
  [`just demo-3-run`](Justfile) / [`just demo-3-run just`](Justfile).

---

<a id="d3-1"></a>
## D3-1. CI: что именно зелёное · ⏱ 1 (1)

- **Главный поинт:** CI вызывает <u>те же `just`-рецепты</u>, без сети за данными и без секретов
  ([ADR-11](../docs/decisions.md#adr-11-ci--вызовы-рецептов-раннера)).
- **Дано / на входе:**
  - [`.github/workflows/ci.yml`](../.github/workflows/ci.yml), последний run на `dev`;
  - проверка: [`just check-demo-3-step-1-input`](steps.just).
- **Шаги:**
  1. [GitHub Actions](https://github.com/dataengy/mlinside-demo-02-dagster/actions) → зелёный run:
     `just install` / `check` / `test`, e2e — отдельный job;
  2. [`ci.yml`](../.github/workflows/ci.yml): каждый шаг — вызов раннера, логики в YAML нет; вместо сети — shipped
     fixtures;
  3. локально то же самое одной командой; деплой и Dagster+ — в docs, не в кадре.

```bash
dbt_parse && dg check defs && ruff check . && pytest   # как `just check test`; install (uv sync, dbt deps) — вне кадра, нужна сеть
```

```bash
just check && just test              # то же, что CI без install: check → test (офлайн)
# just ci                             # не выполнять: полный CI с install (uv sync + dbt deps, нужна сеть)
```

- **UI:**
  - [https://github.com/dataengy/mlinside-demo-02-dagster/actions](https://github.com/dataengy/mlinside-demo-02-dagster/actions) —
    последний run `ci` на `dev`.
- **Fallback:**
  - CI не зелёный — заранее снятый скриншот последнего зелёного run.
- **Результат / на выходе:**
  - понятно, что проверяет CI, и что он повторяется локально;
  - проверка: [`just check-demo-3-step-1-result`](steps.just).

📚 [ADR-11](../docs/decisions.md#adr-11-ci--вызовы-рецептов-раннера) · [`ci.yml`](../.github/workflows/ci.yml)

---

<a id="d3-2"></a>
## D3-2. Алерт: run упал · ⏱ 1 (2)

- **Главный поинт:** упавший run <u>сам приходит</u> в канал — смотреть в UI не нужно.
- **Дано / на входе:**
  - витрина и checks зелёные, [`dbt/models`](../dbt/models/marts/mart_order_features.sql) чистые;
  - сенсоры [`alert_on_run_failure`, `alert_on_failed_check`](../src/olist_ml/defs/automation/sensors.py)
    включены ([P3-2](3-1-prepare.md#p3-2));
  - проверка: [`just check-demo-3-step-2-input`](steps.just) (снимок числа красных `dq_job`).
- **Шаги:**
  1. один экран: [Runs](http://localhost:3000/runs), [Asset checks](http://localhost:3000/assets/mart_order_features?view=checks),
     Freshness;
  2. сломать контракт витрины ([`demo_patch.py break`](../scripts/demo_patch.py)) → пересобрать → checks:
     - blocking-check красный → `dq_job` **Failure**;
     - результат: [Runs](http://localhost:3000/runs);
  3. через ~30 с — сообщение «run dq_job завершился с ошибкой» от `alert_on_run_failure`;
     - результат: [тики сенсора](http://localhost:3000/automation).

```bash
python scripts/demo_patch.py break && dbt_parse
dg launch --job feature_mart_job
dg launch --job dq_job || true                    # красный run → Telegram (~30 с), падает ожидаемо
```

```bash
just demo-break                       # патч: дубли order_id + dbt-parse
just feature-mart                     # пересобрать витрину
just dq || true                       # blocking-check красный → run FAILURE → алерт
```

- **UI:**
  - [http://localhost:3000/runs](http://localhost:3000/runs) — красный `dq_job`;
  - [http://localhost:3000/automation](http://localhost:3000/automation) — тик `alert_on_run_failure` с текстом
    сообщения (dry-run, если `ALERTS_ENABLED=false`).
- **Fallback:**
  - Telegram не пришёл — показать dry-run текст в тиках сенсора.
- **Результат / на выходе:**
  - красный `dq_job` и алерт о нём; витрина пока под патчем `break` (починим в D3-3);
  - проверка: [`just check-demo-3-step-2-result`](steps.just).

📚 [Run failure sensors](https://docs.dagster.io/guides/automate/sensors/run-failure-sensors) · [ADR-18](../docs/decisions.md#adr-18-observability-mvp--dagster-ui--run_failure_sensor--telegram)

---

<a id="d3-3"></a>
## D3-3. Алерт: run зелёный, check жёлтый · ⏱ 1 (3)

- **Главный поинт:** <u>тихая деградация качества</u> тоже приходит в канал, даже если run зелёный.
- **Дано / на входе:**
  - витрина под патчем `break` (после D3-2) или чистая;
  - проверка: [`just check-demo-3-step-3-input`](steps.just).
- **Шаги:**
  1. снять `break` (патч не накладывается поверх патча) и наложить `break-warn`: срок доставки в часах
     вместо дней → warn-check (`severity: warn`) провален;
  2. пересобрать витрину и запустить checks:
     - `dq_job` **Success**, check **жёлтый**;
     - через ~30 с сообщение «check … на mart_order_features провален (severity WARN), run зелёный» от
       `alert_on_failed_check`;
     - результат: [checks витрины](http://localhost:3000/assets/mart_order_features?view=checks), [тики](http://localhost:3000/automation);
  3. вернуть витрину к эталону (обязательно — иначе [демо 4](4-2-run.md) начнётся на сломанных данных);
  4. «Grafana/Prometheus/Loki и drift-отчёты (Evidently) — расширенное демо и docs
     ([ADR-12](../docs/decisions.md#adr-12-docker-compose-и-grafana-стек--вне-mvp-docs-и-расширенное-демо), [ADR-16](../docs/decisions.md#adr-16-evidently--не-в-mvp)). Здесь один экран и один канал».

```bash
python scripts/demo_patch.py fix && python scripts/demo_patch.py break-warn && dbt_parse
dg launch --job feature_mart_job && dg launch --job dq_job        # зелёный run, жёлтый check → Telegram (~30 с)
python scripts/demo_patch.py fix && dbt_parse
dg launch --job feature_mart_job && dg launch --job dq_job
```

```bash
just demo-fix                         # снять break (патч поверх патча не накладывается)
just demo-break-warn                  # warn-патч: срок доставки в часах + dbt-parse
just feature-mart && just dq          # warn-check жёлтый, run SUCCESS → алерт всё равно
just demo-fix                         # вернуть витрину к эталону + dbt-parse
just feature-mart && just dq          # пересобрать витрину, checks зелёные
```

- **UI:**
  - [http://localhost:3000/assets/mart_order_features?view=checks](http://localhost:3000/assets/mart_order_features?view=checks) —
    жёлтый warn-check, затем всё **Passed**;
  - [http://localhost:3000/automation](http://localhost:3000/automation) — тик `alert_on_failed_check`.
- **Результат / на выходе:**
  - второй алерт пришёл; витрина эталонная, checks зелёные → [3-3-results.md](3-3-results.md);
  - проверка: [`just check-demo-3-step-3-result`](steps.just).

📚 [ADR-18](../docs/decisions.md#adr-18-observability-mvp--dagster-ui--run_failure_sensor--telegram) · [Asset checks — severity](https://docs.dagster.io/guides/test/asset-checks)
