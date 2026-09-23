# Демо 3 — подготовка (вне кадра): CI + Observability

[README](README.md) · **3-1-prepare** · [3-2-run](3-2-run.md) · [3-3-results](3-3-results.md) · предыдущее → [2-3-results](2-3-results.md) · следующее → [4-1-prepare](4-1-prepare.md)

## Цель

Показать, что проект проверяется и наблюдается теми же простыми средствами, что и запускается: CI вызывает те
же `just`-рецепты, а статусы run'ов и checks видны на одном экране и приходят в один канал алертов.

## Краткое содержание

- CI ([`ci.yml`](../.github/workflows/ci.yml)) = вызовы рецептов [`Justfile`](../Justfile): install → check →
  test, без сети за данными и без секретов.
- Observability: Runs, Asset checks, Freshness в Dagster UI + два сенсора
  ([`sensors.py`](../src/olist_ml/defs/automation/sensors.py)) → Telegram
  ([`telegram.py`](../src/olist_ml/alerts/telegram.py)).
- Сценарий 1: run упал (blocking-check) → алерт. Сценарий 2: run зелёный, warn-check жёлтый → алерт всё равно.
- ≈ 3 мин, слайды D3-1…D3-3 в [3-2-run.md](3-2-run.md). Обычно идёт сразу после [демо 2](2-2-run.md).

## Навигация

- [P3-1. Состояние «до»](#p3-1)
- [P3-2. Dagster UI с демоном сенсоров](#p3-2)
- [P3-3. Канал алертов](#p3-3)
- [P3-4. CI: зелёный run под рукой](#p3-4)
- [P3-5. Чеклист готовности](#p3-5)

---

<a id="p3-1"></a>
## P3-1. Состояние «до»

- **Дано / на входе:**
  - либо только что прошло [демо 2](2-3-results.md), либо чистая машина после `just install`.
- **Шаги:**
  1. если демо 2 не показывалось — подготовить его состояние ([P2-1](2-1-prepare.md#p2-1));
  2. витрина материализована, checks зелёные, [`dbt/models`](../dbt/models/marts/mart_order_features.sql) чистые.

```bash
dg launch --job clean_all_job && dg launch --job demo_prepare_job   # только если демо 2 не показывалось
dg launch --job feature_mart_job && dg launch --job dq_job
```

```bash
just demo-3-prepare                   # demo-2-prepare + feature-mart + dq: витрина и checks зелёные
```

- **Результат / на выходе:**
  - последний `dq_job` — SUCCESS, `dbt/models` чистые.

<a id="p3-2"></a>
## P3-2. Dagster UI с демоном сенсоров

- **Шаги:**
  1. запустить `dg dev` на :3000 — он поднимает и daemon, который выполняет сенсоры (опрос 30 с);
  2. проверить, что сенсоры `alert_on_run_failure`, `alert_on_failed_check` включены.

```bash
dg dev --port 3000
```

```bash
just dev                              # dbt-parse + dagster.yaml в DAGSTER_HOME + dg dev на :3000
```

- **UI:**
  - [http://localhost:3000/automation](http://localhost:3000/automation) — оба сенсора **Running**.
- **Результат / на выходе:**
  - сенсоры работают, тики идут.

<a id="p3-3"></a>
## P3-3. Канал алертов

- **Шаги:**
  1. в `.env` (шаблон — [`.env.example`](../.env.example)): `ALERTS_ENABLED=true`, `TG_BOT_TOKEN`, `TG_CHAT_ID`;
     - без них — dry-run: текст сообщения пишется в лог тика ([ADR-18](../docs/decisions.md#adr-18-observability-mvp--dagster-ui--run_failure_sensor--telegram));
  2. отправить тестовое сообщение.

```bash
python -m olist_ml.alerts.telegram "проверка канала"
```

```bash
just tg-chat-id                       # узнать TG_CHAT_ID: бот в группе + любое сообщение в ней
just tg-test "проверка канала"        # живое сообщение или dry-run в лог
```

- **Результат / на выходе:**
  - сообщение пришло (или dry-run текст в выводе).

<a id="p3-4"></a>
## P3-4. CI: зелёный run под рукой

- **Шаги:**
  1. открыть [GitHub Actions](https://github.com/dataengy/mlinside-demo-02-dagster/actions) — последний run `ci`
     на `dev` зелёный;
  2. открыть в IDE [`.github/workflows/ci.yml`](../.github/workflows/ci.yml);
  3. снять скриншот зелёного run (fallback).
- **Результат / на выходе:**
  - вкладка и файл открыты.

<a id="p3-5"></a>
## P3-5. Чеклист готовности

```bash
python demo/tests/steps.py ready 3
```

```bash
just demo-3-check-ready               # just check + автопроверка чеклиста ниже
```

- [ ] Базовые пункты демо 2 (инструменты, данные, manifest, `dbt/models`, :3000) — *авто*
- [ ] Витрина в DuckDB, последний `dq_job` SUCCESS — *авто*
- [ ] [`ci.yml`](../.github/workflows/ci.yml) на месте — *авто*
- [ ] Сенсоры **Running**, канал проверен (P3-2, P3-3) — вручную
- [ ] Зелёный run в GitHub Actions открыт (P3-4) — вручную
