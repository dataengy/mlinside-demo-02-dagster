# Демо 3 — результаты: CI + Observability

[README](README.md) · [3-1-prepare](3-1-prepare.md) · [3-2-run](3-2-run.md) · **3-3-results** · следующее → [4-1-prepare](4-1-prepare.md)

## Навигация

- [R3-1. Что должно получиться](#r3-1)
- [R3-2. Чеклист результата](#r3-2)
- [R3-3. Что зритель унёс](#r3-3)
- [R3-4. Типовые сбои и fallback](#r3-4)

---

<a id="r3-1"></a>
## R3-1. Что должно получиться

- **Дано / на входе:**
  - пройдены [D3-1…D3-3](3-2-run.md).
- **Результат / на выходе:**
  - локальный CI (`just ci`) зелёный;
  - два алерта (или два dry-run текста в тиках сенсоров): упавший run и warn-check при зелёном run;
  - витрина эталонная ([`dbt/models`](../dbt/models/marts/mart_order_features.sql) чистые), последний `dq_job` SUCCESS.
- **UI:**
  - [http://localhost:3000/automation](http://localhost:3000/automation) — тики обоих сенсоров;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — красный и зелёные `dq_job`.

<a id="r3-2"></a>
## R3-2. Чеклист результата

```bash
python demo/tests/steps.py complete 3
```

```bash
just demo-3-check-complete            # dbt/models чистые, последний dq_job SUCCESS, красный dq_job в истории
```

- [ ] `dbt/models` возвращены к эталону — *авто*
- [ ] Последний `dq_job` SUCCESS — *авто*
- [ ] В истории есть красный `dq_job` — *авто*
- [ ] Оба алерта пришли (или dry-run) — вручную

<a id="r3-3"></a>
## R3-3. Что зритель унёс

- CI — это вызовы тех же рецептов ([`Justfile`](../Justfile)); логики в YAML нет.
- Один экран (Dagster UI) и один канал (Telegram) закрывают MVP-observability
  ([ADR-18](../docs/decisions.md#adr-18-observability-mvp--dagster-ui--run_failure_sensor--telegram)).
- Warn-check не роняет run, но алерт о нём всё равно приходит.

<a id="r3-4"></a>
## R3-4. Типовые сбои и fallback

- **Алерт не пришёл:** `ALERTS_ENABLED=false` или нет `TG_*` в `.env` — показать dry-run текст на
  [http://localhost:3000/automation](http://localhost:3000/automation); сенсоры выполняет daemon — он поднят
  только вместе с `dg dev`.
- **`патч уже применён`:** [`demo_patch.py`](../scripts/demo_patch.py) не накладывает патч поверх патча —
  сначала `just demo-fix`.
- **CI красный:** скриншот зелёного run; локально `just ci` покажет причину.
- Подробно: [`docs/runbook.md`](../docs/runbook.md).
