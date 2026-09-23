# Демо 4 — результаты: ML

[README](README.md) · [4-1-prepare](4-1-prepare.md) · [4-2-run](4-2-run.md) · **4-3-results** · итог всех демо → [9-3-all-results](9-3-all-results.md)

## Навигация

- [R4-1. Что должно получиться](#r4-1)
- [R4-2. Чеклист результата](#r4-2)
- [R4-3. Что зритель унёс](#r4-3)
- [R4-4. Типовые сбои и fallback](#r4-4)
- [R4-5. Сброс после показа](#r4-5)

---

<a id="r4-1"></a>
## R4-1. Что должно получиться

- **Дано / на входе:**
  - пройдены [D4-1…D4-10](4-2-run.md).
- **Результат / на выходе:**
  - MLflow `olist_late_delivery`: ≥ 2 версий, alias `champion` выставлен;
  - `ml.predictions`: строки с `model_version`, без дублей по `(order_id, batch_id)`;
  - [`dbt/models`](../dbt/models/marts/mart_order_features.sql) возвращены к эталону (D4-10);
  - в Runs: красный `train_job` (gate, D4-4) и красный `score_job` (D4-7), остальные зелёные.
- **UI:**
  - [http://127.0.0.1:5001/#/models/olist_late_delivery](http://127.0.0.1:5001/#/models/olist_late_delivery) — версии и `@champion`;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — история показа.

<a id="r4-2"></a>
## R4-2. Чеклист результата

```bash
python demo/tests/steps.py complete 4
```

```bash
just demo-4-check-complete            # реестр + alias, predictions, dbt/models
```

- [ ] Реестр: ≥ 2 версий, `champion` выставлен — *авто*
- [ ] `ml.predictions`: строки, дублей нет — *авто*
- [ ] `dbt/models` возвращены к эталону — *авто, предупреждение*
- [ ] Главные поинты D4-1…D4-10 произнесены, уложились в ≈ 14.5 мин — вручную

<a id="r4-3"></a>
## R4-3. Что зритель унёс

- По графу видно, из чего получена модель: витрина → выборка → модель → оценка ([`assets.py`](../src/olist_ml/defs/ml/assets.py)).
- Blocking gate останавливает регистрацию; регистрация ≠ promotion, откат — сменой alias.
- Inference берёт версию через `@champion`, `model_version` записан в каждую строку, повтор без дублей.
- Правка SQL витрины пересчитывает только витрину и её ML-потребителей.
- Итог всех демо → [9-3-all-results.md](9-3-all-results.md#r9-1).

<a id="r4-4"></a>
## R4-4. Типовые сбои и fallback

- **D4-4: gate не краснеет:** config не в том op — нужен `model_evaluation_quality_gate`; fallback
  `min_roc_auc: 1.01` или [`just demo-4-gate-fail`](Justfile) ([`gate_fail.json`](materials/2/gate_fail.json)).
- **D4-6: «нет опубликованной модели»:** пропущен D4-5 — `just promote`.
- **D4-8: UI не видит правку SQL:** `just dbt-parse` + **Reload** на [http://localhost:3000/locations](http://localhost:3000/locations).
- **`train_job` пропускает ML-ассеты:** checks витрины красные — `just feature-mart && just dq`.
- **Какой шаг сломан:** `just check-demo-4-step-N-input` / `-result` ([`steps.just`](steps.just)).
- Подробно: [`docs/runbook.md`](../docs/runbook.md).

<a id="r4-5"></a>
## R4-5. Сброс после показа

- **Шаги:**
  1. вернуть состояние «до»: сброс данных и реестра, витрина и checks зелёные.

```bash
dg launch --job clean_all_job && dg launch --job demo_prepare_job
dg launch --job feature_mart_job && dg launch --job dq_job
```

```bash
just demo-4-prepare                   # clean all + demo-prepare + feature-mart + dq
just demo-4-check-ready               # убедиться, что всё снова готово
```

- **Результат / на выходе:**
  - состояние как после [P4-1](4-1-prepare.md#p4-1).
