# Демо 2 — результаты: dbt

[README](README.md) · [2-1-prepare](2-1-prepare.md) · [2-2-run](2-2-run.md) · **2-3-results** · следующее → [3-1-prepare](3-1-prepare.md)

## Навигация

- [R2-1. Что должно получиться](#r2-1)
- [R2-2. Чеклист результата](#r2-2)
- [R2-3. Что зритель унёс](#r2-3)
- [R2-4. Типовые сбои и fallback](#r2-4)
- [R2-5. Сброс после показа](#r2-5)

---

<a id="r2-1"></a>
## R2-1. Что должно получиться

- **Дано / на входе:**
  - пройдены [D2-1…D2-8](2-2-run.md).
- **Результат / на выходе:**
  - `marts.mart_order_features` материализована, checks витрины зелёные;
  - в истории есть красный `dq_job` и остановленный `train_job` ([D2-6](2-2-run.md#d2-6));
  - [`dbt/models`](../dbt/models/marts/mart_order_features.sql) совпадают с эталоном.
- **UI:**
  - [http://localhost:3000/runs](http://localhost:3000/runs) — история показа;
  - [http://localhost:3000/assets/mart_order_features?view=checks](http://localhost:3000/assets/mart_order_features?view=checks) — последние checks **Passed**.

<a id="r2-2"></a>
## R2-2. Чеклист результата

```bash
python demo/tests/steps.py complete 2
```

```bash
just demo-2-check-complete            # витрина, checks, красный dq_job в истории, dbt/models чистые
```

- [ ] Витрина в DuckDB — *авто*
- [ ] Последний `dq_job` SUCCESS — *авто*
- [ ] В истории есть красный `dq_job` — *авто*
- [ ] `dbt/models` возвращены к эталону — *авто*
- [ ] Главные поинты D2-1…D2-7 произнесены, уложились в ≈ 12.5 мин — вручную

<a id="r2-3"></a>
## R2-3. Что зритель унёс

- dbt подключается к графу одним компонентом ([`defs.yaml`](../src/olist_ml/defs/dbt/defs.yaml)); manifest — артефакт.
- Модель dbt = ассет, `ref()` = ребро; граф связный от raw до ML.
- Тест dbt = asset check со своим статусом; blocking-check витрины останавливает ML.
- Freshness — статус ассета; свежесть данных — отдельный вопрос.
- Дальше: алерты на те же checks и CI → [демо 3](3-2-run.md); ML-ветка → [демо 4](4-2-run.md).

<a id="r2-4"></a>
## R2-4. Типовые сбои и fallback

- **UI не видит свежий SQL:** не пересобран manifest — `just dbt-parse`, затем **Reload** на
  [http://localhost:3000/locations](http://localhost:3000/locations).
- **`train_job` пропускает ML-ассеты без видимой причины:** в DuckDB остались дубли после `demo-break` —
  `just feature-mart && just dq`.
- **`патч уже применён — сначала just demo-fix`:** [`demo_patch.py`](../scripts/demo_patch.py) не накладывает
  патч поверх патча — `just demo-fix`.
- **Freshness не деградировала:** окно ещё не прошло — проговорить словами.
- **Порт 3000 занят демо 1:** остановить `dg dev` проекта `demo/olist_ml`.
- Подробно: [`docs/runbook.md`](../docs/runbook.md).

<a id="r2-5"></a>
## R2-5. Сброс после показа

- **Шаги:**
  1. вернуть состояние «до» для следующей репетиции (UI можно не останавливать).

```bash
dg launch --job clean_all_job && dg launch --job demo_prepare_job
```

```bash
just demo-2-prepare                   # clean all + demo-prepare → состояние «до» (2-1-prepare.md P2-1)
just demo-2-check-ready               # убедиться, что всё снова готово
```

- **Результат / на выходе:**
  - состояние как после [P2-1](2-1-prepare.md#p2-1).
