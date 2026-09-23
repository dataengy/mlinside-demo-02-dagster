# Демо 4 — показ: ML в asset graph (≈ 14.5 мин)

[README](README.md) · [4-1-prepare](4-1-prepare.md) · **4-2-run** · [4-3-results](4-3-results.md)

**Тезис демо:**

- по графу видно, <u>из чего получена модель</u>: витрина → выборка → модель → оценка;
- <u>что проверено</u>: blocking gate останавливает регистрацию, регистрация ≠ promotion;
- <u>какой версией рассчитаны предсказания</u>: модель приходит через alias, версия — в каждой строке;
- изменение витрины пересчитывает только её ML-потребителей, через границу dbt → Python.

## Навигация

| # | Слайд | Краткое описание слайда | ⏱ | Накопл. |
|---|---|---|---|---|
| [D4-1](#d4-1) | Три набора строк | витрина / training dataset / scoring input — разные объекты | 1.5 | 1.5 |
| [D4-2](#d4-2) | Утечка | `delivery_delay_days` исключён контрактом витрины | 1 | 2.5 |
| [D4-3](#d4-3) | Обучение как чёрный ящик | `train_job`: выборка → модель → оценка → gate → версия в реестре | 2.5 | 5 |
| [D4-4](#d4-4) | Gate падает | порог 0.99 через config → gate красный, регистрации нет | 2 | 7 |
| [D4-5](#d4-5) | Регистрация ≠ promotion | `champion` переводится явным действием; откат без переобучения | 1.5 | 8.5 |
| [D4-6](#d4-6) | Batch inference | `score_job` версией `@champion`, `model_version` в строках, без дублей | 2 | 10.5 |
| [D4-7](#d4-7) | Без champion | понятная остановка вместо traceback | 0.5 | 11 |
| [D4-8](#d4-8) | Меняем SQL витрины | новая code version → витрина Unsynced | 1 | 12 |
| [D4-9](#d4-9) | Пересчитываем только нужное | витрина + ML-потребители, без raw и staging | 2 | 14 |
| [D4-10](#d4-10) | Dev vs prod | оси отличий через ML-процесс | 0.5 | 14.5 |

- Команды — из корня репозитория, без `uv run`. UI: Dagster [http://localhost:3000](http://localhost:3000),
  MLflow [http://127.0.0.1:5001](http://127.0.0.1:5001).
- `dbt_parse` — функция из [2-2-run.md](2-2-run.md) (прогон [`tests/steps.py`](tests/steps.py) подставляет её сам).
- Проверки шагов: [`just check-demo-4-step-N-input`](steps.just) / [`-result`](steps.just). Прогон всех шагов:
  [`just demo-4-run`](Justfile) / [`just demo-4-run just`](Justfile).

---

<a id="d4-1"></a>
## D4-1. Три набора строк одной витрины · ⏱ 1.5 (1.5)

- **Главный поинт:** _feature mart_, _training dataset_ и _scoring input_ — <u>разные объекты</u>, а не одна таблица.
- **Дано / на входе:**
  - витрина [`mart_order_features`](../dbt/models/marts/mart_order_features.sql), контракт
    [`docs/contracts/features.md`](../docs/contracts/features.md);
  - проверка: [`just check-demo-4-step-1-input`](steps.just).
- **Шаги:**
  1. задача: «по информации, доступной при оформлении заказа, оценить риск, что заказ доставят позже срока»;
  2. три набора:
     - витрина — признаки;
     - `training_dataset` — исторические строки с известным target;
     - `scoring_input` — строки, где target ещё неизвестен (заказ в пути);
  3. у недоставленного заказа `is_late_delivery` = `NULL` («ещё не знаем»), а не 0;
  4. открыть контракт или [yml витрины](../dbt/models/marts/_mart_order_features.yml): 8–10 признаков и правило
     «известно в момент оформления» ([ADR-14](../docs/decisions.md#adr-14-контракты-данных-raw-и-feature-contract)).
- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    `training_dataset` и `scoring_input` читают одну витрину.
- **Результат / на выходе:**
  - в витрине есть строки и с известным, и с неизвестным target;
  - проверка: [`just check-demo-4-step-1-result`](steps.just).

📚 [теория §1–2](../docs/theory/training-in-demo.md#1-задача-и-target)

---

<a id="d4-2"></a>
## D4-2. Единственная ML-остановка: утечка · ⏱ 1 (2.5)

- **Главный поинт:** `delivery_delay_days` даёт почти идеальную метрику. <u>Это не хорошая модель, а утечка
  будущего.</u>
- **Дано / на входе:**
  - feature contract (D4-1), признаки в [`features.py`](../src/olist_ml/ml/features.py);
  - проверка: [`just check-demo-4-step-2-input`](steps.just).
- **Шаги:**
  1. этого поля нет в момент реального scoring, поэтому оно не входит в feature contract; то же — отзывы,
     финальный статус, агрегаты «за весь период»;
  2. «Утечка исключается на уровне витрины и контракта, а не в коде модели. Это работа DE»;
     - в [демо 1](1-2-run.md#d1-5) то же правило было оформлено как check `no_leakage`;
  3. конкретный AUC не обещаем, пока не воспроизведём его на текущем коде.
- **Результат / на выходе:**
  - target и `delivery_delay_days` не входят в признаки;
  - проверка: [`just check-demo-4-step-2-result`](steps.just).

📚 [теория §2](../docs/theory/training-in-demo.md#2-feature-contract-и-утечка) · [ADR-06](../docs/decisions.md#adr-06-ml--чёрный-ящик-одна-детерминированная-модель-810-признаков-один-blocking-gate)

---

<a id="d4-3"></a>
## D4-3. Обучение как чёрный ящик · ⏱ 2.5 (5)

- **Главный поинт:** нас интересует <u>откуда пришли признаки, что прошло проверку и что попало в реестр</u>.
- **Дано / на входе:**
  - витрина и checks зелёные; в реестре только baseline v1;
  - проверка: [`just check-demo-4-step-3-input`](steps.just) (снимок числа версий).
- **Шаги:**
  1. запустить `train_job` ([`jobs.py`](../src/olist_ml/defs/jobs.py)) — **Materialize** ML-ветки;
     - то же командой: `dg launch --job train_job`;
     - результат: [группа ml](http://localhost:3000/locations/olist_ml/asset-groups/ml), [Runs](http://localhost:3000/runs);
  2. пройти по ассетам ([`assets.py`](../src/olist_ml/defs/ml/assets.py)):
     - `training_dataset`: rows train/holdout, positive rate, `dataset_fingerprint`;
     - `model`: ссылка на MLflow run; `model_evaluation`: ROC AUC на holdout;
     - `quality_gate` зелёный; `model_registered`: `model_version`, `reused_existing`;
  3. «Внутри обычный sklearn Pipeline, тот же [`features.py`](../src/olist_ml/ml/features.py), что в демо 1»;
  4. кликнуть `mlflow_url` → run в MLflow: параметры, метрики, артефакт.

```bash
dg launch --job train_job
```

```bash
just train                            # checks витрины → training_dataset → model → evaluation → gate → версия без алиаса
```

- **UI:**
  - [http://localhost:3000/assets/model_registered](http://localhost:3000/assets/model_registered) —
    metadata `model_version`, `registry_url`;
  - [http://127.0.0.1:5001/#/experiments](http://127.0.0.1:5001/#/experiments) — run кандидата.
- **Результат / на выходе:**
  - в реестре v1 (baseline) и v2 (кандидат), alias пуст;
  - проверка: [`just check-demo-4-step-3-result`](steps.just).

📚 [теория §3–5](../docs/theory/training-in-demo.md#3-train--holdout-и-snapshot) · [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html) · [ADR-06](../docs/decisions.md#adr-06-ml--чёрный-ящик-одна-детерминированная-модель-810-признаков-один-blocking-gate) · [ADR-07](../docs/decisions.md#adr-07-свой-mlflowresource-регистрация--promotion-идемпотентность-по-dataset_fingerprint)

---

<a id="d4-4"></a>
## D4-4. Gate падает — регистрации нет · ⏱ 2 (7)

- **Главный поинт:** _blocking check_ по ROC AUC — <u>одна метрика, одна причина остановки</u>.
- **Дано / на входе:**
  - v2 зарегистрирован (D4-3); [`materials/2/gate_fail.json`](materials/2/gate_fail.json) — порог 0.99;
  - проверка: [`just check-demo-4-step-4-input`](steps.just) (снимок версий и alias).
- **Шаги:**
  1. [Launchpad `train_job`](http://localhost:3000/locations/olist_ml/jobs/train_job/playground) → run config
     (op check'а `<asset>_<check>`, как в [демо 1](1-2-run.md#d1-5)):

     ```yaml
     ops:
       model_evaluation_quality_gate:
         config: {min_roc_auc: 0.99}
     ```

     - то же командой: `dg launch --job train_job --config-json …` (ниже);
     - результат: [checks `model_evaluation`](http://localhost:3000/assets/model_evaluation?view=checks);
  2. порог по умолчанию `ML_MIN_ROC_AUC=0.61` ([`.env.example`](../.env.example)), измерен на snapshot;
  3. `model_evaluation` зелёный, `quality_gate` **красный**, `model_registered` <u>не запущен</u>; в MLflow новой
     версии нет, alias не тронут;
  4. «Порог мы подняли честно, через конфигурацию, а не подсунули модель похуже».

```bash
dg launch --job train_job --config-json "$(cat demo/materials/2/gate_fail.json)" || true   # gate красный, падает ожидаемо
```

```bash
just demo-4-gate-fail                 # train_job с порогом 0.99 → gate красный, model_registered не запускается
```

- **UI:**
  - [http://localhost:3000/assets/model_evaluation?view=checks](http://localhost:3000/assets/model_evaluation?view=checks) —
    `quality_gate` **Failed**, `threshold: 0.99`;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — `train_job` **Failure**, `model_registered` не выполнен;
  - [http://127.0.0.1:5001/#/models/olist_late_delivery](http://127.0.0.1:5001/#/models/olist_late_delivery) — по-прежнему v1 и v2.
- **Fallback:**
  - gate не краснеет — `min_roc_auc: 1.01`.
- **Результат / на выходе:**
  - новой версии нет, реестр не изменился;
  - проверка: [`just check-demo-4-step-4-result`](steps.just).

📚 [теория §5](../docs/theory/training-in-demo.md#5-метрика-и-gate) · [Asset checks — blocking](https://docs.dagster.io/guides/test/asset-checks) · [ADR-06](../docs/decisions.md#adr-06-ml--чёрный-ящик-одна-детерминированная-модель-810-признаков-один-blocking-gate)

---

<a id="d4-5"></a>
## D4-5. Регистрация ≠ promotion · ⏱ 1.5 (8.5)

- **Главный поинт:** версия в реестре ещё не production. <u>`champion` переводится явным действием</u>.
- **Дано / на входе:**
  - v1 (baseline) и v2 (кандидат), alias пуст;
  - проверка: [`just check-demo-4-step-5-input`](steps.just).
- **Шаги:**
  1. promote последней версии ([`promotion.py`](../src/olist_ml/defs/promotion.py)) → alias `champion` → v2;
     - результат: [MLflow Models](http://127.0.0.1:5001/#/models/olist_late_delivery), metadata
       `previous_version` → `version` в [Runs](http://localhost:3000/runs);
  2. «Откат — это promote с версией 1 ([`promote_v1.json`](materials/2/promote_v1.json)), переобучать ничего не
     нужно» — проговорить;
  3. «После провала gate (D4-4) версии не появилось, champion не изменился. <u>"Последняя версия = production"</u>
     здесь невозможно».

```bash
dg launch --job promote_job
# dg launch --job promote_job --config-json "$(cat demo/materials/2/promote_v1.json)"   # откат на v1 — только показать
```

```bash
just promote                          # alias champion → последняя версия реестра
```

- **UI:**
  - [http://127.0.0.1:5001/#/models/olist_late_delivery](http://127.0.0.1:5001/#/models/olist_late_delivery) — у v2 `@champion`.
- **Fallback:**
  - alias не переключился — MLflow UI: Models → версия → **Add alias** `champion`.
- **Результат / на выходе:**
  - `champion` = последняя версия;
  - проверка: [`just check-demo-4-step-5-result`](steps.just).

📚 [MLflow Model Registry — aliases](https://mlflow.org/docs/latest/model-registry.html) · [теория §6](../docs/theory/training-in-demo.md#6-регистрация-promotion-алиасы) · [ADR-07](../docs/decisions.md#adr-07-свой-mlflowresource-регистрация--promotion-идемпотентность-по-dataset_fingerprint)

---

<a id="d4-6"></a>
## D4-6. Batch inference опубликованной версией · ⏱ 2 (10.5)

- **Главный поинт:** scoring <u>не зависит от run обучения</u>. Модель приходит через alias, версия записана в
  каждую строку.
- **Дано / на входе:**
  - `champion` выставлен (D4-5);
  - проверка: [`just check-demo-4-step-6-input`](steps.just).
- **Шаги:**
  1. запустить `score_job` ([`inference.py`](../src/olist_ml/defs/ml/inference.py)) — **Materialize** группы `inference`;
     - то же командой: `dg launch --job score_job`;
     - результат: [`predictions`](http://localhost:3000/assets/predictions);
  2. `scoring_input` — заказы с ещё неизвестным target (на snapshot их 138);
  3. `predictions`, в metadata: rows, mean score, predicted-positive rate, **`model_version`**, `batch_id`;
  4. «Training и scoring во время выполнения не связаны: модель передаётся через alias. Реальное качество станет
     известно, когда придут labels» — mean score ≠ drift и ≠ качество;
  5. повторный запуск не создаёт дублей: идемпотентность по `batch_id` ([ADR-13](../docs/decisions.md#adr-13-обучение-и-инференс--разные-контуры-в-mvp--минимальный-непартиционированный-batch-inference)).

```bash
dg launch --job score_job
dg launch --job score_job             # повтор — дублей нет
```

```bash
just score                            # batch inference: scoring_input → predictions версией @champion
just score                            # повтор того же batch_id — строк не прибавилось
```

- **UI:**
  - [http://localhost:3000/assets/predictions](http://localhost:3000/assets/predictions) — metadata `model_version`, `batch_id`;
  - [http://localhost:3000/assets/scoring_input](http://localhost:3000/assets/scoring_input) — `rows: 138`.
- **Результат / на выходе:**
  - `ml.predictions` заполнена, `model_version` = версия `champion`, дублей нет;
  - проверка: [`just check-demo-4-step-6-result`](steps.just).

📚 [теория §7](../docs/theory/training-in-demo.md#7-batch-inference) · [ADR-13](../docs/decisions.md#adr-13-обучение-и-инференс--разные-контуры-в-mvp--минимальный-непартиционированный-batch-inference)

---

<a id="d4-7"></a>
## D4-7. Без champion — понятная остановка · ⏱ 0.5 (11)

- **Главный поинт:** отсутствие опубликованной модели — <u>ожидаемое состояние</u>, а не traceback.
- **Дано / на входе:**
  - `champion` выставлен;
  - проверка: [`just check-demo-4-step-7-input`](steps.just).
- **Шаги:**
  1. снять alias → scoring завершается понятным сообщением «нет опубликованной модели — выполните promote»;
     - результат: [Runs](http://localhost:3000/runs) — `score_job` **Failure** без traceback;
  2. вернуть alias; показывать, только если есть 30 секунд.

```bash
dg launch --job demote_job
dg launch --job score_job || true     # ожидаемая остановка с понятным текстом
dg launch --job promote_job
```

```bash
just demote                           # снять alias champion
just score || true                    # ожидаемая остановка с понятным текстом
just promote                          # вернуть champion на последнюю версию
```

- **Результат / на выходе:**
  - был понятный провал `score_job`, `champion` снова выставлен;
  - проверка: [`just check-demo-4-step-7-result`](steps.just).

📚 [ADR-13](../docs/decisions.md#adr-13-обучение-и-инференс--разные-контуры-в-mvp--минимальный-непартиционированный-batch-inference)

---

<a id="d4-8"></a>
## D4-8. Меняем SQL витрины · ⏱ 1 (12)

- **Главный поинт:** Dagster знает, что <u>код ассета изменился</u>, ещё до пересчёта — как с Python в
  [демо 1](1-2-run.md#d1-6), так и с SQL.
- **Дано / на входе:**
  - витрина и ML-ветка синхронизированы, `dbt/models` чистые;
  - проверка: [`just check-demo-4-step-8-input`](steps.just).
- **Шаги:**
  1. безобидная правка [SQL витрины](../dbt/models/marts/mart_order_features.sql)
     ([`demo_patch.py sql-change`](../scripts/demo_patch.py)) + `dbt parse`;
  2. [Reload definitions](http://localhost:3000/locations) ([ADR-04b](../docs/decisions.md#adr-04b-prepare_if_dev-false--manifest-собирает-just-dbt-parse-devcheckci-офлайн));
  3. `mart_order_features` получает статус **Unsynced** («has a new code version»; в dagster-dbt `code_version` =
     `sha1(raw_sql)`);
  4. «Downstream ML-ассеты <u>не помечаются транзитивно</u>: их код не менялся».

```bash
python scripts/demo_patch.py sql-change && dbt_parse
```

```bash
just demo-sql-change                  # правка SQL mart_order_features + dbt-parse (manifest)
```

- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/marts](http://localhost:3000/locations/olist_ml/asset-groups/marts) —
    у витрины **Unsynced**;
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) — ML без метки.
- **Fallback:**
  - **Unsynced** не отобразился — всё равно выполнить D4-9 и показать по run events, что запускалось.
- **Результат / на выходе:**
  - SQL витрины изменён, пересчёта ещё не было;
  - проверка: [`just check-demo-4-step-8-result`](steps.just).

📚 [Code versions / stale](https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching) · [ADR-17](../docs/decisions.md#adr-17-выборочный-пересчёт-и-freshness--центральный-dagster-сценарий)

---

<a id="d4-9"></a>
## D4-9. Пересчитываем только нужное · ⏱ 2 (14)

- **Главный поинт:** пересчитываются <u>выбранная витрина и её ML-потребители</u>, без raw и неизменившегося staging.
- **Дано / на входе:**
  - витрина **Unsynced** (D4-8);
  - проверка: [`just check-demo-4-step-9-input`](steps.just) (снимок счётчиков витрины, реестра, raw, staging).
- **Шаги:**
  1. в UI выбрать `mart_order_features` + `training_dataset … model_registered` → **Materialize selected**;
     - то же командой: `dg launch --assets "mart_order_features,training_dataset,model,model_evaluation,model_registered"`;
     - результат: [Runs](http://localhost:3000/runs) — последний run;
  2. по run events: `dbt_feature_branch → training_dataset → model → model_evaluation → quality_gate →
     model_registered`; `raw/*` и staging не запускались;
  3. не обещаем: что все downstream станут Unsynced; что статический job сам выберет только Unsynced;
  4. реплика про Cosmos: «здесь — выбранные ассеты по факту изменения, и Python-потребители в той же выборке».

```bash
dg launch --assets "mart_order_features,training_dataset,model,model_evaluation,model_registered"
```

```bash
just demo-recompute                   # витрина + её ML-потребители; raw и staging не запускаются
```

- **UI:**
  - [http://localhost:3000/locations/olist_ml/asset-groups/ml](http://localhost:3000/locations/olist_ml/asset-groups/ml) —
    выделение и **Materialize selected**;
  - [http://localhost:3000/runs](http://localhost:3000/runs) — только перечисленные шаги.
- **Результат / на выходе:**
  - витрина и `model_registered` пересчитаны, `raw/orders` и `stg_orders` — нет;
  - проверка: [`just check-demo-4-step-9-result`](steps.just).

📚 [ADR-17](../docs/decisions.md#adr-17-выборочный-пересчёт-и-freshness--центральный-dagster-сценарий) · [Asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax)

---

<a id="d4-10"></a>
## D4-10. Dev vs prod через ML-процесс · ⏱ 0.5 (14.5)

- **Главный поинт:** различаются <u>данные, окно, promotion, расписания, состояние и executor</u>, а не загрузчики.
- **Дано / на входе:**
  - весь путь пройден на snapshot;
  - проверка: [`just check-demo-4-step-10-input`](steps.just).
- **Шаги:**
  1. «В разработке проверяем весь путь на небольшом snapshot и пишем в отдельный эксперимент»;
  2. «В проде — утверждённые данные и ресурсы, кандидата проверяем и отдельно допускаем к применению; инференс
     берёт опубликованную версию, обучение при этом не запускается»;
  3. оговорка: dev-прогон отвечает «пайплайн работает», а не «модель хорошая»;
  4. вне кадра — вернуть витрину к эталону после D4-8.

```bash
python scripts/demo_patch.py fix && dbt_parse     # вне кадра: убрать sql-change
```

```bash
just demo-fix                         # вне кадра: вернуть витрину к эталону + dbt-parse
```

- **Результат / на выходе:**
  - оси dev / prod названы, витрина эталонная → [итог всех демо](9-3-all-results.md#r9-1);
  - проверка: [`just check-demo-4-step-10-result`](steps.just).

📚 [`docs/overview.md` §4.1](../docs/overview.md#41-dev-vs-prod-через-ml-процесс-а-не-через-загрузчики)
