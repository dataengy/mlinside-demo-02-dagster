# Демо 3 — ML: проверка задачи на реальных данных

Лейн `ml`, подготовка `mlinside-demo/02-dagster`. Всё проверено на
`/Users/user/gi/@dataengy/mlinside-hw-olist/dbt/olist.duckdb` (read-only, 141 МБ,
DuckDB 1.5.5, sklearn 1.9.1, Python 3.12). Реальные репозитории не менялись.

---

## 1. Инвентаризация БД

Схемы: `main_seeds`, `main_staging`, `main_intermediate`, `main_marts`,
`main` (артефакты `dbt_project_evaluator`), `main_test_failures` (60 таблиц
`store_failures` — это не данные, а сохранённые падения тестов).

| Таблица | Строк |
|---|---|
| `main_intermediate.int_orders_enriched` | **99 441** |
| `main_intermediate.int_order_items_enriched` | 112 650 |
| `main_intermediate.int_order_distance` | 98 666 |
| `main_staging.stg_orders` / `stg_customers` | 99 441 |
| `main_staging.stg_order_items` | 112 650 |
| `main_staging.stg_products` | 32 951 |
| `main_marts.mart_customer_value_profile` | 96 096 |
| `main_marts.mart_daily_state_metrics` | 10 742 |

### Таргет и полнота (по `int_orders_enriched`)

| Показатель | Значение | Доля |
|---|---|---|
| Всего заказов | 99 441 | 100 % |
| Доставлено (`is_delivered = 1`) | 96 476 | 97.02 % |
| Отменено (`is_canceled = 1`) | 1 234 | 1.24 % |
| **Просрочено среди доставленных** | **6 535** | **6.774 %** |
| Заказы без позиций | 775 | 0.78 % (среди доставленных — **0**) |
| Заказы без платежа | 1 | 0.001 % |
| Заказы без отзыва | 768 | 0.77 % |
| Заказы без расстояния до продавца | 1 265 | 1.27 % (среди доставленных — 477) |
| Заказы без обещанной даты доставки | 0 | — |

Период: 2016-09-04 … 2018-10-17. Доля просрочек нестабильна по месяцам:
0.7 % (2016-10) … 12.4 % (2017-11, Black Friday) … **18.96 % (2018-03)**,
и падает до 1.2 % (2018-06). Это главный источник сигнала — и главный риск
(см. п. 7).

---

## 2. Витрина признаков `mart_order_features`

Файл: `mart_order_features.sql` (стиль проекта: русский `{# #}`-комментарий,
`{{ ref() }}`, `{{ safe_div() }}`, `{{ dbt.datediff() }}`, `ch_table_config`).
Гранулярность — **одна строка на доставленный заказ, 96 476**, проверено.

Требуется один новый макрос — `month_of()` в `macros/cross_db/date_parts.sql`
(рядом с `hour_of` / `dow_of`), готовый текст в `macro_month_of.sql`.
DuckDB-only функций нет; `row_number() over (...)`, `coalesce`, `left join`,
`extract`/`toMonth` через dispatch — работают на обоих таргетах.

**Признаки (22): только то, что известно в момент покупки.**

- корзина: `items_cnt`, `distinct_products_cnt`, `distinct_sellers_cnt`;
- деньги: `items_value`, `freight_value`, `order_value`, `freight_share`
  (через `safe_div`), `is_large_order`;
- платёж: `main_payment_type` (5), `max_installments`;
- география: `customer_state` (27), `customer_region` (5),
  `customer_seller_distance_km` (477 NULL — оставлены как есть, импьютер в Pipeline);
- время: `order_hour`, `order_dow`, `purchase_month`,
  `estimated_delivery_span_days` = `datediff(purchase, estimated)`;
- товары: `main_product_category` (72, доминирующая категория по сумме цен позиций
  с тай-брейком по имени), `total_weight_g`, `total_volume_cm3`, `max_item_weight_g`.

**Исключены как утечка:** `delivery_days`, `delivery_delay_days`,
`order_delivered_customer_date`, `review_score`, `is_bad_review`,
`is_good_review`, `has_review`, а также `order_status` / `is_canceled`
(финальный статус, не стартовый). Проверено, что это не теория:

| Что подмешали к 3 безобидным признакам | ROC AUC | PR AUC |
|---|---|---|
| `delivery_delay_days` | **1.0000** | 1.0000 |
| `delivery_days` | 0.9707 | 0.7969 |
| `review_score` | 0.8292 | 0.3458 |
| ничего (база) | 0.5846 | 0.0871 |

Стоимость витрины: SQL отрабатывает за **0.14 с** на полном объёме — категорию
и габариты товаров брать дёшево, отдельная «лёгкая» версия не нужна.

Эквивалентность dbt-версии и rendered-версии прототипа проверена построчно:
`a.equals(b) == True` на всех 96 476 × 24.

---

## 3. Решение: колонки `split` в витрине НЕТ, сплит в Python по хешу `order_id`

Рекомендация подтверждена, аргументы:

1. `sample_frac` и `test_frac` живут в `dg.Config` / `settings`. С колонкой
   `split` их изменение означало бы пересборку dbt-модели — три секунды
   и лишний ре-materialize 96 тысяч строк ради одного числа.
2. `bucket = md5(f"{order_id}|{salt}") % 10000` даёт **вложенные** выборки:
   `sample_frac=0.1` строго внутри `0.2`. Заказ всегда попадает в ту же часть
   при любом `sample_frac`, на любой машине, при любом порядке строк.
3. Встроенный `hash()` использовать НЕЛЬЗЯ: `PYTHONHASHSEED` рандомизируется
   на процесс, и holdout менялся бы при каждом запуске Dagster. Только `hashlib`.
4. Хеш-функции DuckDB и ClickHouse не совпадают (`hash()` vs `cityHash64`),
   поэтому «сплит в SQL» непереносим между таргетами проекта.
5. Соли разные для сэмплирования и сплита (`sample|{seed}`, `split|{seed}`),
   иначе holdout оказался бы вложен в сэмпл неслучайно.

Единственный минус — сплит не виден в SQL/BI. Компенсируется тем, что размеры
train/test и `dataset_fingerprint` уезжают в метаданные ассета.

---

## 4. Метрики прототипа (`prototype_train.py`, seed=42, holdout 20 %)

Модели: `LogisticRegression` в `Pipeline` (median-impute + StandardScaler +
OneHotEncoder(handle_unknown="ignore", min_frequency=20)) и
`HistGradientBoostingClassifier` (OrdinalEncoder + нативные категориальные,
`max_iter=100, max_leaf_nodes=15, min_samples_leaf=50, l2=1.0`).

| sample_frac | строк | train | holdout | pos % | модель | ROC AUC | PR AUC | accuracy | train ROC AUC | fit, с | perm, с |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.1 | 9 482 | 7 493 | 1 989 | 7.10 | logreg | 0.6671 | 0.1761 | 0.9281 | 0.7246 | **0.06** | 0.39 |
| 0.1 | 9 482 | 7 493 | 1 989 | 7.10 | **hgb** | **0.7408** | 0.2287 | 0.9246 | 0.9679 | **0.61** | 1.26 |
| 0.2 | 18 983 | 15 056 | 3 927 | 6.82 | logreg | 0.6752 | 0.1491 | 0.9297 | 0.7108 | **0.11** | 0.52 |
| 0.2 | 18 983 | 15 056 | 3 927 | 6.82 | **hgb** | **0.7822** | 0.2517 | 0.9300 | 0.9239 | **0.62** | 1.84 |
| 1.0 | 96 476 | 76 936 | 19 540 | 6.77 | logreg | 0.6911 | 0.1459 | 0.9328 | 0.7045 | **0.52** | 1.56 |
| 1.0 | 96 476 | 76 936 | 19 540 | 6.77 | **hgb** | **0.7836** | 0.2539 | 0.9332 | 0.8365 | **1.52** | 7.06 |

Прочие времена (одинаковы на всех долях): SQL витрины **0.14 с**,
генерация обоих PNG **0.16 с** (30–37 КБ каждый — спокойно влезает
в `MetadataValue.png`). Полный прогон скрипта целиком, включая старт `uv`:
4.8 с (0.1) / 5.6 с (0.2) / **13.5 с (1.0)** — прерывать не пришлось.

Дефолты `HistGradientBoostingClassifier` (`max_iter=200, max_leaf_nodes=31`)
на этих данных дают train ROC AUC = 1.0 при holdout 0.70 и обучаются в 3–4 раза
дольше. Ограничение листьев одновременно убирает переобучение и ускоряет fit.

### Стабильность по сидам (5 сидов: 1, 7, 17, 42, 2026)

| sample_frac | модель | ROC AUC медиана | min | max | σ |
|---|---|---|---|---|---|
| 0.1 | logreg | 0.6675 | 0.6319 | 0.7178 | 0.028 |
| 0.1 | hgb | 0.7392 | **0.6833** | 0.7408 | 0.022 |
| 0.2 | logreg | 0.6868 | 0.6642 | 0.7034 | 0.013 |
| 0.2 | hgb | 0.7725 | **0.7260** | 0.7822 | 0.022 |
| 1.0 | logreg | 0.6919 | 0.6898 | 0.7047 | 0.005 |
| 1.0 | hgb | 0.7836 | 0.7767 | 0.7853 | 0.003 |

Детерминизм проверен: два запуска с одним сидом дают **побитово равные**
ROC AUC и один и тот же `dataset_fingerprint`.

### Топ-признаков (permutation importance, HGB, sample_frac=1.0)

`purchase_month` 0.139 · `customer_state` 0.095 · `estimated_delivery_span_days` 0.091 ·
`customer_seller_distance_km` 0.025 · `main_product_category` 0.008 ·
`freight_value` 0.006 · `distinct_sellers_cnt` 0.005 · `main_payment_type` 0.002 ·
`max_item_weight_g` 0.002 · `items_cnt` 0.002.

---

## 5. Рекомендации для демо

**Модель — `HistGradientBoostingClassifier`.** ROC AUC 0.78 против 0.68
у логрегрессии, fit 0.6 с при `sample_frac=0.2`, категориальные — нативно,
NULL в `customer_seller_distance_km` обрабатывает сам. Логрегрессию оставить
в коде вторым вариантом: она нужна в кадре, чтобы quality gate можно было
уронить «по-честному», переключив модель конфигом, а не подкрутив порог.
Обе модели вместе укладываются в 0.7 с — можно обучать обе и показывать
две кривые на одном ROC-графике.

**`sample_frac` по умолчанию — 0.2, не 0.1.** На 0.1 худший сид даёт 0.683,
и gate 0.70 мигал бы; на 0.2 худший из пяти сидов — 0.726.

**Quality gate: `ML_MIN_ROC_AUC = 0.70`** (`@asset_check(blocking=True)`).
Медиана по сидам на дефолте 0.7725, худший сид 0.7260 → запас 0.026 от худшего
и 0.07 от медианы. Правило «медиана − 0.03» дало бы 0.7425 и падало бы
на сиде 17 — при разбросе 0.022 брать нужно от минимума, а не от медианы.
Как ломать gate в кадре: поднять порог до 0.85, либо `MODEL_KIND=logreg`,
либо `sample_frac=0.02`. Вторым, неблокирующим чеком — PR AUC ≥ 0.18
(при positive rate 6.8 % baseline PR AUC = 0.068, то есть модель в 3.7 раза
лучше случайной; именно эту цифру стоит назвать на лекции, а не accuracy).

**Accuracy в gate не брать.** 0.93 получает модель, предсказывающая «никогда
не опоздает»: positive rate предсказаний у логрегрессии — 0.15 % при истинных
7 %. Показывать её в таблице можно, но только рядом с positive rate.

**Что показывать в Dagster UI** (основной путь — метаданные, ноутбук не нужен):

- `MetadataValue.md` — таблица метрик (ROC AUC, PR AUC, accuracy, positive rate
  предсказанный/истинный, n_train, n_test, fit_sec) + строка про gate;
- `MetadataValue.png` — ROC-кривая обеих моделей (0.16 с на оба PNG, 32 КБ);
- `MetadataValue.png` — топ-10 permutation importance (на `sample_frac=0.2`
  это 1.8 с; на 1.0 — 7 с, поэтому `n_repeats` и размер выборки для
  permutation держать в settings, а на полном объёме считать только коэффициенты);
- `MetadataValue.url` — `mlflow_url` вида `{MLFLOW_UI_URL}/#/experiments/{exp}/runs/{run_id}`;
- `MetadataValue.int/float` — `rows_train`, `rows_test`, `roc_auc` (числовые
  метаданные Dagster рисует графиком по материализациям — бесплатный
  «дрейф метрики во времени»);
- `MetadataValue.text` — `dataset_fingerprint` (см. ниже).

**Детерминизм `training_dataset`:**
`sort_values(order_id, kind="mergesort")` → `bucket = int(md5(f"{order_id}|split|{seed}")[:8], 16) % 10000`
→ `test = bucket < test_frac*10000`. Ни `train_test_split`, ни `df.sample`,
ни встроенный `hash()`. `seed`, `sample_frac`, `test_frac` — из `settings`.

**Идемпотентная регистрация в MLflow — дёшево, делать:**
`dataset_fingerprint = md5(",".join(sorted(order_id)) + "|" + json(params))`.
Замер: **0.01 с на 96 476 id** — дешевле, чем читать витрину. Алгоритм:
`mlflow.set_tag("dataset_fingerprint", fp)` на каждом run'е (run'ы плодятся —
это нормально, они и есть история); перед `register_model` читать теги
последней версии зарегистрированной модели и, если fingerprint совпал,
пропускать регистрацию, записав в метаданные ассета
`registered=False, reason="fingerprint unchanged", version=<текущая>`.
Тогда повторный `full_pipeline_job` не плодит версии — это и есть
наглядная иллюстрация п. 5 «Ключевых требований».

---

## 6. Утечка признаков в существующих витринах

В самом `int_orders_enriched` опасны шесть колонок (`delivery_days`,
`delivery_delay_days`, `order_delivered_customer_date`, `review_score`,
`is_bad_review`/`is_good_review`, `has_review`) плюс `order_status`/`is_canceled`.

Отдельно: **ни одну из шести существующих `mart_*` нельзя join'ить к обучающей
выборке как источник признаков.** Все они содержат агрегаты, посчитанные
по всему периоду, включая holdout:

- `mart_delivery_quality_by_state`, `mart_delivery_quality_by_category`,
  `mart_seller_analytics`, `mart_daily_state_metrics`, `mart_hourly_order_pattern`
  — `late_delivery_rate`, `avg_delivery_days`, `p95_delivery_days`,
  `bad_review_rate`, `avg_review_score`, `cancel_rate`;
- `mart_customer_value_profile` — `late_orders_cnt`, `bad_reviews_cnt`,
  `canceled_orders_cnt`, `avg_review_score`, `recency_days`.

Это классический target encoding, посчитанный на будущем: например,
`mart_delivery_quality_by_state.late_delivery_rate` для штата включает те самые
заказы, которые лежат в holdout. Поэтому `mart_order_features` строится
**только** от `int_orders_enriched` + `stg_order_items` + `stg_products`,
и это стоит проговорить на лекции — вопрос «а почему не переиспользовали
готовую витрину» прозвучит обязательно.

---

## 7. Риски

1. **Дисбаланс 6.8 %.** Accuracy бессмысленна, `predict` при пороге 0.5 даёт
   positive rate 1.3 % (hgb) и 0.15 % (logreg). В демо показывать ROC AUC и
   PR AUC, а на слайде — «в 3.7 раза лучше случайного угадывания».
2. **`purchase_month` даёт почти половину качества.** Без него HGB падает
   с 0.782 до 0.687. Модель запоминает конкретные инциденты (ноябрь 2017,
   февраль–март 2018 — 14–19 % просрочек), а не сезонность вообще.
3. **Time-based holdout честнее и заметно хуже.** Если резать по дате
   (последние 20 %): HGB 0.7095, logreg 0.7096, PR AUC падает до 0.07, потому
   что positive rate в тесте 3.49 % против 7.59 % в train — датасет обрывается
   в октябре 2018, и у поздних заказов просто меньше шансов оказаться
   просроченными. Для демо сплит по хешу корректен и нагляднее, но в
   `decisions.md` это надо зафиксировать честно, иначе в аудитории MLE
   найдётся человек, который спросит.
4. **Разброс по сидам на малых долях** (σ ≈ 0.022 при `sample_frac` 0.1–0.2)
   больше, чем расстояние между «прошёл gate» и «не прошёл», если ставить порог
   от медианы. Отсюда порог от минимума — см. п. 5.
5. **Слабая предсказуемость в принципе.** 0.78 ROC AUC — это «модель что-то
   знает», а не рабочий сервис. На лекции это плюс: quality gate, дрейф и
   алерт на падение метрики выглядят осмысленно именно на такой задаче.
6. **Permutation importance на полном объёме — 7 с**, то есть дороже самого
   обучения. Держать `n_repeats` и размер выборки в settings.

---

## 8. Допущения

- Схемы DuckDB — `main_staging` / `main_intermediate` / `main_marts`
  (проверено по `information_schema`); в копии dbt-проекта внутри `02-dagster`
  они должны остаться такими же, иначе прототипу нужны флаги
  `--schema-staging` / `--schema-intermediate` (они предусмотрены).
- `test_frac` = 0.2 по всему отчёту; для gate брался holdout, не CV
  (CV на 96 тысячах строк — ещё 5 × fit, в бюджет демо не нужен).
- «Просрочка» = определение проекта: `late_delivery_grace_days = 0`,
  то есть любое превышение обещанной даты.
- Пороги 0.70 / 0.18 выведены из 5 сидов × 3 долей; при смене набора признаков
  или `late_delivery_grace_days` их надо пересчитать тем же скриптом.
- MLflow не проверялся в этом лейне (его нет в окружении) — рекомендация по
  идемпотентной регистрации опирается на замер стоимости fingerprint, а не на
  живой прогон `register_model`.
- Пакеты: `duckdb 1.5.5`, `pandas 3.0.6`, `scikit-learn 1.9.1`,
  `matplotlib 3.11.2`. `LogisticRegression(n_jobs=...)` в sklearn 1.8+
  устарел — в скрипте не используется.

---

## 9. Файлы лейна

| Файл | Что это |
|---|---|
| `mart_order_features.sql` | dbt-модель витрины признаков, стиль проекта, DuckDB + ClickHouse |
| `macro_month_of.sql` | недостающий макрос `month_of()` для `macros/cross_db/date_parts.sql` |
| `features_sql.py` | тот же SQL с прямыми именами таблиц — для прототипа и тестов |
| `prototype_train.py` | CLI-прототип: `--db --sample-frac --seed --test-frac --out-dir` |
| `artifacts/metrics_frac{0.1,0.2,1.0}_seed42.json` | полные метрики прогонов |
| `artifacts/roc_*.png`, `artifacts/importance_*.png` | PNG для `MetadataValue.png` |
| `REPORT.md` | этот отчёт |
