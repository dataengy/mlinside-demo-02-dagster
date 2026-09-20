# Feature contract: витрина, обучающая выборка, scoring input, predictions

> ADR-14, ADR-06, ADR-13. Четыре разных объекта — четыре ассета в графе. Список признаков ниже — **кандидаты**;
> итоговые 8–10 утверждаются на шаге M3 после измерения на shipped snapshot.

## Формулировка задачи

«На основании информации, доступной при оформлении заказа, оценить риск, что заказ будет доставлен позднее
обещанного срока». Бинарная классификация; target `is_late_delivery = order_delivered_customer_date >
order_estimated_delivery_date` известен только после доставки.

## Правило отбора признаков

В контракт входит только то, что известно **в момент оформления заказа** (когда реальный scoring и происходил бы).
Всё, что появляется позже, — утечка будущего, даже если даёт «отличную» метрику:

| Запрещено | Почему |
|---|---|
| `delivery_days`, `delivery_delay_days`, `order_delivered_customer_date`, `order_delivered_carrier_date` | из них считается target; `delivery_delay_days` даёт почти идеальный AUC — это не модель, а утечка |
| `review_score`, `is_bad_review`, `is_good_review`, `has_review`, любые поля `order_reviews` | отзыв появляется после доставки |
| `order_status`, `is_canceled`, `is_delivered` | финальный статус, не стартовый |
| агрегаты `mart_*` канона (профили клиентов, продавцов, категорий за весь период) | считаются по всему периоду, включая будущее относительно заказа (target encoding из будущего) |

`order_estimated_delivery_date` — **разрешён**: обещанный срок известен при оформлении.

## 1. `mart_order_features` (dbt, схема `marts`)

Одна строка на заказ. Только `ref()` на `int_orders_enriched`, `stg_order_items`, `stg_products`
(+ `stg_order_payments` через intermediate). Target `is_late_delivery` — `NULL`, пока заказ не доставлен
(не 0!). Владелец — DE (owner в yml), тесты: `unique(order_id)`, `not_null(order_id)`,
`accepted_values(is_late_delivery: [0, 1])` для доставленных.

| Группа | Колонки-кандидаты (тип) | Источник |
|---|---|---|
| ключ | `order_id` (varchar) | `int_orders_enriched` |
| корзина | `items_cnt`, `distinct_sellers_cnt` (int), `items_value`, `freight_value`, `order_value`, `freight_share` (double) | `int_orders_enriched`, `stg_order_items` |
| оплата | `main_payment_type` (varchar), `max_installments` (int) | `stg_order_payments` |
| география | `customer_state` (varchar UF), `customer_region` (varchar), `customer_seller_distance_km` (double, NULL если нет координат) | `int_orders_enriched` / `int_order_distance` |
| время | `order_hour`, `order_dow`, `purchase_month` (int) | `stg_orders` |
| обещание | `estimated_delivery_span_days` (int) = `order_estimated_delivery_date − order_purchase_date` | `stg_orders` |
| товар | `main_product_category` (varchar, `unknown` если пусто), `total_weight_g`, `total_volume_cm3` (double) | `stg_products` |
| target | `is_late_delivery` (int 0/1 или NULL) | `stg_orders` |
| техн. | `order_purchase_timestamp` (timestamp) — для окон/партиций в appendix, **не признак** | `stg_orders` |

Оговорка про `purchase_month`: в полном датасете он даёт заметную долю качества («память об инцидентах»
2017-11, 2018-03); если признак войдёт в список — проговаривается в кадре.

## 2. `training_dataset` (Python, `data/ml/`)

Исторические строки витрины с **известным** target (`is_late_delivery IS NOT NULL`). Детерминированный сплит по
`md5(order_id|split|RANDOM_STATE)` на train / holdout (`ML_TEST_FRAC`). Ассет **сохраняет оба snapshot'а**
(`train_<fingerprint>.parquet`, `holdout_<fingerprint>.parquet`) и метаданные `rows_train`, `rows_holdout`,
`positive_rate`, `dataset_fingerprint`. `model` учится только на train (preprocessing `fit` внутри Pipeline на
train); `model_evaluation` читает holdout-snapshot того же fingerprint — витрину не перечитывает. Random split —
учебное упрощение; temporal split — appendix (overview §6.2).

## 3. `scoring_input` (Python или dbt-модель `marts.scoring_input`)

Строки витрины, для которых target **ещё неизвестен** (`is_late_delivery IS NULL` — заказы, не доставленные на
момент snapshot) плюс, для наглядности объёма в демо, фиксированный batch по `batch_id`. Те же признаки, тот же
порядок колонок, что у train; target отсутствует. Не «уже доставленные заказы» — иначе scoring не production-like.

## 4. `predictions` (DuckDB `ml.predictions`)

Результат применения **опубликованной** модели (`models:/<MLFLOW_MODEL_NAME>@champion`, версия разрешается
один раз в начале run).

| Колонка | Тип | Смысл |
|---|---|---|
| `order_id` | varchar | ключ вместе с `batch_id` |
| `score` | double | вероятность класса 1 |
| `predicted_class` | int | `score >= threshold` (порог — из settings, не из модели) |
| `model_version` | int | версия MLflow, которой сделан расчёт |
| `batch_id` | varchar | идентификатор batch (в демо — фиксированная строка из settings) |
| `scored_at` | timestamp | момент расчёта |

Идемпотентность: повторный запуск того же `batch_id` — `delete where batch_id = ?` + `insert`, дублей нет.
Метаданные ассета: `rows`, `mean_score`, `predicted_positive_rate`, `model_version`, `batch_id` — это
**не** доказательство drift или качества. Без `champion` ассет завершается понятной ошибкой «нет опубликованной
модели — выполните promote». Обучение из `predictions` никогда не запускается.

## Общий контракт train ↔ inference

Прямой runtime-зависимости нет; модель передаётся через alias `champion`. Общими остаются: список и порядок
признаков (этот документ), preprocessing внутри Pipeline, семантика `NULL`. Изменение списка признаков =
изменение контракта → новая версия витрины, переобучение и promote; старые версии в реестре остаются.
