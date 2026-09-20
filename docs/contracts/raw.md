# Контракт raw-слоя

> ADR-14. Контракт — то, на что опираются dbt (`sources.yml`) и, через витрину, ML-код. Смена загрузчика
> (seed → dlt → внешний warehouse) обязана сохранить всё перечисленное, тогда dbt/ML-код не меняется.
> Проверяется smoke-тестом `tests/smoke/test_raw_contract.py` (после M1): `sources.yml` ↔ `information_schema`
> DuckDB — таблицы, колонки, гранулярность, ключи.

## Общие правила

| Аспект | Значение |
|---|---|
| Ключ ассета Dagster | `raw/<table>` (группа `raw`) = `meta.dagster.asset_key: [raw, <table>]` в `sources.yml` |
| Адрес в хранилище | `olist.raw.<table>` (DuckDB `data/olist.duckdb`, схема `raw`; в prod — `<CH_RAW_DB>.<table>`) |
| Имена таблиц | как у dbt-source `olist_raw`: `orders`, `order_items`, `order_payments`, `order_reviews`, `customers`, `sellers`, `products`, `geolocation` (без префикса `olist_` и суффикса `_dataset` из CSV) |
| Типы | как при чтении CSV автотипизацией: timestamps, числа, текст (вывод `dbt seed`/agate ≈ `read_csv_auto` канона); staging-модели канона **не кастуют** и опираются на эти типы. Загрузчик, который даёт только `VARCHAR` (dlt `dtype=str`), обязан привести типы к тем же — это часть контракта |
| NULL / пустая строка | пустое поле CSV → `NULL` (не `''`); `NULL` в датах доставки означает «событие ещё не произошло», а не «неизвестно» |
| Кодировка | UTF-8; `product_category_name_translation.csv` содержит BOM — при загрузке использовать `utf-8-sig` |
| Служебные колонки загрузчика | `_dlt_*` и подобные в `sources.yml` не заносятся и контрактом не являются |
| Объём в демо | shipped snapshot ~5 000 заказов, связанных по `order_id` (все дочерние таблицы — только строки этих заказов; `customers`/`sellers`/`products` — только упомянутые; `geolocation` — префиксы клиентов и продавцов) |
| Идемпотентность загрузки | полная замена таблицы (`dbt seed` = create-or-replace; dlt `write_disposition="replace"`) |

## Таблицы

Колонки — заголовки CSV Olist (порядок сохраняется). «Гранулярность» и «ключ» — то, что проверяют dbt-тесты source.

### `orders` — заказ
Гранулярность: одна строка на заказ; ключ `order_id` (not_null, unique).

| Колонка | Смысл | Примечание |
|---|---|---|
| `order_id` | PK заказа | |
| `customer_id` | ссылка на `customers` | уникален **для заказа**, не для человека (человек — `customer_unique_id`) |
| `order_status` | статус на момент выгрузки | `delivered, shipped, canceled, unavailable, invoiced, processing, created, approved` |
| `order_purchase_timestamp` | момент оформления | всегда заполнен; `loaded_at_field` для source freshness |
| `order_approved_at` | подтверждение оплаты | может быть NULL |
| `order_delivered_carrier_date` | передача перевозчику | NULL, пока не произошло |
| `order_delivered_customer_date` | фактическая доставка | NULL у ~3 % заказов, в т.ч. у части `delivered` |
| `order_estimated_delivery_date` | обещанная дата доставки | известна в момент оформления → **разрешена как признак** |

### `order_items` — позиция заказа
Гранулярность: строка на позицию; ключ (`order_id`, `order_item_id`).

| Колонка | Смысл |
|---|---|
| `order_id`, `order_item_id` | ключ; `order_item_id` — порядковый номер с 1 |
| `product_id`, `seller_id` | ссылки на `products`, `sellers` |
| `shipping_limit_date` | срок передачи перевозчику |
| `price` | цена позиции без доставки (BRL) |
| `freight_value` | доставка, отнесённая на позицию |

### `order_payments` — платежи
Гранулярность: строка на платёж; ключ (`order_id`, `payment_sequential`).

| Колонка | Смысл |
|---|---|
| `order_id`, `payment_sequential` | ключ |
| `payment_type` | `credit_card, boleto, voucher, debit_card, not_defined` |
| `payment_installments` | число платежей в рассрочку |
| `payment_value` | сумма |

### `order_reviews` — отзывы
Гранулярность: строка на отзыв; **ключа нет** (`review_id` дублируется, у части заказов >1 отзыва) — дедупликация в `stg_order_reviews`.
Колонки: `review_id`, `order_id`, `review_score` (1–5), `review_comment_title`, `review_comment_message`,
`review_creation_date`, `review_answer_timestamp`. Всё содержимое появляется **после доставки** → в feature
contract не входит.

### `customers` — покупатель заказа
Гранулярность: строка на заказ; ключ `customer_id` (unique). Колонки: `customer_id`, `customer_unique_id`
(идентификатор человека), `customer_zip_code_prefix`, `customer_city`, `customer_state` (UF, двухбуквенный).

### `sellers` — продавцы
Ключ `seller_id` (unique). Колонки: `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state`.

### `products` — товары
Ключ `product_id` (unique). Колонки: `product_id`, `product_category_name` (португальский; NULL у части
товаров → в витринах `unknown`), `product_name_lenght`, `product_description_lenght`, `product_photos_qty`,
`product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`.

### `geolocation` — координаты по zip-префиксам
Гранулярность: много строк на префикс (в полном датасете ~53 точки на префикс); **ключа нет** — агрегируется в
`stg_geolocation` (среднее по префиксу). Колонки: `geolocation_zip_code_prefix`, `geolocation_lat`,
`geolocation_lng`, `geolocation_city`, `geolocation_state`. В snapshot ограничивается префиксами клиентов и
продавцов выборки (`GEO_MAX_ROWS`).

### Справочник `product_category_name_translation`
Не source, а обычный seed канонического проекта (`product_category_name` → `product_category_name_english`);
в контракт raw не входит, живёт в `dbt/seeds/`.

## Как проверять паритет при втором загрузчике (appendix)

1. Загрузить snapshot обоими способами в две схемы (`raw` и `raw_alt`).
2. Один и тот же smoke-тест контракта на обеих: набор таблиц и колонок из `sources.yml`, гранулярность и ключи
   (`count(*)` = `count(distinct key)`), `count(*)` по таблицам, `NULL`-семантика (`count(*) filter (where col = '')` = 0).
3. `dbt build --select +mart_order_features` на каждой схеме → одинаковые row counts витрины и хеш
   `md5(string_agg(order_id order by order_id))`.
