{#
    Витрина 2. Качество исполнения по категориям товаров.
    Гранулярность: категория (английское название).

    Аналог mart_fraud_by_category: там искали категории с наибольшей долей
    фрода, здесь — категории, где чаще всего опаздывают и получают плохие оценки.

    Читает модель ПОЗИЦИЙ, а не заказов: вопрос задан про товары, и единицей
    наблюдения должна быть позиция. Число заказов при этом считается
    как count(distinct order_id) — один заказ может дать несколько позиций
    в одной категории.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(product_category_name_english)')
) }}

select
    product_category_name_english,
    min(product_category_name) as product_category_name_pt,

    count(*) as items_cnt,
    count(distinct order_id) as orders_cnt,
    count(distinct seller_id) as sellers_cnt,

    sum(item_total) as total_revenue,
    avg(price) as avg_price,
    {{ percentile('price', 0.95) }} as p95_price,
    avg(freight_share) as avg_freight_share,

    {{ count_if('is_delivered = 1') }} as delivered_cnt,
    {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate,
    avg(delivery_days) as avg_delivery_days,

    {{ pct(count_if('is_bad_review = 1'), count_if('has_review = 1')) }} as bad_review_rate,
    avg(review_score) as avg_review_score

from {{ ref('int_order_items_enriched') }}
group by product_category_name_english
