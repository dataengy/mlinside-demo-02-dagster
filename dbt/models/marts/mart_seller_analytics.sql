{#
    Витрина 6. Аналитика по продавцам.
    Гранулярность: seller_id (3 095 строк).

    Аналог mart_merchant_analytics: там флаг is_suspicious ставился по доле
    фрода, здесь is_underperforming — по доле просрочек. Порог вынесен
    в переменную seller_late_rate_threshold.

    Флаг намеренно требует ещё и минимального числа заказов: продавец
    с одним заказом, который опоздал, даёт 100 % просрочек — это шум,
    а не сигнал, и без порога такие продавцы возглавили бы антирейтинг.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(seller_id)')
) }}

with seller_metrics as (
    select
        seller_id,
        min(seller_state) as seller_state,
        min(seller_city) as seller_city,
        min(seller_region) as seller_region,

        count(*) as items_sold,
        count(distinct order_id) as orders_cnt,
        count(distinct product_id) as unique_products,
        count(distinct product_category_name_english) as unique_categories,
        {{ string_agg_distinct('product_category_name_english') }} as categories,
        count(distinct customer_state) as states_served,

        sum(item_total) as total_revenue,
        avg(price) as avg_price,
        sum(freight_value) as total_freight,

        {{ count_if('is_delivered = 1') }} as delivered_cnt,
        {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate,
        avg(delivery_days) as avg_delivery_days,

        {{ count_if('has_review = 1') }} as reviewed_cnt,
        {{ pct(count_if('is_bad_review = 1'), count_if('has_review = 1')) }} as bad_review_rate,
        avg(review_score) as avg_review_score,

        avg(customer_seller_distance_km) as avg_distance_km

    from {{ ref('int_order_items_enriched') }}
    group by seller_id
)

select
    *,

    -- Проблемным считаем продавца, у которого просрочек больше порога
    -- И при этом достаточно заказов, чтобы доля что-то значила.
    case
        when
            orders_cnt >= 10
            and late_delivery_rate > {{ var('seller_late_rate_threshold') }}
            then 1
        else 0
    end as is_underperforming

from seller_metrics
