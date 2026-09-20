{#
    Витрина 3. Географический разрез качества доставки.
    Гранулярность: штат покупателя (27 строк).

    Аналог mart_fraud_by_state. Здесь же проверяется гипотеза, ради которой
    считался haversine_km: растёт ли просрочка с расстоянием до продавца.
    Логистика Бразилии сильно централизована вокруг Сан-Паулу, поэтому
    северные штаты ждут дольше — витрина это показывает численно.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(customer_state)')
) }}

select
    customer_state,
    min(customer_region) as customer_region,

    count(*) as orders_cnt,
    count(distinct customer_unique_id) as unique_customers,
    sum(distinct_sellers_cnt) as seller_links_cnt,

    sum(order_value) as total_revenue,
    avg(order_value) as avg_order_value,
    sum(freight_value) as total_freight,
    {{ pct('sum(freight_value)', 'sum(order_value)') }} as freight_share_pct,

    {{ count_if('is_delivered = 1') }} as delivered_cnt,
    {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate,
    avg(delivery_days) as avg_delivery_days,
    {{ percentile('delivery_days', 0.95) }} as p95_delivery_days,

    {{ pct(count_if('is_bad_review = 1'), count_if('has_review = 1')) }} as bad_review_rate,
    avg(review_score) as avg_review_score,

    avg(customer_seller_distance_km) as avg_distance_km

from {{ ref('int_orders_enriched') }}
group by customer_state
