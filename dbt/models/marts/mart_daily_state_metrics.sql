{#
    Витрина 1. Дневные метрики по штатам.
    Гранулярность: (дата покупки, штат покупателя).

    Прямой аналог mart_daily_state_metrics из ДЗ по фрод-транзакциям:
    те же счётчик, сумма, средний чек, P95 и доля крупных заказов —
    плюс качество доставки вместо доли фрода.
#}

{{ config(
    materialized='table',
    **ch_table_config(
        order_by='(order_purchase_date, customer_state)',
        partition_by='toYYYYMM(order_purchase_date)'
    )
) }}

select
    order_purchase_date,
    customer_state,
    min(customer_region) as customer_region,

    count(*) as orders_cnt,
    count(distinct customer_unique_id) as unique_customers,

    sum(order_value) as total_revenue,
    avg(order_value) as avg_order_value,
    {{ percentile('order_value', 0.95) }} as p95_order_value,
    {{ pct(count_if('is_large_order = 1'), 'count(*)') }} as large_order_share,

    -- Знаменатель — только доставленные заказы: у 3 % даты доставки нет,
    -- и делить на общее число значило бы занизить долю просрочек.
    {{ count_if('is_delivered = 1') }} as delivered_cnt,
    {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate,

    -- Знаменатель — только заказы с отзывом (у 768 его нет).
    {{ pct(count_if('is_bad_review = 1'), count_if('has_review = 1')) }} as bad_review_rate,

    avg(delivery_days) as avg_delivery_days

from {{ ref('int_orders_enriched') }}
group by order_purchase_date, customer_state
