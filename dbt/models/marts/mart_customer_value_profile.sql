{#
    Витрина 4. Профиль ценности клиента.
    Гранулярность: customer_unique_id (96 096 строк).

    Аналог mart_customer_risk_profile: та же идея сегментации CASE-выражением,
    но у Olist нет фрода, поэтому сегментируем по ценности, а не по риску.

    ГЛАВНОЕ: группировка по customer_unique_id, а НЕ по customer_id.
    customer_id уникален для каждого заказа, и группировка по нему выдала бы
    99 441 «клиента», каждый ровно с одним заказом (см. docs/DATASET.md).

    Границы сегментов вынесены в переменные проекта: их естественно
    захочется покрутить, не трогая SQL.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(customer_unique_id)')
) }}

with customer_orders as (
    select
        customer_unique_id,
        min(customer_state) as customer_state,
        min(customer_region) as customer_region,

        count(*) as orders_cnt,
        sum(order_value) as total_spend,
        avg(order_value) as avg_order_value,
        max(order_value) as max_order_value,

        min(order_purchase_date) as first_order_date,
        max(order_purchase_date) as last_order_date,

        {{ count_if('is_late_delivery = 1') }} as late_orders_cnt,
        {{ count_if('is_bad_review = 1') }} as bad_reviews_cnt,
        {{ count_if('is_canceled = 1') }} as canceled_orders_cnt,
        {{ count_if('has_review = 1') }} as reviewed_orders_cnt,
        avg(review_score) as avg_review_score

    from {{ ref('int_orders_enriched') }}
    group by customer_unique_id
),

-- Давность считаем от последнего заказа в датасете, а не от сегодняшней даты:
-- данные статические (последний заказ 2018-10-17), и привязка к now() делала бы
-- витрину невоспроизводимой — при каждой сборке получались бы разные числа.
dataset_bounds as (
    select max(last_order_date) as dataset_last_date
    from customer_orders
)

select
    c.customer_unique_id,
    c.customer_state,
    c.customer_region,

    c.orders_cnt,
    c.total_spend,
    c.avg_order_value,
    c.max_order_value,

    c.first_order_date,
    c.last_order_date,
    {{ dbt.datediff('c.last_order_date', 'b.dataset_last_date', 'day') }} as recency_days,

    c.late_orders_cnt,
    c.bad_reviews_cnt,
    c.canceled_orders_cnt,
    c.reviewed_orders_cnt,
    c.avg_review_score,

    case when c.orders_cnt > 1 then 1 else 0 end as is_repeat_customer,

    case
        when c.total_spend >= {{ var('customer_high_value_threshold') }} then 'HIGH'
        when c.total_spend >= {{ var('customer_medium_value_threshold') }} then 'MEDIUM'
        else 'LOW'
    end as value_segment

from customer_orders as c
cross join dataset_bounds as b
