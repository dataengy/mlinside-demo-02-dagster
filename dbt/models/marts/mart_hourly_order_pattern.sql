{#
    Витрина 5. Временные паттерны заказов.
    Гранулярность: (день недели, час) — максимум 7 × 24 = 168 строк.

    Аналог mart_hourly_fraud_pattern: там искали временные окна с повышенным
    риском фрода, здесь — окна с повышенной долей просрочек и плохих отзывов.

    День недели приходит из макроса dow_of, приведённого к ISO на обоих
    таргетах (понедельник = 1). Без этого приведения ClickHouse и DuckDB
    ответили бы на вопрос «какой день недели проблемный» по-разному.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(order_dow, order_hour)')
) }}

select
    order_dow,
    order_hour,

    case order_dow
        when 1 then 'Понедельник'
        when 2 then 'Вторник'
        when 3 then 'Среда'
        when 4 then 'Четверг'
        when 5 then 'Пятница'
        when 6 then 'Суббота'
        when 7 then 'Воскресенье'
    end as dow_name,

    case when order_dow >= 6 then 1 else 0 end as is_weekend,

    count(*) as orders_cnt,
    count(distinct customer_unique_id) as unique_customers,

    sum(order_value) as total_revenue,
    avg(order_value) as avg_order_value,
    {{ pct(count_if('is_large_order = 1'), 'count(*)') }} as large_order_share,

    {{ count_if('is_delivered = 1') }} as delivered_cnt,
    {{ pct(count_if('is_late_delivery = 1'), count_if('is_delivered = 1')) }} as late_delivery_rate,
    {{ pct(count_if('is_bad_review = 1'), count_if('has_review = 1')) }} as bad_review_rate,
    {{ pct(count_if('is_canceled = 1'), 'count(*)') }} as cancel_rate

from {{ ref('int_orders_enriched') }}
group by order_dow, order_hour
