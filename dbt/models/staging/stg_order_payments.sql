{#
    Платежи. Гранулярность сырых данных — (order_id, payment_sequential):
    один заказ можно оплатить несколькими способами (например, часть ваучером,
    часть картой). Схлопываем до одного заказа, чтобы модель можно было
    соединять с заказами без разветвления.
#}

with source as (
    select * from {{ source('olist_raw', 'order_payments') }}
)

select
    order_id,

    sum(payment_value) as payment_total,
    count(*) as payments_cnt,
    max(payment_installments) as max_installments,

    -- Основной способ оплаты — тот, на который пришлась наибольшая сумма.
    -- Считаем через argMax-подобный приём, переносимый между диалектами.
    min(
        case when payment_value = max_value then payment_type end
    ) as main_payment_type,

    {{ string_agg_distinct('payment_type') }} as payment_types

from (
    select
        *,
        max(payment_value) over (partition by order_id) as max_value
    from source
) as ranked
group by order_id
