{#
    Заказы: приведение типов, части даты и флаги исхода.

    Здесь же живёт фильтр по окну дат — его проставляет Dagster при запуске
    партиции (--vars '{"start_date": "...", "end_date": "..."}'). Пустые
    переменные означают «собрать всё», поэтому обычный `dbt build` работает
    как раньше.
#}

with source as (
    select * from {{ source('olist_raw', 'orders') }}

    {% if var('start_date') and var('end_date') %}
        where order_purchase_timestamp >= cast('{{ var("start_date") }}' as {{ dbt.type_timestamp() }})
          and order_purchase_timestamp <  cast('{{ var("end_date") }}' as {{ dbt.type_timestamp() }})
    {% endif %}
)

select
    order_id,
    customer_id,
    order_status,

    order_purchase_timestamp,
    {{ to_date_col('order_purchase_timestamp') }} as order_purchase_date,
    {{ hour_of('order_purchase_timestamp') }} as order_hour,
    {{ dow_of('order_purchase_timestamp') }} as order_dow,

    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    -- Доставленным считаем заказ по НАЛИЧИЮ ДАТЫ, а не по статусу: два заказа
    -- имеют status = 'delivered' без даты доставки (см. docs/DATASET.md).
    case when order_delivered_customer_date is not null then 1 else 0 end as is_delivered,

    case
        when order_status in ('canceled', 'unavailable') then 1 else 0
    end as is_canceled,

    -- Просрочка определена только для фактически доставленных заказов.
    -- Для остальных NULL, а не 0: «неизвестно» и «вовремя» — разные вещи,
    -- и NULL не даст витринам занизить долю просрочек.
    case
        when order_delivered_customer_date is null then null
        when
            {{ dbt.datediff('order_estimated_delivery_date', 'order_delivered_customer_date', 'day') }}
            > {{ var('late_delivery_grace_days') }}
            then 1
        else 0
    end as is_late_delivery,

    -- Сколько дней заказ шёл до покупателя и на сколько разошёлся с обещанием
    -- (отрицательное значение = приехал раньше срока).
    {{ dbt.datediff('order_purchase_timestamp', 'order_delivered_customer_date', 'day') }}
        as delivery_days,
    {{ dbt.datediff('order_estimated_delivery_date', 'order_delivered_customer_date', 'day') }}
        as delivery_delay_days

from source
