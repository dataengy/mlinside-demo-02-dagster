{#
    Позиции заказов. Гранулярность: одна строка на позицию,
    ключ (order_id, order_item_id). 112 650 строк против 99 441 заказа —
    именно здесь возникает разветвление, из-за которого метрики «по заказам»
    нельзя считать на этой таблице.
#}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,

    price,
    freight_value,
    price + freight_value as item_total,

    -- Доля доставки в стоимости позиции: дорогая доставка при дешёвом товаре —
    -- типичная причина плохого отзыва.
    {{ safe_div('freight_value', 'price + freight_value') }} as freight_share,

    {{ price_bucket('price') }} as price_segment

from {{ source('olist_raw', 'order_items') }}
