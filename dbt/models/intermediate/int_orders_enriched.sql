{#
    Заказ со всем, что к нему относится. РОВНО ОДНА СТРОКА НА ЗАКАЗ — 99 441.

    Это опорная модель для всех витрин, где считаются метрики «по заказам»:
    по дням и штатам, по клиентам, по времени суток. Считать их по позициям
    нельзя — заказы с несколькими позициями получили бы больший вес.

    Все соединения — LEFT: 775 заказов не имеют позиций, 768 — отзыва,
    один — платежа. INNER JOIN тихо выбросил бы их и занизил выручку.
    Сохранение числа строк проверяется тестом dbt_utils.equal_rowcount.
#}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

payments as (
    select * from {{ ref('stg_order_payments') }}
),

regions as (
    select * from {{ ref('seed_state_region') }}
),

-- Позиции сворачиваем до уровня заказа ДО соединения: иначе join размножит строки.
order_totals as (
    select
        order_id,
        count(*) as items_cnt,
        count(distinct product_id) as distinct_products_cnt,
        count(distinct seller_id) as distinct_sellers_cnt,
        sum(price) as items_value,
        sum(freight_value) as freight_value,
        sum(item_total) as order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

-- Расстояние считается в отдельной модели: вместе с ней запрос не помещался
-- в лимит парсера ClickHouse (см. комментарий в int_order_distance).
order_distance as (
    select * from {{ ref('int_order_distance') }}
)

select
    -- order_id и customer_id встречаются сразу в нескольких соединяемых
    -- отношениях, поэтому ClickHouse оставляет за ними квалифицированное имя
    -- (`o.order_id`), и снаружи модели колонка `order_id` просто не находится.
    -- Явный алиас снимает вопрос на обоих таргетах.
    o.order_id as order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_date,
    o.order_hour,
    o.order_dow,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    o.is_delivered,
    o.is_canceled,
    o.is_late_delivery,
    o.delivery_days,
    o.delivery_delay_days,

    c.customer_id as customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    r.region as customer_region,

    -- Заказы без позиций реальны, но их стоимость неизвестна: оставляем 0,
    -- чтобы суммы не становились NULL, а счётчик items_cnt показывал 0.
    coalesce(t.items_cnt, 0) as items_cnt,
    coalesce(t.distinct_products_cnt, 0) as distinct_products_cnt,
    coalesce(t.distinct_sellers_cnt, 0) as distinct_sellers_cnt,
    coalesce(t.items_value, 0) as items_value,
    coalesce(t.freight_value, 0) as freight_value,
    coalesce(t.order_value, 0) as order_value,

    {{ price_bucket('coalesce(t.order_value, 0)') }} as order_value_segment,
    case
        when coalesce(t.order_value, 0) >= {{ var('large_order_threshold') }} then 1
        else 0
    end as is_large_order,

    p.payment_total,
    p.main_payment_type,
    p.max_installments,

    -- NULL, если отзыва не было: это «не знаем», а не «оценка ноль».
    rv.review_score,
    rv.is_bad_review,
    rv.is_good_review,
    case when rv.order_id is not null then 1 else 0 end as has_review,

    d.customer_seller_distance_km

from orders as o
inner join customers as c on o.customer_id = c.customer_id
left join order_totals as t on o.order_id = t.order_id
left join payments as p on o.order_id = p.order_id
left join reviews as rv on o.order_id = rv.order_id
left join order_distance as d on o.order_id = d.order_id
left join regions as r on c.customer_state = r.state_code
