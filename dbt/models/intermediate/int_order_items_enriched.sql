{#
    Позиция заказа со свойствами товара, продавца и исходом заказа.
    РОВНО ОДНА СТРОКА НА ПОЗИЦИЮ — 112 650.

    Опорная модель для витрин, где единица наблюдения — товар или продавец:
    по категориям и по продавцам. Флаги исхода (просрочка, плохой отзыв)
    приезжают сюда с уровня заказа и потому повторяются на всех его позициях —
    это осознанно: вопрос «в каких категориях чаще опаздывают» задаётся
    именно про позиции.
#}

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

regions as (
    select * from {{ ref('seed_state_region') }}
)

select
    -- Явные алиасы для колонок, которые есть больше чем в одном соединяемом
    -- отношении: иначе ClickHouse сохранит квалифицированное имя (`i.order_id`)
    -- и колонка станет недоступна снаружи модели.
    i.order_id as order_id,
    i.order_item_id as order_item_id,

    i.price,
    -- freight_value есть и у позиции, и у заказа (там это сумма по заказу) —
    -- без алиаса ClickHouse оставит имя `i.freight_value`.
    i.freight_value as freight_value,
    i.item_total,
    i.freight_share,
    i.price_segment,

    p.product_id as product_id,
    p.product_category_name,
    p.product_category_name_english,
    p.product_weight_g,
    p.product_volume_cm3,

    s.seller_id as seller_id,
    s.seller_state,
    s.seller_city,
    sr.region as seller_region,

    o.order_purchase_date,
    o.order_hour,
    o.order_dow,
    o.customer_state,
    o.customer_region,
    o.order_status,

    o.is_delivered,
    o.is_canceled,
    o.is_late_delivery,
    o.delivery_days,
    o.review_score,
    o.is_bad_review,
    o.has_review,
    o.customer_seller_distance_km

from items as i
inner join orders as o on i.order_id = o.order_id
left join products as p on i.product_id = p.product_id
left join sellers as s on i.seller_id = s.seller_id
left join regions as sr on s.seller_state = sr.state_code
