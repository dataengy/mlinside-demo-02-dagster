{#
    Расстояние от покупателя до продавца по каждому заказу, км.

    Вынесено в отдельную модель не только ради читаемости. Макрос haversine_km
    разворачивается в громоздкое выражение, и вместе с шестью соединениями
    int_orders_enriched запрос перестаёт помещаться в лимит парсера ClickHouse
    (Code 718: Maximum amount of backtracking exceeded). Отдельная модель
    разрывает запрос на два — оба разбираются без проблем и на ClickHouse,
    и на DuckDB.

    Берём ближайшего (min) продавца заказа: у подавляющего большинства заказов
    продавец единственный, а усреднение по нескольким было бы уже другой метрикой.
#}

with items as (
    select
        order_id,
        seller_id
    from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
),

customers as (
    select
        customer_id,
        customer_zip_code_prefix
    from {{ ref('stg_customers') }}
),

sellers as (
    select
        seller_id,
        seller_zip_code_prefix
    from {{ ref('stg_sellers') }}
),

geo as (
    select
        zip_code_prefix,
        latitude,
        longitude
    from {{ ref('stg_geolocation') }}
),

paired as (
    select
        i.order_id as order_id,
        cg.latitude as customer_lat,
        cg.longitude as customer_lon,
        sg.latitude as seller_lat,
        sg.longitude as seller_lon
    from items as i
    inner join orders as o on i.order_id = o.order_id
    inner join customers as c on o.customer_id = c.customer_id
    inner join sellers as s on i.seller_id = s.seller_id
    left join geo as cg on c.customer_zip_code_prefix = cg.zip_code_prefix
    left join geo as sg on s.seller_zip_code_prefix = sg.zip_code_prefix
),

-- Перевод в радианы отдельным шагом: так формула гаверсинуса остаётся плоской
-- и укладывается в ограничения разборщиков обоих движков (см. haversine_km).
in_radians as (
    select
        order_id,
        radians(customer_lat) as customer_lat_rad,
        radians(customer_lon) as customer_lon_rad,
        radians(seller_lat) as seller_lat_rad,
        radians(seller_lon) as seller_lon_rad
    from paired
)

select
    order_id,
    min({{ haversine_km(
        'customer_lat_rad', 'customer_lon_rad', 'seller_lat_rad', 'seller_lon_rad'
    ) }}) as customer_seller_distance_km
from in_radians
group by order_id
