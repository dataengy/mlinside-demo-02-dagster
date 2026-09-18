"""Единый источник SQL витрины признаков mart_order_features.

Тот же самый запрос, что и в dbt-модели LANE/mart_order_features.sql, но
с прямыми именами таблиц вместо {{ ref() }} — чтобы прототип не требовал
запуска dbt. Макросы проекта раскрыты вручную:
  {{ safe_div(a, b) }}        -> case when (b) = 0 then null else (a)/(b) end
  {{ dbt.datediff(a, b, 'day') }} -> datediff('day', a, b)   (DuckDB)
"""

FEATURES_SQL = """
with orders as (
    select * from {sch_int}.int_orders_enriched
    where is_delivered = 1
      and is_late_delivery is not null
),

-- Доминирующая категория и габариты заказа считаются ДО соединения,
-- иначе join по позициям размножил бы строки заказа.
item_products as (
    select
        i.order_id            as order_id,
        p.product_category_name_english as product_category_name_english,
        i.price               as price,
        p.product_weight_g    as product_weight_g,
        p.product_volume_cm3   as product_volume_cm3
    from {sch_stg}.stg_order_items as i
    left join {sch_stg}.stg_products as p on i.product_id = p.product_id
),

order_dims as (
    select
        order_id,
        sum(coalesce(product_weight_g, 0))    as total_weight_g,
        sum(coalesce(product_volume_cm3, 0))  as total_volume_cm3,
        max(coalesce(product_weight_g, 0))    as max_item_weight_g
    from item_products
    group by order_id
),

category_totals as (
    select
        order_id,
        coalesce(product_category_name_english, 'unknown') as product_category_name_english,
        sum(price) as category_value
    from item_products
    group by order_id, coalesce(product_category_name_english, 'unknown')
),

-- Тай-брейк по имени категории обязателен: без него порядок строк
-- с одинаковой суммой не определён и витрина перестаёт быть детерминированной.
category_ranked as (
    select
        order_id,
        product_category_name_english,
        row_number() over (
            partition by order_id
            order by category_value desc, product_category_name_english asc
        ) as rn
    from category_totals
),

main_category as (
    select order_id, product_category_name_english as main_product_category
    from category_ranked
    where rn = 1
)

select
    o.order_id as order_id,

    cast(o.is_late_delivery as integer) as is_late_delivery,

    o.items_cnt,
    o.distinct_products_cnt,
    o.distinct_sellers_cnt,
    o.items_value,
    o.freight_value,
    o.order_value,
    case when (o.order_value) = 0 then null
         else (o.freight_value) / (o.order_value) end as freight_share,
    o.is_large_order,

    coalesce(o.main_payment_type, 'unknown') as main_payment_type,
    coalesce(o.max_installments, 0) as max_installments,

    o.customer_state,
    o.customer_region,
    o.customer_seller_distance_km,

    o.order_hour,
    o.order_dow,
    extract(month from o.order_purchase_timestamp) as purchase_month,

    datediff('day', o.order_purchase_timestamp, o.order_estimated_delivery_date)
        as estimated_delivery_span_days,

    coalesce(mc.main_product_category, 'unknown') as main_product_category,
    coalesce(d.total_weight_g, 0) as total_weight_g,
    coalesce(d.total_volume_cm3, 0) as total_volume_cm3,
    coalesce(d.max_item_weight_g, 0) as max_item_weight_g,

    o.order_purchase_timestamp

from orders as o
left join main_category as mc on o.order_id = mc.order_id
left join order_dims  as d  on o.order_id = d.order_id
"""


def build_sql(sch_stg="main_staging", sch_int="main_intermediate"):
    return FEATURES_SQL.format(sch_stg=sch_stg, sch_int=sch_int)
