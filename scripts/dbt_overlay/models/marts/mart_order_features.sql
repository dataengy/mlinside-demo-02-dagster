{#
    Витрина признаков для ML-задачи «заказ будет доставлен позднее обещанного срока» (ADR-06, ADR-14).
    Отличие от канона (ADR-04): новая модель; кандидат на перенос в канон.

    РОВНО ОДНА СТРОКА НА ЗАКАЗ. Таргет is_late_delivery известен только у доставленных заказов;
    у остальных он NULL — «ещё не знаем», а не «вовремя». Витрина хранит и те и другие:
      training_dataset берёт строки с известным target, scoring_input — с NULL (docs/contracts/features.md).

    ПРАВИЛО ОТБОРА ПРИЗНАКОВ: только то, что известно в момент оформления заказа.
    Из int_orders_enriched намеренно НЕ берутся:
      delivery_days, delivery_delay_days, order_delivered_* — производные таргета;
      review_score, is_bad_review, is_good_review, has_review — отзыв появляется после доставки;
      order_status, is_canceled, is_delivered — финальный статус, не стартовый.
    Любая из них дала бы ROC AUC около 1.0 и бесполезную модель (утечка).
    order_estimated_delivery_date брать можно: обещанный срок известен при оформлении.

    Колонки split нет: деление train/holdout делает Python по хешу order_id.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(order_id)')
) }}

with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

item_products as (

    select
        i.order_id as order_id,
        i.price as price,
        p.product_category_name_english as product_category_name_english,
        p.product_weight_g as product_weight_g,
        p.product_volume_cm3 as product_volume_cm3
    from {{ ref('stg_order_items') }} as i
    left join {{ ref('stg_products') }} as p on i.product_id = p.product_id

),

order_dims as (

    select
        order_id,
        sum(coalesce(product_weight_g, 0)) as total_weight_g,
        sum(coalesce(product_volume_cm3, 0)) as total_volume_cm3,
        max(coalesce(product_weight_g, 0)) as max_item_weight_g
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

-- Доминирующая категория заказа; тай-брейк по имени обязателен — иначе витрина недетерминирована
-- и тест идемпотентности («собрать дважды — одно состояние») мигает.
category_ranked as (

    select
        order_id,
        product_category_name_english,
        row_number() over (
            partition by order_id
            order by category_value desc, product_category_name_english asc
        ) as category_rank
    from category_totals

),

main_category as (

    select order_id, product_category_name_english as main_product_category
    from category_ranked
    where category_rank = 1

)

select
    o.order_id as order_id,

    -- ТАРГЕТ: 0/1 у доставленных, NULL у остальных.
    o.is_late_delivery,

    -- Корзина.
    o.items_cnt,
    o.distinct_products_cnt,
    o.distinct_sellers_cnt,

    -- Деньги.
    o.items_value,
    o.freight_value,
    o.order_value,
    {{ safe_div('o.freight_value', 'o.order_value') }} as freight_share,
    o.is_large_order,

    -- Платёж (coalesce: у единичных заказов нет строки платежа).
    coalesce(o.main_payment_type, 'unknown') as main_payment_type,
    coalesce(o.max_installments, 0) as max_installments,

    -- География; distance оставлен с NULL — «координаты неизвестны», импьютер в Pipeline.
    o.customer_state,
    o.customer_region,
    o.customer_seller_distance_km,

    -- Время покупки и обещанный срок доставки (известен при оформлении — признак, не утечка).
    o.order_hour,
    o.order_dow,
    {{ month_of('o.order_purchase_timestamp') }} as purchase_month,
    {{ dbt.datediff('o.order_purchase_timestamp', 'o.order_estimated_delivery_date', 'day') }}
        as estimated_delivery_span_days,

    -- Товары.
    coalesce(mc.main_product_category, 'unknown') as main_product_category,
    coalesce(d.total_weight_g, 0) as total_weight_g,
    coalesce(d.total_volume_cm3, 0) as total_volume_cm3,
    coalesce(d.max_item_weight_g, 0) as max_item_weight_g,

    -- Не признак: для окон/партиций (appendix) и метаданных «на каком периоде обучались».
    o.order_purchase_timestamp

from orders as o
left join main_category as mc on o.order_id = mc.order_id
left join order_dims as d on o.order_id = d.order_id
