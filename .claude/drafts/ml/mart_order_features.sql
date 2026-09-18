{#
    Витрина признаков для ML-задачи «заказ приедет с опозданием».
    РОВНО ОДНА СТРОКА НА ДОСТАВЛЕННЫЙ ЗАКАЗ — 96 476.

    Обучающая выборка ограничена доставленными заказами (is_delivered = 1):
    для недоставленных таргет is_late_delivery равен NULL — это «ещё не знаем»,
    а не «вовремя». Включив их, мы бы обучали модель на 3 % строк с выдуманным
    нулём (см. комментарий про NULL в stg_orders).

    ПРАВИЛО ОТБОРА ПРИЗНАКОВ: здесь только то, что известно в МОМЕНТ ПОКУПКИ.
    Из int_orders_enriched намеренно НЕ берутся:
      delivery_days, delivery_delay_days   — прямые производные таргета;
      order_delivered_customer_date        — из неё таргет и считается;
      review_score, is_bad_review,
      is_good_review, has_review           — отзыв появляется ПОСЛЕ доставки;
      order_status, is_canceled            — финальный статус, не стартовый.
    Любая из них дала бы ROC AUC около 1.0 и бесполезную модель.

    order_estimated_delivery_date брать МОЖНО и нужно: обещанный срок известен
    в момент оформления, и estimated_delivery_span_days — третий по силе признак.

    Витрина не содержит колонки split: деление на train/holdout делает Python
    по хешу order_id (см. defs/ml/assets.py). Так доля holdout и sample_frac
    меняются конфигом Dagster, не пересборкой 96 тысяч строк, и один и тот же
    order_id всегда попадает в ту же часть — на любом адаптере и при любом
    sample_frac. Хеш-функции DuckDB и ClickHouse между собой не совпадают,
    поэтому «сплит в SQL» был бы непереносим.
#}

{{ config(
    materialized='table',
    **ch_table_config(order_by='(order_id)')
) }}

with orders as (

    select * from {{ ref('int_orders_enriched') }}
    where is_delivered = 1
      and is_late_delivery is not null

),

-- Свойства товаров приклеиваем к позициям, а свёртку до заказа делаем ниже:
-- join по позициям напрямую к заказу размножил бы строки.
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

-- Доминирующая категория заказа = категория с наибольшей суммой цен позиций.
-- Тай-брейк по имени категории обязателен: без него две категории с равной
-- суммой дают неопределённый порядок, витрина перестаёт быть детерминированной,
-- и тест идемпотентности («собрать дважды — одно состояние») начинает мигать.
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
    -- Ключ строки и он же основа детерминированного сплита в Python.
    o.order_id as order_id,

    -- ТАРГЕТ.
    o.is_late_delivery,

    -- Состав корзины.
    o.items_cnt,
    o.distinct_products_cnt,
    o.distinct_sellers_cnt,

    -- Деньги.
    o.items_value,
    o.freight_value,
    o.order_value,
    {{ safe_div('o.freight_value', 'o.order_value') }} as freight_share,
    o.is_large_order,

    -- Платёж. coalesce — у одного заказа во всём датасете нет строки платежа,
    -- и NULL в категориальном признаке пришлось бы лечить уже в Python.
    coalesce(o.main_payment_type, 'unknown') as main_payment_type,
    coalesce(o.max_installments, 0) as max_installments,

    -- География. customer_seller_distance_km оставлен с NULL (477 заказов):
    -- это честное «координаты продавца неизвестны», и импьютер в Pipeline
    -- заполняет его медианой обучающей выборки, а не всей витрины.
    o.customer_state,
    o.customer_region,
    o.customer_seller_distance_km,

    -- Время покупки. Обещанный срок доставки известен при оформлении,
    -- поэтому это признак, а не утечка.
    o.order_hour,
    o.order_dow,
    {{ month_of('o.order_purchase_timestamp') }} as purchase_month,
    {{ dbt.datediff('o.order_purchase_timestamp', 'o.order_estimated_delivery_date', 'day') }}
        as estimated_delivery_span_days,

    -- Товары: что везём и сколько оно весит.
    coalesce(mc.main_product_category, 'unknown') as main_product_category,
    coalesce(d.total_weight_g, 0) as total_weight_g,
    coalesce(d.total_volume_cm3, 0) as total_volume_cm3,
    coalesce(d.max_item_weight_g, 0) as max_item_weight_g,

    -- Не признак: нужен для time-based среза в анализе и для метаданных
    -- ассета («на каком периоде обучались»).
    o.order_purchase_timestamp

from orders as o
left join main_category as mc on o.order_id = mc.order_id
left join order_dims as d on o.order_id = d.order_id
