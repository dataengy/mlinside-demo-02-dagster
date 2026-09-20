{#
    Товары с приклеенным английским названием категории.

    У 610 товаров категория не заполнена. Их НЕ выбрасываем: заказы с этими
    товарами реальны, и молчаливая потеря 610 товаров исказила бы выручку.
    Вместо этого категория становится 'unknown' — видно и в витринах.

    Соединение с seed — left join по той же причине: если в справочнике
    не окажется какой-то категории, товар должен остаться в выборке.
#}

with products as (
    select * from {{ source('olist_raw', 'products') }}
),

translation as (
    select * from {{ ref('product_category_name_translation') }}
)

select
    p.product_id,

    coalesce(p.product_category_name, 'unknown') as product_category_name,

    -- nullif здесь не украшение, а обязательная защита от расхождения диалектов.
    -- При LEFT JOIN без совпадения ClickHouse подставляет в НЕ-Nullable колонку
    -- значение по умолчанию (пустую строку), а не NULL — настройка join_use_nulls
    -- по умолчанию выключена. DuckDB в том же месте даёт NULL.
    -- Без nullif те же 610 товаров получили бы 'unknown' на DuckDB и '' на
    -- ClickHouse, и витрины по категориям разошлись бы между таргетами.
    coalesce(nullif(t.product_category_name_english, ''), 'unknown')
        as product_category_name_english,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_photos_qty,

    -- Объём в кубических сантиметрах: крупногабаритный товар дольше едет
    -- и чаще опаздывает — пригодится при разборе просрочек.
    p.product_length_cm * p.product_height_cm * p.product_width_cm as product_volume_cm3

from products as p
left join translation as t
    on p.product_category_name = t.product_category_name
