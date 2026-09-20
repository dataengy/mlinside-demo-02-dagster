-- Денежные величины не бывают отрицательными.
--
-- Проверяем не только цену позиции, но и агрегаты витрин: отрицательная
-- выручка означала бы ошибку в соединениях или в приведении типов
-- (например, строку, молча разобранную как число).

select
    'stg_order_items.price' as source_column,
    cast(price as {{ dbt.type_string() }}) as bad_value
from {{ ref('stg_order_items') }}
where price < 0

union all

select
    'stg_order_items.freight_value' as source_column,
    cast(freight_value as {{ dbt.type_string() }}) as bad_value
from {{ ref('stg_order_items') }}
where freight_value < 0

union all

select
    'int_orders_enriched.order_value' as source_column,
    cast(order_value as {{ dbt.type_string() }}) as bad_value
from {{ ref('int_orders_enriched') }}
where order_value < 0

union all

select
    'mart_daily_state_metrics.total_revenue' as source_column,
    cast(total_revenue as {{ dbt.type_string() }}) as bad_value
from {{ ref('mart_daily_state_metrics') }}
where total_revenue < 0

union all

select
    'mart_seller_analytics.total_revenue' as source_column,
    cast(total_revenue as {{ dbt.type_string() }}) as bad_value
from {{ ref('mart_seller_analytics') }}
where total_revenue < 0
