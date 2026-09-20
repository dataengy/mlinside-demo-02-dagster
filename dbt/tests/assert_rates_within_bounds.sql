-- Любая доля в процентах обязана лежать в [0; 100] либо быть NULL.
--
-- Тест ловит классическую ошибку в знаменателе: если посчитать долю просрочек
-- как late_orders / delivered_orders, но в числитель по недосмотру попадут
-- и недоставленные заказы, значение вылезет за 100 %. Проверяем все витрины
-- сразу, чтобы правило нельзя было обойти в одной из них.

with all_rates as (

    select
        'mart_daily_state_metrics' as model_name,
        'late_delivery_rate' as metric_name,
        late_delivery_rate as metric_value
    from {{ ref('mart_daily_state_metrics') }}

    union all

    select
        'mart_daily_state_metrics' as model_name,
        'bad_review_rate' as metric_name,
        bad_review_rate as metric_value
    from {{ ref('mart_daily_state_metrics') }}

    union all

    select
        'mart_daily_state_metrics' as model_name,
        'large_order_share' as metric_name,
        large_order_share as metric_value
    from {{ ref('mart_daily_state_metrics') }}

    union all

    select
        'mart_delivery_quality_by_category' as model_name,
        'late_delivery_rate' as metric_name,
        late_delivery_rate as metric_value
    from {{ ref('mart_delivery_quality_by_category') }}

    union all

    select
        'mart_delivery_quality_by_category' as model_name,
        'bad_review_rate' as metric_name,
        bad_review_rate as metric_value
    from {{ ref('mart_delivery_quality_by_category') }}

    union all

    select
        'mart_delivery_quality_by_state' as model_name,
        'late_delivery_rate' as metric_name,
        late_delivery_rate as metric_value
    from {{ ref('mart_delivery_quality_by_state') }}

    union all

    select
        'mart_delivery_quality_by_state' as model_name,
        'freight_share_pct' as metric_name,
        freight_share_pct as metric_value
    from {{ ref('mart_delivery_quality_by_state') }}

    union all

    select
        'mart_hourly_order_pattern' as model_name,
        'late_delivery_rate' as metric_name,
        late_delivery_rate as metric_value
    from {{ ref('mart_hourly_order_pattern') }}

    union all

    select
        'mart_hourly_order_pattern' as model_name,
        'cancel_rate' as metric_name,
        cancel_rate as metric_value
    from {{ ref('mart_hourly_order_pattern') }}

    union all

    select
        'mart_seller_analytics' as model_name,
        'late_delivery_rate' as metric_name,
        late_delivery_rate as metric_value
    from {{ ref('mart_seller_analytics') }}

    union all

    select
        'mart_seller_analytics' as model_name,
        'bad_review_rate' as metric_name,
        bad_review_rate as metric_value
    from {{ ref('mart_seller_analytics') }}

)

select
    model_name,
    metric_name,
    metric_value
from all_rates
where metric_value < 0 or metric_value > 100
