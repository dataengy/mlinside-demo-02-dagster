-- Выручка витрины по дням и штатам обязана сходиться с суммой по позициям.
--
-- Самый ценный тест набора: он ловит разветвление (fan-out). Если соединение
-- заказов с платежами или отзывами размножит строки, выручка витрины вырастет
-- в разы — и это не заметит ни один тест на not_null или диапазон.
--
-- Допуск 0,01 % оставлен на накопление ошибки округления при суммировании
-- сотен тысяч значений с плавающей точкой, а не на «примерно сошлось».

with mart_total as (
    select sum(total_revenue) as revenue
    from {{ ref('mart_daily_state_metrics') }}
),

source_total as (
    select sum(price + freight_value) as revenue
    from {{ ref('stg_order_items') }}
)

select
    m.revenue as mart_revenue,
    s.revenue as source_revenue,
    m.revenue - s.revenue as difference
from mart_total as m
cross join source_total as s
where abs(m.revenue - s.revenue) > s.revenue * 0.0001
