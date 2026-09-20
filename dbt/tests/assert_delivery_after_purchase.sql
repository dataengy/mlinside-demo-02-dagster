-- Заказ не может быть доставлен раньше, чем оформлен.
--
-- Проверка направления времени. Ловит две реальные ошибки: перепутанные
-- местами аргументы datediff (что дало бы отрицательные сроки доставки)
-- и неверный разбор дат при загрузке, когда день и месяц меняются местами.

select
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    delivery_days
from {{ ref('int_orders_enriched') }}
where
    order_delivered_customer_date is not null
    and (
        order_delivered_customer_date < order_purchase_timestamp
        or delivery_days < 0
    )
