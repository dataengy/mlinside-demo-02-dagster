{#
    Покупатели.

    Ключевая тонкость датасета: customer_id — это идентификатор СТРОКИ ЗАКАЗА,
    он уникален для каждого заказа (99 441 значение). Реального человека
    опознаёт customer_unique_id (96 096 значений). Профиль клиента, построенный
    по customer_id, покажет, что каждый клиент сделал ровно один заказ.
#}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

from {{ source('olist_raw', 'customers') }}
