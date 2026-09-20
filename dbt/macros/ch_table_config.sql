{#
    Настройки физического хранения таблицы для ClickHouse.

    ClickHouse требует указать движок и ключ сортировки; DuckDB таких понятий
    не знает и упадёт на незнакомых параметрах конфигурации. Макрос возвращает
    словарь параметров на ClickHouse и пустой словарь на всех остальных
    адаптерах, а модель распаковывает его в config().

    Использование в модели:

        {{ config(
            materialized='table',
            **ch_table_config(order_by='(order_purchase_date, customer_state)')
        ) }}

    Почему allow_nullable_key: часть витрин сортируется по колонкам, которые
    могут быть NULL (например, категория товара — у 610 товаров её нет).
    Без этой настройки ClickHouse откажется создавать таблицу.
#}

{% macro ch_table_config(order_by, partition_by=none) %}
    {% if target.type == 'clickhouse' %}
        {% set config_dict = {
            'engine': 'MergeTree()',
            'order_by': order_by,
            'settings': {'allow_nullable_key': 1}
        } %}
        {% if partition_by is not none %}
            {% do config_dict.update({'partition_by': partition_by}) %}
        {% endif %}
        {{ return(config_dict) }}
    {% endif %}
    {{ return({}) }}
{% endmacro %}
