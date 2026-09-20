{#
    Перцентиль. Самое заметное расхождение диалектов:

        ClickHouse   quantile(0.95)(x)                          — параметрическая агрегатная функция
        DuckDB       quantile_cont(x, 0.95)                     — обычная агрегатная
        стандарт     percentile_cont(0.95) within group (...)   — оконный синтаксис

    dbt_utils такой макрос не предоставляет, поэтому пишем свой через
    adapter.dispatch. Второй аргумент dispatch — имя проекта из dbt_project.yml:
    именно в его пространстве имён dbt ищет реализации <adapter>__percentile.

    Использование:  {{ percentile('order_value', 0.95) }} as p95_order_value
#}

{% macro percentile(column, q=0.95) %}
    {{ return(adapter.dispatch('percentile', 'olist_dbt')(column, q)) }}
{% endmacro %}


{% macro default__percentile(column, q) %}
    percentile_cont({{ q }}) within group (order by {{ column }})
{% endmacro %}


{% macro clickhouse__percentile(column, q) %}
    quantile({{ q }})({{ column }})
{% endmacro %}


{% macro duckdb__percentile(column, q) %}
    quantile_cont({{ column }}, {{ q }})
{% endmacro %}
