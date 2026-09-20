{#
    Подсчёт строк по условию.

        ClickHouse   countIf(condition)
        DuckDB       count(*) filter (where condition)
        стандарт     sum(case when condition then 1 else 0 end)

    Реализация по умолчанию нарочно переносимая: если проект однажды поедет
    на третий адаптер, он заработает без новой ветки.

    Использование:
        {{ count_if('is_late_delivery = 1') }} as late_orders_cnt
#}

{% macro count_if(condition) %}
    {{ return(adapter.dispatch('count_if', 'olist_dbt')(condition)) }}
{% endmacro %}


{% macro default__count_if(condition) %}
    sum(case when {{ condition }} then 1 else 0 end)
{% endmacro %}


{% macro clickhouse__count_if(condition) %}
    countIf({{ condition }})
{% endmacro %}


{% macro duckdb__count_if(condition) %}
    count(*) filter (where {{ condition }})
{% endmacro %}
