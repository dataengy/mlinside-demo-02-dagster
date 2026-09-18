{#
    ДОБАВИТЬ В КОНЕЦ macros/cross_db/date_parts.sql (копии dbt-проекта
    внутри 02-dagster). Рядом с hour_of и dow_of не хватает только месяца.

        ClickHouse   toMonth(ts)
        DuckDB       extract(month from ts)

    ClickHouse формально понимает и EXTRACT(MONTH FROM ...), но проект уже
    держит остальные части даты через dispatch — держим и эту, чтобы не
    заводить исключение из правила.
#}

{% macro month_of(ts) %}
    {{ return(adapter.dispatch('month_of', 'olist_dbt')(ts)) }}
{% endmacro %}

{% macro default__month_of(ts) %}
    extract(month from {{ ts }})
{% endmacro %}

{% macro clickhouse__month_of(ts) %}
    toMonth({{ ts }})
{% endmacro %}
