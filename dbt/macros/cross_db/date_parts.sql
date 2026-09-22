{#
    Извлечение частей даты и времени.

        ClickHouse   toDate(ts)      toHour(ts)             toDayOfWeek(ts)      toMonth(ts)
        DuckDB       cast(ts as date) extract(hour from ts)  isodow(ts)          extract(month from ts)

    День недели специально приведён к ISO в обеих ветках: понедельник = 1,
    воскресенье = 7. ClickHouse toDayOfWeek по умолчанию уже ISO, а вот
    DuckDB dayofweek() считает с воскресенья = 0 — поэтому берём isodow().
    Если этого не сделать, витрина mart_hourly_order_pattern на двух таргетах
    даст разные ответы на вопрос «в какой день недели больше просрочек».

    Отличие от канона (ADR-04): добавлен month_of() — нужен витрине mart_order_features
    (признак purchase_month). Кандидат на перенос в канон.
#}

{% macro to_date_col(ts) %}
    {{ return(adapter.dispatch('to_date_col', 'olist_dbt')(ts)) }}
{% endmacro %}

{% macro default__to_date_col(ts) %}
    cast({{ ts }} as date)
{% endmacro %}

{% macro clickhouse__to_date_col(ts) %}
    toDate({{ ts }})
{% endmacro %}


{% macro hour_of(ts) %}
    {{ return(adapter.dispatch('hour_of', 'olist_dbt')(ts)) }}
{% endmacro %}

{% macro default__hour_of(ts) %}
    extract(hour from {{ ts }})
{% endmacro %}

{% macro clickhouse__hour_of(ts) %}
    toHour({{ ts }})
{% endmacro %}


{#  День недели по ISO: понедельник = 1 … воскресенье = 7  #}
{% macro dow_of(ts) %}
    {{ return(adapter.dispatch('dow_of', 'olist_dbt')(ts)) }}
{% endmacro %}

{% macro default__dow_of(ts) %}
    isodow({{ ts }})
{% endmacro %}

{% macro clickhouse__dow_of(ts) %}
    toDayOfWeek({{ ts }})
{% endmacro %}


{% macro month_of(ts) %}
    {{ return(adapter.dispatch('month_of', 'olist_dbt')(ts)) }}
{% endmacro %}

{% macro default__month_of(ts) %}
    extract(month from {{ ts }})
{% endmacro %}

{% macro clickhouse__month_of(ts) %}
    toMonth({{ ts }})
{% endmacro %}
