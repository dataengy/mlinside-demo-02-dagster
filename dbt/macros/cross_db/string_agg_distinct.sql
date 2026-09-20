{#
    Склейка уникальных значений группы в одну строку.

        ClickHouse   arrayStringConcat(arraySort(groupUniqArray(x)), ', ')
        DuckDB       string_agg(distinct x, ', ' order by x)

    Сортировка обязательна в обеих ветках: без неё порядок зависит от плана
    выполнения, и одна и та же витрина на двух таргетах (и даже на одном
    при повторной сборке) даст разные строки. Тогда любое сравнение
    результатов между таргетами становится бессмысленным.

    Использование:
        {{ string_agg_distinct('product_category_name_english') }} as top_categories
#}

{% macro string_agg_distinct(column, delimiter=', ') %}
    {{ return(adapter.dispatch('string_agg_distinct', 'olist_dbt')(column, delimiter)) }}
{% endmacro %}


{% macro default__string_agg_distinct(column, delimiter) %}
    string_agg(distinct {{ column }}, '{{ delimiter }}' order by {{ column }})
{% endmacro %}


{% macro clickhouse__string_agg_distinct(column, delimiter) %}
    arrayStringConcat(arraySort(groupUniqArray({{ column }})), '{{ delimiter }}')
{% endmacro %}
