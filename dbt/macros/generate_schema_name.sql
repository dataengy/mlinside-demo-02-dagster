{#
    Отличие от канона (ADR-04): схемы без префикса target.schema — `raw`, `staging`, `intermediate`,
    `marts`, `seeds`, а не `main_staging` и т.п. В демо имена схем видны в UI и в контракте raw
    (docs/contracts/raw.md: olist.raw.<table>); префикс только мешал бы читать.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
