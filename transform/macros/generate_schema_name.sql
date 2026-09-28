-- By default dbt prefixes custom schemas with the target schema ("main_marts").
-- Use the custom schema name as-is instead, so the lake has raw / staging / marts.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
