{#- Use the configured schema name exactly (staging / intermediate / gold) instead of
    dbt's default "<target schema>_<custom schema>" so lightweight mode and dbt agree. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
