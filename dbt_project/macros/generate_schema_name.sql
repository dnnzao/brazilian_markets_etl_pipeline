{% macro generate_schema_name(custom_schema_name, node) -%}
{#
    Override dbt's default schema generation.
    Uses the custom schema name directly instead of concatenating with target schema.

    This allows models to be placed exactly where we want them:
    - staging models -> staging schema
    - marts models -> analytics schema
#}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
