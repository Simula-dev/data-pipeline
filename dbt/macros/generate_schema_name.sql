{#
  Override dbt's default schema naming.

  Default behavior:
      {{ target.schema }}_{{ custom_schema_name }}   e.g. RAW_STAGING

  Our override:
      {{ custom_schema_name }}                       e.g. STAGING

  This lets models in dbt_project.yml declare `+schema: staging` and land
  in a clean, top-level STAGING schema instead of RAW_STAGING.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
