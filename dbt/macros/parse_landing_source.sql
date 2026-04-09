{#
  Reusable macro: pull all rows for a single logical source from raw.landing.

  Usage:
      WITH raw_data AS ( {{ parse_landing_source('github_trending_repos') }} )
      SELECT (data->>'id')::bigint AS id, ... FROM raw_data
#}

{% macro parse_landing_source(source_name) -%}
    SELECT
        load_id,
        file_path,
        ingested_at,
        data
    FROM {{ source('raw', 'landing') }}
    WHERE source = '{{ source_name }}'
{%- endmacro %}
