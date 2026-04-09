{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

-- Staging model: parses GitHub /search/repositories response rows from raw.landing.
-- PostgreSQL jsonb uses ->> for text extraction, -> for nested objects.

WITH raw_records AS (
    {{ parse_landing_source('github_trending_repos') }}
),

parsed AS (
    SELECT
        (data->>'id')::bigint                    AS repo_id,
        data->>'name'                            AS repo_name,
        data->>'full_name'                       AS full_name,
        data->>'description'                     AS description,
        data->>'language'                        AS language,
        (data->>'stargazers_count')::integer     AS star_count,
        (data->>'forks_count')::integer          AS fork_count,
        (data->>'watchers_count')::integer       AS watcher_count,
        (data->>'open_issues_count')::integer    AS open_issue_count,
        (data->>'size')::integer                 AS size_kb,
        data->>'default_branch'                  AS default_branch,
        data->'topics'                           AS topics,
        (data->>'created_at')::timestamptz       AS repo_created_at,
        (data->>'updated_at')::timestamptz       AS repo_updated_at,
        (data->>'pushed_at')::timestamptz        AS repo_pushed_at,
        data->'owner'->>'login'                  AS owner_login,
        (data->'owner'->>'id')::bigint           AS owner_id,
        data->'owner'->>'type'                   AS owner_type,
        data->>'html_url'                        AS html_url,
        data->>'homepage'                        AS homepage_url,
        data->'license'->>'spdx_id'              AS license_spdx,
        load_id,
        file_path,
        ingested_at
    FROM raw_records
)

SELECT * FROM parsed
