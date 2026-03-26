{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

-- Staging model: parses GitHub /search/repositories response rows from RAW.LANDING.
-- One row per (load_id, repo_id). Kept as a view so every dbt run reflects
-- the latest landed data without materialization cost.

WITH raw_records AS (
    {{ parse_landing_source('github_trending_repos') }}
),

parsed AS (
    SELECT
        data:id::NUMBER                     AS repo_id,
        data:name::STRING                   AS repo_name,
        data:full_name::STRING              AS full_name,
        data:description::STRING            AS description,
        data:language::STRING               AS language,
        data:stargazers_count::NUMBER       AS star_count,
        data:forks_count::NUMBER            AS fork_count,
        data:watchers_count::NUMBER         AS watcher_count,
        data:open_issues_count::NUMBER      AS open_issue_count,
        data:size::NUMBER                   AS size_kb,
        data:default_branch::STRING         AS default_branch,
        data:topics::ARRAY                  AS topics,
        data:created_at::TIMESTAMP_TZ       AS repo_created_at,
        data:updated_at::TIMESTAMP_TZ       AS repo_updated_at,
        data:pushed_at::TIMESTAMP_TZ        AS repo_pushed_at,
        data:owner.login::STRING            AS owner_login,
        data:owner.id::NUMBER               AS owner_id,
        data:owner.type::STRING             AS owner_type,
        data:html_url::STRING               AS html_url,
        data:homepage::STRING               AS homepage_url,
        data:license.spdx_id::STRING        AS license_spdx,
        load_id,
        file_path,
        ingested_at
    FROM raw_records
)

SELECT * FROM parsed
