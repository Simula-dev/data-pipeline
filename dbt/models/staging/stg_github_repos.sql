{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

-- Staging model: parses GitHub /search/repositories response rows from raw.landing.
-- One row per (load_id, repo_id). Kept as a view so every dbt run reflects
-- the latest landed data without materialization cost.
--
-- Redshift SUPER type uses dot notation (`data.field`) instead of Snowflake's
-- colon notation (`data:field`).

WITH raw_records AS (
    {{ parse_landing_source('github_trending_repos') }}
),

parsed AS (
    SELECT
        data.id::bigint                   AS repo_id,
        data.name::varchar(500)           AS repo_name,
        data.full_name::varchar(500)      AS full_name,
        data.description::varchar(65535)  AS description,
        data.language::varchar(100)       AS language,
        data.stargazers_count::integer    AS star_count,
        data.forks_count::integer         AS fork_count,
        data.watchers_count::integer      AS watcher_count,
        data.open_issues_count::integer   AS open_issue_count,
        data.size::integer                AS size_kb,
        data.default_branch::varchar(255) AS default_branch,
        data.topics                       AS topics,  -- leave as SUPER array
        data.created_at::timestamptz      AS repo_created_at,
        data.updated_at::timestamptz      AS repo_updated_at,
        data.pushed_at::timestamptz       AS repo_pushed_at,
        data.owner.login::varchar(255)    AS owner_login,
        data.owner.id::bigint             AS owner_id,
        data.owner."type"::varchar(50)    AS owner_type,
        data.html_url::varchar(1000)      AS html_url,
        data.homepage::varchar(1000)      AS homepage_url,
        data.license.spdx_id::varchar(50) AS license_spdx,
        load_id,
        file_path,
        ingested_at
    FROM raw_records
)

SELECT * FROM parsed
