{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'github']
    )
}}

-- Intermediate model: one row per repo, picking the most recent ingest.
-- PostgreSQL doesn't support SELECT * EXCLUDE (...), so we list columns explicitly.

WITH ranked AS (
    SELECT
        repo_id,
        repo_name,
        full_name,
        description,
        language,
        star_count,
        fork_count,
        watcher_count,
        open_issue_count,
        size_kb,
        default_branch,
        topics,
        repo_created_at,
        repo_updated_at,
        repo_pushed_at,
        owner_login,
        owner_id,
        owner_type,
        html_url,
        homepage_url,
        license_spdx,
        load_id,
        file_path,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM {{ ref('stg_github_repos') }}
)

SELECT
    repo_id,
    repo_name,
    full_name,
    description,
    language,
    star_count,
    fork_count,
    watcher_count,
    open_issue_count,
    size_kb,
    default_branch,
    topics,
    repo_created_at,
    repo_updated_at,
    repo_pushed_at,
    owner_login,
    owner_id,
    owner_type,
    html_url,
    homepage_url,
    license_spdx,
    load_id,
    file_path,
    ingested_at
FROM ranked
WHERE row_num = 1
