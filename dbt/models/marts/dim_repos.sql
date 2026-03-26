{{
    config(
        materialized='table',
        tags=['marts', 'github', 'dimension']
    )
}}

-- Dimension table: one row per GitHub repo, with slowly-changing descriptive
-- attributes. Built from the latest observation in the intermediate layer.

SELECT
    repo_id,
    repo_name,
    full_name,
    owner_login,
    owner_id,
    owner_type,
    language,
    license_spdx,
    description,
    homepage_url,
    html_url,
    default_branch,
    topics,
    repo_created_at,
    repo_updated_at,
    ingested_at AS dim_refreshed_at
FROM {{ ref('int_github_repos_latest') }}
