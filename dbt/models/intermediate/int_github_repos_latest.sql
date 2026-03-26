{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'github']
    )
}}

-- Intermediate model: one row per repo, picking the most recent ingest.
-- Ephemeral materialization \u2014 inlined into downstream models, no warehouse
-- storage cost. Use for business logic that doesn't need its own table.

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM {{ ref('stg_github_repos') }}
)

SELECT
    * EXCLUDE (row_num)
FROM ranked
WHERE row_num = 1
