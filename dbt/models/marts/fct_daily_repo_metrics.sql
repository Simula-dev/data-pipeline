{{
    config(
        materialized='incremental',
        unique_key=['repo_id', 'metric_date'],
        on_schema_change='append_new_columns',
        tags=['marts', 'github', 'fact']
    )
}}

-- Daily repo metrics fact table. Incremental \u2014 each run appends any new
-- (repo_id, metric_date) combinations not yet in the target.
--
-- On the very first run, processes all history. Subsequent runs only
-- process ingests newer than the latest metric_date already materialized.

WITH source_data AS (
    SELECT
        repo_id,
        DATE(ingested_at) AS metric_date,
        star_count,
        fork_count,
        watcher_count,
        open_issue_count,
        ingested_at
    FROM {{ ref('stg_github_repos') }}

    {% if is_incremental() %}
        -- Only consider ingests strictly newer than the latest materialized date.
        -- Using `>= MAX(metric_date)` catches same-day late-arriving data.
        WHERE DATE(ingested_at) >= (SELECT COALESCE(MAX(metric_date), '1900-01-01') FROM {{ this }})
    {% endif %}
),

daily_agg AS (
    SELECT
        repo_id,
        metric_date,
        MAX(star_count)         AS star_count,
        MAX(fork_count)         AS fork_count,
        MAX(watcher_count)      AS watcher_count,
        MAX(open_issue_count)   AS open_issue_count,
        COUNT(*)                AS observation_count,
        MAX(ingested_at)        AS last_observed_at
    FROM source_data
    GROUP BY repo_id, metric_date
)

SELECT * FROM daily_agg
