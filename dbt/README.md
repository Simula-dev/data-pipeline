# dbt Project

Transforms raw VARIANT data in `DATA_PIPELINE.RAW.LANDING` into analytics-ready
marts. Runs on ECS Fargate, orchestrated by the pipeline Step Function.

## Layers

| Layer | Schema | Materialization | Purpose |
|---|---|---|---|
| **Staging** | `STAGING` | view | One model per source. Parses `VARIANT` into typed columns. |
| **Intermediate** | `INTERMEDIATE` | ephemeral | Business logic (dedup, enrichment). No warehouse storage. |
| **Marts** | `MARTS` | table / incremental | Analytics-ready dimensions and facts. |

## Example models (delete and replace with your own)

- `stg_github_repos` — parses GitHub `/search/repositories` responses
- `stg_coingecko_markets` — parses CoinGecko `/coins/markets` responses
- `int_github_repos_latest` — latest observation per repo
- `dim_repos` — repo dimension table
- `fct_daily_repo_metrics` — incremental daily metrics fact

## Adding a new source

1. Ingest the source via the Step 1 Lambda with `source_name: my_new_source`
2. Create `models/staging/stg_my_new_source.sql`:
   ```sql
   {{ config(materialized='view', tags=['staging']) }}

   WITH raw AS ( {{ parse_landing_source('my_new_source') }} )

   SELECT
       data:id::NUMBER   AS my_id,
       data:name::STRING AS my_name,
       ...
       load_id,
       ingested_at
   FROM raw
   ```
3. Add the model entry to `models/staging/_stg_schema.yml` with tests
4. `dbt run -s stg_my_new_source` locally to verify
5. Commit and push — GitHub Actions deploys the new image + state machine picks it up

## Running dbt locally

```bash
cd dbt

# Install Python deps
pip install dbt-core==1.8.* dbt-snowflake==1.8.*

# Install dbt packages (dbt_utils)
dbt deps --profiles-dir .

# Set env vars
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="PIPELINE_USER"
export SNOWFLAKE_PASSWORD="..."

# Run against the dev target
dbt build --profiles-dir . --target dev

# Inspect generated docs
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir .
```

## Running dbt in the pipeline

Step Functions invokes the dbt Fargate task with:

```bash
dbt build --profiles-dir /app/dbt --target prod
```

`dbt build` runs **source freshness → seeds → models → tests** in one pass,
in correct DAG order. If any test fails, downstream models are skipped and
the task exits non-zero — Step Functions routes to the failure notification.

## Debugging a failed run

```bash
# Tail Fargate container logs
aws logs tail /data-pipeline/dbt --follow

# Filter to errors
aws logs tail /data-pipeline/dbt --follow --filter-pattern "ERROR"

# Run a single model locally against the same target
dbt run -s stg_github_repos --profiles-dir . --target prod
```
