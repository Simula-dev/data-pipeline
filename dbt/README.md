# dbt Project

This is the transformation layer. It takes the raw jsonb data that the ingest/load steps drop into `raw.landing` and turns it into clean, typed, analytics-ready tables. Runs on ECS Fargate as part of the Step Functions pipeline.

## Model layers

The models follow standard dbt layering:

**Staging** (`staging` schema, materialized as views) - One model per source. These parse the raw `jsonb` column into properly typed columns using PostgreSQL's `->>`  operator. Think of them as the "unpack and clean" step.

**Intermediate** (`intermediate` schema, ephemeral) - Business logic lives here: deduplication, enrichment, joins across sources. Ephemeral means they don't create tables in the database - they're just reusable CTEs that dbt inlines into downstream models.

**Marts** (`marts` schema, tables or incremental) - The final output. Dimensions and facts that are ready for dashboards, analysis, or downstream consumers.

## Example models

These ship as working examples you can replace with your own sources:

- `stg_github_repos` - parses GitHub `/search/repositories` responses
- `stg_coingecko_markets` - parses CoinGecko `/coins/markets` responses
- `int_github_repos_latest` - latest observation per repo
- `dim_repos` - repo dimension table
- `fct_daily_repo_metrics` - incremental daily metrics fact

## Adding a new source

Once you've set up a new ingest source (Step 1 Lambda with `source_name: my_new_source`), adding the dbt models is straightforward:

1. Create `models/staging/stg_my_new_source.sql`:
   ```sql
   {{ config(materialized='view', tags=['staging']) }}

   WITH raw AS ( {{ parse_landing_source('my_new_source') }} )

   SELECT
       (data->>'id')::bigint   AS my_id,
       data->>'name'           AS my_name,
       ...
       load_id,
       ingested_at
   FROM raw
   ```
2. Add the model entry to `models/staging/_stg_schema.yml` with tests
3. Run `dbt run -s stg_my_new_source` locally to verify it works
4. Commit and push - GitHub Actions deploys the new image and the state machine picks it up on the next run

## Running dbt locally

```bash
cd dbt

# Install Python deps
pip install dbt-core==1.8.* dbt-postgres==1.8.*

# Install dbt packages (dbt_utils)
dbt deps --profiles-dir .

# Set env vars (get these from Secrets Manager: data-pipeline/rds/admin)
export POSTGRES_HOST="your-rds-endpoint.region.rds.amazonaws.com"
export POSTGRES_USER="dbadmin"
export POSTGRES_PASSWORD="..."
export POSTGRES_DATABASE="data_pipeline"

# Run against the dev target
dbt build --profiles-dir . --target dev

# Browse the generated docs
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir .
```

## How dbt runs in the pipeline

Step Functions kicks off the dbt Fargate task with:

```bash
dbt build --profiles-dir /app/dbt --target prod
```

`dbt build` handles source freshness, seeds, models, and tests in one pass, respecting the DAG order. If any test fails, downstream models get skipped and the task exits non-zero. Step Functions catches that and routes to the failure notification.

## Debugging a failed run

```bash
# Tail the Fargate container logs
aws logs tail /data-pipeline/dbt --follow

# Filter to just errors
aws logs tail /data-pipeline/dbt --follow --filter-pattern "ERROR"

# Reproduce locally against the same target
dbt run -s stg_github_repos --profiles-dir . --target prod
```
