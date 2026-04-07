# Redshift Setup

Two SQL scripts to run after `cdk deploy DataPipeline-Redshift` completes.
They're idempotent — safe to re-run.

## Prerequisites

- `cdk deploy` has finished and the `data-pipeline` workgroup is available
- You have the admin password from AWS Secrets Manager:
  `data-pipeline/redshift/admin`

## Order

| Script | What it creates |
|---|---|
| `01_schemas.sql` | Schemas (raw, staging, intermediate, marts, ml), groups, grants |
| `02_tables.sql` | `raw.landing` table, audit view, ml tables |

## Running them

### Option A — Redshift Query Editor v2 (easiest)

1. Log in to AWS Console → Redshift → Query editor v2
2. Connect to the `data-pipeline` workgroup using admin credentials
3. Open each SQL file, paste, run

### Option B — psql / DBeaver

```bash
# Get admin password from Secrets Manager
PASSWORD=$(aws secretsmanager get-secret-value \
    --secret-id data-pipeline/redshift/admin \
    --query SecretString --output text | jq -r .password)

# Get the workgroup endpoint
ENDPOINT=$(aws redshift-serverless get-workgroup \
    --workgroup-name data-pipeline \
    --query 'workgroup.endpoint.address' --output text)

PGPASSWORD=$PASSWORD psql -h $ENDPOINT -p 5439 -U admin -d data_pipeline -f 01_schemas.sql
PGPASSWORD=$PASSWORD psql -h $ENDPOINT -p 5439 -U admin -d data_pipeline -f 02_tables.sql
```

Note: the workgroup is in a private subnet, so psql from your laptop won't work
unless you use a bastion host or VPN. Use Query Editor v2 (which runs inside AWS)
for simplicity.

### Option C — Redshift Data API from your laptop (no network access needed)

```bash
aws redshift-data execute-statement \
    --workgroup-name data-pipeline \
    --database data_pipeline \
    --sql "$(cat 01_schemas.sql)"

aws redshift-data execute-statement \
    --workgroup-name data-pipeline \
    --database data_pipeline \
    --sql "$(cat 02_tables.sql)"
```

The Data API calls go through the AWS API plane — no network path to the
cluster needed.
