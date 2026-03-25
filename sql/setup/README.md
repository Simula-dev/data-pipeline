# Snowflake Setup

Run these SQL scripts **in order** to prepare your Snowflake account for the pipeline.
They're idempotent — safe to re-run after the first setup.

## Prerequisites

- A Snowflake account with `ACCOUNTADMIN` access
- `cdk deploy DataPipeline-Ingestion` already run (you need the outputs)
- AWS CLI access to update the IAM role trust policy

## Order

| Script | What it creates |
|---|---|
| `01_database_warehouse.sql` | Database, warehouse, schemas, roles, service user |
| `02_storage_integration.sql` | S3 storage integration (bidirectional handshake) |
| `03_file_format_stage.sql` | NDJSON file format, external S3 stage |
| `04_raw_table.sql` | `RAW.LANDING` table, audit view, grants |

## Running them

### Option A — Snowflake web UI (easiest)

1. Log in to Snowflake as `ACCOUNTADMIN`
2. Open a new worksheet
3. Paste each script in order, filling in the placeholders, and run

### Option B — SnowSQL CLI

```bash
snowsql -a <account> -u <admin-user> -f 01_database_warehouse.sql
# ... etc
```

## The trust policy handshake (between scripts 02 and 03)

Snowflake storage integrations need a two-sided setup:

1. Run `02_storage_integration.sql`
2. At the bottom, `DESC INTEGRATION S3_RAW_INTEGRATION` prints two values:
   - `STORAGE_AWS_IAM_USER_ARN` — Snowflake's IAM user in your account
   - `STORAGE_AWS_EXTERNAL_ID` — unique external id for this integration
3. Update the AWS IAM role trust policy:

```bash
# Create trust-policy.json
cat > trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" }
    }
  }]
}
EOF

aws iam update-assume-role-policy \
    --role-name data-pipeline-snowflake-integration \
    --policy-document file://trust-policy.json

rm trust-policy.json
```

4. Then run `03_file_format_stage.sql`. The `LIST @S3_RAW_STAGE` at the bottom
   verifies the handshake worked — if it returns without error, Snowflake can
   successfully assume the role and list the bucket.

## Storing the service user password in SSM

After creating `PIPELINE_USER` in script 01, mirror the password in AWS:

```bash
aws ssm put-parameter \
    --name /data-pipeline/snowflake/password \
    --value "<your-password>" \
    --type SecureString \
    --overwrite
```

Same for the other config values (see `SETUP.md` in project root).
