# Database Setup

Two SQL scripts to run after `cdk deploy DataPipeline-RDS` completes. They're idempotent - safe to re-run.

## What you'll need

- The deploy has finished and the RDS instance is available
- Admin credentials from Secrets Manager (`data-pipeline/rds/admin`)

## Scripts

| Script | What it creates |
|---|---|
| `01_schemas.sql` | Schemas (raw, staging, intermediate, marts, ml), roles, grants |
| `02_tables.sql` | `raw.landing` table, audit view, ML tables |

## How to run them

Since the RDS instance is in a private subnet, you can't connect directly from your laptop. A few options:

### Option A - Temporary admin Lambda (quickest)

Create a small Lambda with pg8000 in the same VPC, invoke it with the SQL statements, then delete it. This is what the initial setup used during development.

### Option B - EC2 bastion host

If you have a bastion/jump box in the VPC, connect through it:

```bash
# Get the admin password from Secrets Manager
PASSWORD=$(aws secretsmanager get-secret-value \
    --secret-id data-pipeline/rds/admin \
    --query SecretString --output text | python -c "import sys,json; print(json.loads(sys.stdin.read())['password'])")

# Get the RDS endpoint
ENDPOINT=$(aws rds describe-db-instances \
    --query 'DBInstances[?DBName==`data_pipeline`].Endpoint.Address' --output text)

PGPASSWORD=$PASSWORD psql -h $ENDPOINT -p 5432 -U dbadmin -d data_pipeline -f 01_schemas.sql
PGPASSWORD=$PASSWORD psql -h $ENDPOINT -p 5432 -U dbadmin -d data_pipeline -f 02_tables.sql
```

### Option C - AWS Session Manager port forwarding

If you have SSM access to an instance in the VPC, you can forward the PostgreSQL port to localhost without a traditional bastion.
