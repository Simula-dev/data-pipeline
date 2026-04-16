# Local dbt development

How to run dbt against the deployed RDS instance from your laptop. The database
lives in a private subnet, so we tunnel through a small EC2 bastion using
AWS Systems Manager.

## One-time setup

### 1. Install the Session Manager plugin

The SSM plugin is separate from the AWS CLI. Download and install:
https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html

Verify:
```powershell
aws ssm start-session help
```

### 2. Deploy the bastion stack

```powershell
cdk deploy DataPipeline-Bastion --require-approval never
```

Takes about 2 minutes. Creates a t3.micro EC2 in the VPC private subnet.

### 3. Install dbt locally

From the project root:
```powershell
.venv\Scripts\activate
pip install dbt-core==1.8.* dbt-postgres==1.8.*
cd dbt
dbt deps --profiles-dir .
cd ..
```

## Starting a dev session

You need two terminals.

**Terminal 1: open the tunnel**
```powershell
.\scripts\dev-tunnel.ps1
```
Leave this running. You'll see "Waiting for connections..." when it's ready.

**Terminal 2: set env vars and run dbt**
```powershell
.\scripts\dev-env.ps1
cd dbt
dbt run --profiles-dir . --target dev
```

## Typical workflow

```powershell
# Compile without running (fast iteration on SQL)
dbt compile --profiles-dir . --target dev

# Run a single model
dbt run --profiles-dir . --target dev --select stg_github_repos

# Run a model and everything downstream of it
dbt run --profiles-dir . --target dev --select stg_github_repos+

# Run tests
dbt test --profiles-dir . --target dev

# Generate and serve docs in your browser
dbt docs generate --profiles-dir . --target dev
dbt docs serve --profiles-dir .
```

## Cost control

The bastion is t3.micro which is free for the first 12 months of your AWS account.
After that it's about $8/month running 24/7.

To save money when you're not developing, stop the instance:
```powershell
$id = aws cloudformation describe-stacks --stack-name DataPipeline-Bastion `
    --query 'Stacks[0].Outputs[?OutputKey==`BastionInstanceId`].OutputValue' --output text
aws ec2 stop-instances --instance-ids $id
```

Start it again when needed:
```powershell
aws ec2 start-instances --instance-ids $id
```

Stopped instances only incur EBS storage cost (~$1/month for an 8GB root volume).

## Connecting other tools

Any PostgreSQL client works once the tunnel is open. Point it at `localhost:5432`
with the credentials from `dev-env.ps1`.

- **DBeaver**: Database -> New connection -> PostgreSQL
- **psql**: `psql -h localhost -p 5433 -U dbadmin -d data_pipeline`
- **pgcli**: `pgcli postgresql://dbadmin@localhost:5433/data_pipeline`
