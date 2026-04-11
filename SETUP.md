# Local Development Setup

This walks you through everything you need to get the pipeline running locally and deployed to your AWS account. It looks like a lot of steps, but most of it is one-time setup that you won't touch again.

## What you'll need

Before diving in, make sure you have these installed. If you're on Windows, `winget` makes this painless. Mac/Linux equivalents are straightforward - just swap the install commands.

### Python 3.12+
```powershell
winget install Python.Python.3.12
```
Verify: `python --version`

### Node.js 20+ (for the CDK CLI)
```powershell
winget install OpenJS.NodeJS
```
Verify: `node --version && npm --version`

### AWS CLI v2
```powershell
winget install Amazon.AWSCLI
```
Verify: `aws --version`

Then configure your credentials:
```bash
aws configure
# Enter access key, secret, region (us-east-1), output (json)
```

### AWS CDK CLI
```bash
npm install -g aws-cdk
cdk --version
```

### Docker Desktop (optional)

You only need Docker if you want to run `cdk synth --all` or `cdk deploy` locally, since the dbt stack uses a `DockerImageAsset`. The Lambda functions don't need it - they use pg8000 (pure Python) which gets bundled locally without Docker.

<https://www.docker.com/products/docker-desktop/>

## Clone and set up the project

Pretty standard Python project setup here:

```bash
git clone <your-new-repo-url> data-pipeline
cd data-pipeline

# Create a virtualenv
python -m venv .venv
.venv\Scripts\activate     # Windows
# source .venv/bin/activate  # Mac/Linux

# Install everything (CDK + boto3 + pytest + moto)
pip install -r requirements-dev.txt

# Make sure the tests pass before going further
pytest tests/ -v
```

The test suite is fully mocked with moto, so you don't need AWS credentials to run it.

## Configure your AWS account

Open `cdk.json` and replace `YOUR_AWS_ACCOUNT_ID` with your actual account ID:
```json
"account": "123456789012",
"region": "us-east-1"
```

Then bootstrap CDK in your account. This is a one-time thing per account/region:
```bash
cdk bootstrap aws://123456789012/us-east-1
```

## Validate locally before deploying

This is a good sanity check. It synthesizes all the CloudFormation templates without actually deploying anything:

```bash
cdk synth --all --context alert_email=you@example.com
```

You should see 8 templates under `cdk.out/`:
- `DataPipeline-Network`
- `DataPipeline-RDS`
- `DataPipeline-Compute`
- `DataPipeline-Ingestion`
- `DataPipeline-DataSync`
- `DataPipeline-SageMaker`
- `DataPipeline-Monitoring`
- `DataPipeline-Orchestration`

## Deploy the infrastructure

Here's the big moment:

```bash
cdk deploy --all --context alert_email=you@example.com --require-approval never
```

First deploy takes 15-20 minutes (the RDS instance is the slow part). Grab some coffee. When it finishes, look for these stack outputs:
- `DataPipeline-RDS.AdminSecretArn` - Secrets Manager ARN with your PostgreSQL admin password
- `DataPipeline-RDS.RDSEndpoint` - the database hostname
- `DataPipeline-Ingestion.RawBucketName` - your S3 raw bucket

## Run the database setup SQL

The database needs schemas and tables before the pipeline can load data. Full details are in `sql/setup/README.md`. Since the RDS instance is in a private subnet, the easiest way is to use a temporary Lambda or connect via the admin credentials from Secrets Manager.

The SQL scripts to run (in order):
1. `sql/setup/01_schemas.sql` - creates schemas, roles, and grants
2. `sql/setup/02_tables.sql` - creates the landing table, ML tables, and audit views

## (Optional) Store API keys for ingest sources

If your data sources need authentication (like a GitHub PAT), store them in SSM Parameter Store:
```bash
aws ssm put-parameter \
  --name /data-pipeline/secrets/github_token \
  --value "ghp_..." \
  --type SecureString
```

## Trigger your first pipeline run

Now for the fun part. The `examples/` folder has ready-to-use event payloads. The simplest one pulls from JSONPlaceholder - no auth needed, no extra setup:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:data-pipeline-orchestrator \
  --input file://examples/event_jsonplaceholder.json
```

## Watching it run

Once the execution starts, you've got a few ways to follow along:

- **Step Functions console** has a visual graph that updates in real time as each step completes or fails. This is the best view for debugging.
- **CloudWatch dashboard** named `data-pipeline` shows metrics across runs.
- **SNS notifications** go to whatever email you passed as `alert_email`. You'll get a message when the pipeline finishes, whether it succeeded or failed.
