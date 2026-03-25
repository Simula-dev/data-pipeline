# Local Development Setup

Complete guide for getting the data pipeline running locally and deployed to AWS.

## 1. Install prerequisites

### Python 3.12+
Download from <https://www.python.org/downloads/> or:
```powershell
winget install Python.Python.3.12
```
Verify: `python --version`

### Node.js 20+ (for AWS CDK CLI)
Download from <https://nodejs.org/> or:
```powershell
winget install OpenJS.NodeJS
```
Verify: `node --version && npm --version`

### AWS CLI v2
```powershell
winget install Amazon.AWSCLI
```
Verify: `aws --version`

Then configure credentials:
```bash
aws configure
# Enter access key, secret, region (us-east-1), output (json)
```

### AWS CDK CLI
```bash
npm install -g aws-cdk
cdk --version
```

### Docker Desktop (for dbt image build)
Download from <https://www.docker.com/products/docker-desktop/>

## 2. Clone and set up the project

```bash
git clone <your-new-repo-url> data-pipeline
cd data-pipeline

# Create venv
python -m venv .venv
.venv\Scripts\activate     # Windows
# source .venv/bin/activate  # Mac/Linux

# Install Python deps (CDK + boto3 + pytest + moto)
pip install -r requirements-dev.txt
```

## 3. Configure your AWS account

Edit `cdk.json` and replace `YOUR_AWS_ACCOUNT_ID` with your real account ID:
```json
"account": "123456789012",
"region": "us-east-1"
```

Bootstrap CDK in your account (one-time per account/region):
```bash
cdk bootstrap aws://123456789012/us-east-1
```

## 4. Store secrets in SSM Parameter Store

Snowflake (used by Steps 2, 3, 5):
```bash
aws ssm put-parameter --name /data-pipeline/snowflake/account --value "xy12345.us-east-1" --type SecureString
aws ssm put-parameter --name /data-pipeline/snowflake/user --value "PIPELINE_USER" --type SecureString
aws ssm put-parameter --name /data-pipeline/snowflake/password --value "..." --type SecureString
aws ssm put-parameter --name /data-pipeline/snowflake/database --value "DATA_PIPELINE" --type String
aws ssm put-parameter --name /data-pipeline/snowflake/warehouse --value "TRANSFORM_WH" --type String
aws ssm put-parameter --name /data-pipeline/snowflake/schema --value "raw" --type String
```

Any API keys your ingest sources need:
```bash
aws ssm put-parameter --name /data-pipeline/secrets/my_api_token --value "..." --type SecureString
```

## 5. Deploy

```bash
# Synth to validate
cdk synth --all

# Deploy everything
cdk deploy --all --context alert_email=you@example.com
```

Or deploy a single stack during development:
```bash
cdk deploy DataPipeline-Ingestion
```

## 6. Run tests

```bash
pytest tests/ -v
```

Expected output: all tests pass (config, S3 writer, HTTP client).

## 7. Trigger a pipeline run

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:data-pipeline-orchestrator \
  --input file://examples/event_jsonplaceholder.json
```

See `examples/` for ready-to-use event payloads.
