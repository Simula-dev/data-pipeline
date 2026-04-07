# Local Development Setup

Complete guide for getting the data pipeline running locally and deployed to AWS.

## 1. Install prerequisites

### Python 3.12+
```powershell
winget install Python.Python.3.12
```
Verify: `python --version`

### Node.js 20+ (for AWS CDK CLI)
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
Only needed locally if you want to run `cdk synth --all` or `cdk deploy`
against the dbt `DockerImageAsset`. Lambda bundling is NOT required — all
Lambdas use the Redshift Data API via boto3 (in the runtime).
<https://www.docker.com/products/docker-desktop/>

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

# Run the test suite (no AWS credentials required)
pytest tests/ -v
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

## 4. Validate stacks synthesize locally

```bash
cdk synth --all --context alert_email=you@example.com
```

Should produce 7 CloudFormation templates under `cdk.out/`:
- `DataPipeline-Compute`
- `DataPipeline-Redshift`
- `DataPipeline-Ingestion`
- `DataPipeline-DataSync`
- `DataPipeline-SageMaker`
- `DataPipeline-Monitoring`
- `DataPipeline-Orchestration`

## 5. Deploy the infrastructure

```bash
cdk deploy --all --context alert_email=you@example.com --require-approval never
```

First deploy takes ~15-20 minutes. Watch for the stack outputs at the end:
- `DataPipeline-Redshift.AdminSecretArn` — Secrets Manager ARN with the Redshift admin password
- `DataPipeline-Redshift.WorkgroupName` — should be `data-pipeline`
- `DataPipeline-Ingestion.RawBucketName` — the S3 raw bucket

## 6. Run the Redshift setup SQL

Follow `sql/setup/README.md`. Quick version via Redshift Query Editor v2:

1. AWS Console → Redshift → Query editor v2
2. Connect to the `data-pipeline` workgroup using admin credentials
   (retrieve from Secrets Manager: `data-pipeline/redshift/admin`)
3. Open and run `sql/setup/01_schemas.sql`
4. Open and run `sql/setup/02_tables.sql`

## 7. (Optional) Store API keys for HTTP ingest sources

If your ingest sources need authentication (GitHub PAT, etc.):
```bash
aws ssm put-parameter \
  --name /data-pipeline/secrets/github_token \
  --value "ghp_..." \
  --type SecureString
```

## 8. Trigger a pipeline run

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:data-pipeline-orchestrator \
  --input file://examples/event_jsonplaceholder.json
```

See `examples/` for ready-to-use event payloads. The simplest is
`event_jsonplaceholder.json` — no auth, no setup beyond the infrastructure.

## 9. Monitor the execution

- **Step Functions console**: watch the visual state machine graph
- **CloudWatch dashboard**: `data-pipeline` dashboard
- **Notify SNS topic**: `data-pipeline-notifications` — subscribe your email
  via the `alert_email` context arg or manually via the SNS console
