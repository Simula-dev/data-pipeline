# Data Pipeline

AWS CDK data pipeline — S3 → Snowflake → dbt → SageMaker, orchestrated by Step Functions.

> **Setup instructions:** see [SETUP.md](SETUP.md) for installing Python, Node, CDK, AWS CLI, and running tests.

## Build status

| Step | Status | Module |
|---|---|---|
| 1. Ingest (HTTP + DataSync) | ✅ Complete | `lambdas/ingest/`, `cdk/stacks/datasync_stack.py` |
| 2. Load to Snowflake | ✅ Complete | `lambdas/load/`, `sql/setup/` |
| 3. dbt Transformation | ✅ Complete | `dbt/`, `cdk/stacks/compute_stack.py` |
| 4. ML Inference (SageMaker) | ✅ Complete | `ml/`, `lambdas/ml_export/`, `lambdas/ml_load/`, `cdk/stacks/sagemaker_stack.py` |
| 5. Data Quality Gate | ⬜ Stub | `lambdas/quality_gate/` |
| 6. Notify + Finalize | ⬜ Basic | `cdk/stacks/monitoring_stack.py` |

## Stack

| Layer | Technology |
|---|---|
| IaC | AWS CDK (Python) |
| Orchestration | AWS Step Functions |
| Storage | S3 (raw zone) + Snowflake |
| Transformation | dbt Core on ECS Fargate |
| AI / ML | AWS SageMaker |
| CI/CD | GitHub Actions → CDK Deploy |
| Monitoring | CloudWatch + SNS |

## Project structure

```
├── app.py                      # CDK entry point
├── cdk.json                    # CDK config (set your account/region here)
├── requirements.txt
├── cdk/stacks/
│   ├── ingestion_stack.py      # S3 bucket + ingest/load/quality Lambdas
│   ├── compute_stack.py        # ECS Fargate cluster for dbt
│   ├── stepfunctions_stack.py  # Pipeline state machine
│   └── monitoring_stack.py     # SNS + CloudWatch dashboard
├── lambdas/
│   ├── ingest/                 # Raw data extraction → S3
│   ├── quality_gate/           # Post-dbt quality checks against Snowflake
│   └── notify/                 # Standalone notification helper
├── dbt/
│   ├── Dockerfile              # dbt container (push to ECR before deploy)
│   ├── dbt_project.yml
│   ├── profiles.yml            # Snowflake connection (env vars at runtime)
│   └── models/
│       ├── staging/            # Raw → cleaned views
│       ├── intermediate/       # Business logic (ephemeral)
│       └── marts/              # Final analytical tables
├── ml/
│   └── train.py                # SageMaker training script entry point
└── .github/workflows/
    └── deploy.yml              # Push to main → cdk deploy
```

## Setup

### Prerequisites

```bash
# Node (for CDK CLI)
node --version   # 18+

# Python
python --version  # 3.12+

# CDK CLI
npm install -g aws-cdk
cdk --version

# Python deps
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### First deploy

1. Set your AWS account ID in `cdk.json`
2. Bootstrap CDK in your account (one-time):
   ```bash
   cdk bootstrap aws://YOUR_ACCOUNT_ID/us-east-1
   ```
3. Store Snowflake credentials in SSM Parameter Store:
   ```bash
   aws ssm put-parameter --name /data-pipeline/snowflake/account  --value "..." --type SecureString
   aws ssm put-parameter --name /data-pipeline/snowflake/user     --value "..." --type SecureString
   aws ssm put-parameter --name /data-pipeline/snowflake/password --value "..." --type SecureString
   aws ssm put-parameter --name /data-pipeline/snowflake/database --value "DATA_PIPELINE" --type String
   aws ssm put-parameter --name /data-pipeline/snowflake/warehouse --value "TRANSFORM_WH" --type String
   aws ssm put-parameter --name /data-pipeline/snowflake/schema   --value "raw" --type String
   ```
4. Build and push the dbt Docker image to ECR (see `dbt/Dockerfile`)
5. Deploy all stacks:
   ```bash
   cdk deploy --all --context alert_email=you@example.com
   ```

### GitHub Actions CI/CD

Add these secrets to your GitHub repo:
- `AWS_DEPLOY_ROLE_ARN` — IAM role ARN for OIDC deploy
- `AWS_ACCOUNT_ID`
- `ALERT_EMAIL`

Push to `main` triggers automatic deploy.

## Pipeline execution

Trigger a pipeline run by starting a Step Functions execution:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:YOUR_ACCOUNT:stateMachine:data-pipeline-orchestrator \
  --input '{"source": "my_api", "params": {}}'
```
