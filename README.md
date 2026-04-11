# Data Pipeline

I built this to answer a question that kept bugging me: what does a real production data pipeline actually look like? Not a tutorial that stops at "read CSV, load to database," but the full thing - ingestion, transformation, ML inference, quality gates, alerting, CI/CD, all wired together with infrastructure-as-code.

The result is a fully orchestrated ETL/ELT pipeline on AWS. It pulls from any REST API, lands raw data in S3, loads into PostgreSQL as jsonb, transforms through dbt on ECS Fargate, optionally runs SageMaker batch inference, validates data quality, and notifies on success or failure. One command deploys it. One command tears it down.

For the full architecture walkthrough, see [docs/architecture.md](docs/architecture.md).

## How the pipeline flows

Everything is orchestrated by a Step Functions state machine. When you trigger a run, it kicks off the ingest Lambda, which pulls data from whatever REST API you've configured and drops the raw JSON into S3. From there, a load Lambda picks it up and writes it into PostgreSQL's raw schema as jsonb.

Once the raw data lands, dbt takes over. It runs inside a Fargate container and transforms data through three layers: staging models that parse jsonb into typed columns, intermediate models for business logic like dedup and enrichment, and mart models that produce the final analytics-ready tables. All standard dbt layering.

After dbt finishes, a quality gate Lambda runs validation checks against the transformed data. If everything looks good and you've configured the ML step, SageMaker batch transform runs inference. Finally, a notification goes out via SNS - either a success summary or failure details with enough context to debug.

If any step fails, the state machine short-circuits to the notification step. You get an alert with the failure details, not silence.

## Tech stack

| Layer | Technology |
|---|---|
| IaC | AWS CDK (Python) |
| Orchestration | AWS Step Functions |
| Database | Amazon RDS PostgreSQL (free tier) |
| Object storage | S3 (raw landing zone) |
| Transformation | dbt Core (dbt-postgres) on ECS Fargate |
| DB driver | pg8000 (pure Python, no Docker bundling needed) |
| ML | AWS SageMaker (Batch Transform) |
| CI/CD | GitHub Actions |
| Monitoring | CloudWatch (EMF custom metrics) + SNS |

This pipeline went through a few warehouse iterations (Snowflake, then Redshift Serverless, then RDS PostgreSQL). The warehouse-coupled layer is roughly 30% of the codebase, cleanly separated from the rest - so swapping databases is a focused refactor, not a rewrite. The earlier implementations are in the commit history if you're curious.

## Project structure

```
app.py                          # CDK entry point
cdk.json                        # CDK config (account/region)
cdk/stacks/
    ingestion_stack.py          # S3 bucket + ingest/load/quality Lambdas
    compute_stack.py            # ECS Fargate cluster for dbt
    stepfunctions_stack.py      # Pipeline state machine
    monitoring_stack.py         # SNS + CloudWatch dashboard
lambdas/
    ingest/                     # Raw data extraction to S3
    quality_gate/               # Post-dbt quality checks
    notify/                     # Notification helper
dbt/
    Dockerfile                  # dbt container (push to ECR before deploy)
    models/
        staging/                # Raw to cleaned views
        intermediate/           # Business logic (ephemeral)
        marts/                  # Final analytical tables
ml/
    train.py                    # SageMaker training entry point
.github/workflows/
    deploy.yml                  # Push to main triggers cdk deploy
```

## Getting started

You'll need Python 3.12+, Node 18+ (for the CDK CLI), and the AWS CLI configured with credentials. If that's already set up, the quick version is:

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
npm install -g aws-cdk
```

For a full walkthrough - including CDK bootstrap, database setup, and your first pipeline run - see [SETUP.md](SETUP.md).

Once everything is deployed, trigger a pipeline run with:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:YOUR_ACCOUNT:stateMachine:data-pipeline-orchestrator \
  --input '{"source": "my_api", "params": {}}'
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for branching conventions, commit style, and PR workflow.
