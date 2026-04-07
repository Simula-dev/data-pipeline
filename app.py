#!/usr/bin/env python3
"""
CDK App entry point.
Instantiates all pipeline stacks and wires cross-stack references.

Dependency graph:
    Compute  \u2192 VPC + dbt Fargate SG + dbt task def
    \u2193
    Redshift \u2192 Serverless namespace/workgroup (uses VPC + grants 5439 from dbt SG)
    \u2193                                          + writes SSM / Secrets Manager
    Ingestion \u2192 S3 raw bucket + Lambdas (references Redshift workgroup/db names)
    \u2193
    SageMaker \u2192 execution role + model package group (references raw bucket)
    \u2193
    Monitoring \u2192 SNS topic + notify Lambda + dashboard
    \u2193
    Orchestration \u2192 Step Functions state machine (wires all of the above)
"""

import aws_cdk as cdk

from cdk.stacks.compute_stack import ComputeStack
from cdk.stacks.redshift_stack import RedshiftStack
from cdk.stacks.ingestion_stack import IngestionStack
from cdk.stacks.datasync_stack import DataSyncStack
from cdk.stacks.sagemaker_stack import SageMakerStack
from cdk.stacks.monitoring_stack import MonitoringStack
from cdk.stacks.stepfunctions_stack import StepFunctionsStack


app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

# Compute first \u2014 owns VPC + dbt Fargate SG + task definition shell
compute = ComputeStack(app, "DataPipeline-Compute", env=env)

# Ingestion creates the S3 raw bucket (needed by Redshift + SageMaker + DataSync)
# It references Redshift workgroup/database by fixed name, so no construct dep.
ingestion = IngestionStack(
    app,
    "DataPipeline-Ingestion",
    redshift_workgroup_name="data-pipeline",
    redshift_database_name="data_pipeline",
    redshift_s3_role_arn=(
        f"arn:aws:iam::{env.account}:role/data-pipeline-redshift-s3"
    ),
    env=env,
)

# Redshift needs VPC + dbt SG from Compute, raw bucket from Ingestion
redshift = RedshiftStack(
    app,
    "DataPipeline-Redshift",
    vpc=compute.vpc,
    raw_bucket=ingestion.raw_bucket,
    dbt_security_group=compute.dbt_security_group,
    env=env,
)

datasync = DataSyncStack(
    app,
    "DataPipeline-DataSync",
    raw_bucket=ingestion.raw_bucket,
    env=env,
)

sagemaker_stack = SageMakerStack(
    app,
    "DataPipeline-SageMaker",
    raw_bucket=ingestion.raw_bucket,
    env=env,
)

monitoring = MonitoringStack(app, "DataPipeline-Monitoring", env=env)

StepFunctionsStack(
    app,
    "DataPipeline-Orchestration",
    ingest_function=ingestion.ingest_function,
    load_function=ingestion.load_function,
    ml_export_function=ingestion.ml_export_function,
    ml_load_function=ingestion.ml_load_function,
    quality_gate_function=ingestion.quality_gate_function,
    notify_function=monitoring.notify_function,
    dbt_cluster=compute.cluster,
    dbt_task_definition=compute.dbt_task_definition,
    dbt_security_group=compute.dbt_security_group,
    raw_bucket_name=ingestion.raw_bucket.bucket_name,
    env=env,
)

app.synth()
