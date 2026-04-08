#!/usr/bin/env python3
"""
CDK App entry point.

Dependency graph (stacks are deployed in this order):

    Network         \u2192 VPC (3 AZs)
    \u2193
    Ingestion       \u2192 S3 raw bucket + Lambdas
    \u2193
    Redshift        \u2192 Serverless workgroup + dbt SG + admin secret + SSM params
    \u2193
    Compute         \u2192 ECS cluster + dbt task def + dbt image (uses Redshift creds)
    \u2193
    DataSync        \u2192 staging bucket + DataSync task (independent of Redshift)
    SageMaker       \u2192 execution role + model package group
    Monitoring      \u2192 SNS topic + notify Lambda + dashboard (independent)
    \u2193
    Orchestration   \u2192 Step Functions state machine (wires everything)
"""

import aws_cdk as cdk

from cdk.stacks.network_stack import NetworkStack
from cdk.stacks.ingestion_stack import IngestionStack
from cdk.stacks.redshift_stack import RedshiftStack
from cdk.stacks.compute_stack import ComputeStack
from cdk.stacks.datasync_stack import DataSyncStack
from cdk.stacks.sagemaker_stack import SageMakerStack
from cdk.stacks.monitoring_stack import MonitoringStack
from cdk.stacks.stepfunctions_stack import StepFunctionsStack


app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

network = NetworkStack(app, "DataPipeline-Network", env=env)

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

redshift = RedshiftStack(
    app,
    "DataPipeline-Redshift",
    vpc=network.vpc,
    raw_bucket=ingestion.raw_bucket,
    env=env,
)

compute = ComputeStack(
    app,
    "DataPipeline-Compute",
    vpc=network.vpc,
    dbt_security_group=redshift.dbt_security_group,
    redshift_admin_secret=redshift.admin_secret,
    redshift_workgroup_param=redshift.workgroup_param,
    redshift_database_param=redshift.database_param,
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
