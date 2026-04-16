#!/usr/bin/env python3
"""
CDK App entry point.

Dependency graph:
    Network   \u2192 VPC (3 AZs)
    RDS       \u2192 PostgreSQL instance + pipeline SG + admin secret + SSM params
    Ingestion \u2192 S3 raw bucket + Lambdas (VPC-attached, pg8000)
    Compute   \u2192 ECS cluster + dbt task def (uses RDS creds)
    DataSync  \u2192 staging bucket + DataSync task
    SageMaker \u2192 execution role + model package group
    Monitoring \u2192 SNS + notify Lambda + dashboard
    Orchestration \u2192 Step Functions state machine
"""

import aws_cdk as cdk

from cdk.stacks.network_stack import NetworkStack
from cdk.stacks.rds_stack import RDSStack
from cdk.stacks.ingestion_stack import IngestionStack
from cdk.stacks.compute_stack import ComputeStack
from cdk.stacks.datasync_stack import DataSyncStack
from cdk.stacks.sagemaker_stack import SageMakerStack
from cdk.stacks.monitoring_stack import MonitoringStack
from cdk.stacks.stepfunctions_stack import StepFunctionsStack
from cdk.stacks.bastion_stack import BastionStack


app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

network = NetworkStack(app, "DataPipeline-Network", env=env)

rds = RDSStack(
    app,
    "DataPipeline-RDS",
    vpc=network.vpc,
    env=env,
)

ingestion = IngestionStack(
    app,
    "DataPipeline-Ingestion",
    vpc=network.vpc,
    pipeline_security_group=rds.pipeline_security_group,
    rds_secret=rds.admin_secret,
    env=env,
)

compute = ComputeStack(
    app,
    "DataPipeline-Compute",
    vpc=network.vpc,
    pipeline_security_group=rds.pipeline_security_group,
    rds_admin_secret=rds.admin_secret,
    rds_host_param=rds.host_param,
    rds_database_param=rds.database_param,
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

bastion = BastionStack(
    app,
    "DataPipeline-Bastion",
    vpc=network.vpc,
    pipeline_security_group=rds.pipeline_security_group,
    env=env,
)

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
    dbt_security_group=compute.pipeline_security_group,
    raw_bucket_name=ingestion.raw_bucket.bucket_name,
    env=env,
)

app.synth()
