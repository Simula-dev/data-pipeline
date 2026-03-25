#!/usr/bin/env python3
"""
CDK App entry point.
Instantiates all pipeline stacks and wires cross-stack references.
"""

import aws_cdk as cdk
from cdk.stacks.ingestion_stack import IngestionStack
from cdk.stacks.compute_stack import ComputeStack
from cdk.stacks.stepfunctions_stack import StepFunctionsStack
from cdk.stacks.monitoring_stack import MonitoringStack
from cdk.stacks.datasync_stack import DataSyncStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

ingestion = IngestionStack(app, "DataPipeline-Ingestion", env=env)
compute = ComputeStack(app, "DataPipeline-Compute", env=env)
monitoring = MonitoringStack(app, "DataPipeline-Monitoring", env=env)

datasync = DataSyncStack(
    app,
    "DataPipeline-DataSync",
    raw_bucket=ingestion.raw_bucket,
    env=env,
)

StepFunctionsStack(
    app,
    "DataPipeline-Orchestration",
    ingest_function=ingestion.ingest_function,
    load_function=ingestion.load_function,
    quality_gate_function=ingestion.quality_gate_function,
    notify_topic=monitoring.notify_topic,
    dbt_cluster=compute.cluster,
    dbt_task_definition=compute.dbt_task_definition,
    env=env,
)

app.synth()
