"""
Notify Lambda \u2014 formats execution results, publishes to SNS, emits CloudWatch metrics.

Invoked by Step Functions at the two terminal states:
  - NotifySuccess (when quality gate passes)
  - NotifyFailure (when any task errors or quality gate fails)

Expected event shape (constructed in stepfunctions_stack.py):
    {
        "status": "SUCCESS" | "FAILURE",
        "executionId": "...",
        "executionArn": "arn:aws:states:...:execution:...:...",
        "startTime": "2026-04-08T12:00:00Z",
        "stateMachineName": "data-pipeline-orchestrator",
        "region": "us-east-1",
        "state": { ... full state machine state ... }
    }
"""

from __future__ import annotations

import os

import boto3

from formatter import (
    build_message,
    build_subject,
    compute_duration_seconds,
    extract_stats,
)
from logger import get_logger, log_event
from metrics import emit_pipeline_metrics


logger = get_logger("notify")
sns = boto3.client("sns")

TOPIC_ARN = os.environ["NOTIFY_TOPIC_ARN"]


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "notify_invoked", status=event.get("status"))

    status = event.get("status", "UNKNOWN")
    execution_id = event.get("executionId", "")
    execution_arn = event.get("executionArn")
    start_time = event.get("startTime")
    state_machine_name = event.get("stateMachineName", "data-pipeline-orchestrator")
    region = event.get("region", os.environ.get("AWS_REGION", "us-east-1"))
    state = event.get("state") or {}

    stats = extract_stats(state)
    duration = compute_duration_seconds(start_time)

    message = build_message(
        status=status,
        execution_id=execution_id,
        execution_arn=execution_arn,
        start_time=start_time,
        state_machine_name=state_machine_name,
        region=region,
        stats=stats,
    )
    subject = build_subject(status, execution_id, stats)

    # Emit metrics via EMF (just a print statement) \u2014 free, no API calls
    emit_pipeline_metrics(status=status, stats=stats, duration_seconds=duration)

    response = sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=message,
    )

    log_event(
        logger,
        "notify_complete",
        status=status,
        subject=subject,
        sns_message_id=response.get("MessageId"),
        duration_seconds=duration,
    )

    return {
        "status": status,
        "subject": subject,
        "messageId": response.get("MessageId"),
        "durationSeconds": duration,
    }
