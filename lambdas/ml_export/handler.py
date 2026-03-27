"""
ML Export Lambda \u2014 unloads Snowflake inference input table to S3 as CSV.

Uses Snowflake COPY INTO @stage to write directly to S3 (no data transits
through the Lambda). Returns the S3 prefix that Batch Transform will read.

Event (from Step Functions):
    { "loadId": "...", other upstream fields... }

Returns:
    {
        "s3InputPrefix": "s3://.../ml/input/<load_id>/",
        "rowsExported": 1234,
        "skipped": false
    }
"""

from __future__ import annotations

import os

import boto3

from logger import get_logger, log_event


logger = get_logger("ml_export")
ssm = boto3.client("ssm")

SSM_PREFIX = os.environ.get("SNOWFLAKE_PARAM_PREFIX", "/data-pipeline/snowflake")
RAW_BUCKET = os.environ["RAW_BUCKET"]
INFERENCE_INPUT_TABLE = os.environ.get(
    "ML_INFERENCE_INPUT_TABLE", "MARTS.ML_INFERENCE_INPUT"
)
STAGE_NAME = os.environ.get("SNOWFLAKE_STAGE", "RAW.S3_RAW_STAGE")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_export_invoked", event=event)

    # Per-run id for S3 prefix so concurrent pipelines don't collide
    run_id = event.get("loadResult", {}).get("Payload", {}).get("loadId") or \
             getattr(context, "aws_request_id", "local-test")

    s3_prefix_path = f"ml/input/{run_id}/"
    s3_prefix_url = f"s3://{RAW_BUCKET}/{s3_prefix_path}"

    # Local import so tests can stub SnowflakeClient without the connector installed
    from snowflake_client import SnowflakeClient, SnowflakeConfig

    config = SnowflakeConfig.from_ssm(SSM_PREFIX)
    with SnowflakeClient(config) as client:
        rows = client.unload_to_stage(
            table=INFERENCE_INPUT_TABLE,
            stage_name=STAGE_NAME,
            stage_path=s3_prefix_path,
        )

    if rows == 0:
        log_event(logger, "ml_export_no_rows")
        return {
            "skipped": True,
            "reason": "no rows in inference input table",
            "s3InputPrefix": s3_prefix_url,
            "rowsExported": 0,
        }

    summary = {
        "skipped": False,
        "s3InputPrefix": s3_prefix_url,
        "rowsExported": rows,
        "runId": run_id,
    }
    log_event(logger, "ml_export_complete", **summary)
    return summary
