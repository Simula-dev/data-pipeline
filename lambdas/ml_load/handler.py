"""
ML Load Lambda \u2014 COPY INTO Snowflake from the S3 prefix Batch Transform wrote to.

Event (from Step Functions, after SageMakerCreateTransformJob):
    {
        "mlExport": { "Payload": { "runId": "...", "s3InputPrefix": "s3://.../" } },
        "transformResult": { "TransformOutput": { "S3OutputPath": "s3://.../ml/output/<id>/" } }
    }

Writes rows into MARTS.ML_PREDICTIONS (load_id, predicted_at, input_row, prediction).
Returns a summary with rows loaded.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

import boto3

from logger import get_logger, log_event


logger = get_logger("ml_load")
ssm = boto3.client("ssm")

SSM_PREFIX = os.environ.get("SNOWFLAKE_PARAM_PREFIX", "/data-pipeline/snowflake")
STAGE_NAME = os.environ.get("SNOWFLAKE_STAGE", "RAW.S3_RAW_STAGE")
PREDICTIONS_TABLE = os.environ.get("ML_PREDICTIONS_TABLE", "MARTS.ML_PREDICTIONS")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_load_invoked", payload=event)

    s3_output_path = _extract_output_path(event)
    if not s3_output_path:
        return {"status": "SKIPPED", "reason": "no transform output path", "rowsLoaded": 0}

    run_id = (
        event.get("mlExport", {}).get("Payload", {}).get("runId")
        or getattr(context, "aws_request_id", "local-test")
    )

    # Convert s3://bucket/key/ to stage-relative path
    parsed = urlparse(s3_output_path)
    stage_rel_path = parsed.path.lstrip("/")

    from snowflake_client import SnowflakeClient, SnowflakeConfig

    config = SnowflakeConfig.from_ssm(SSM_PREFIX)
    with SnowflakeClient(config) as client:
        result = client.copy_predictions_into_marts(
            stage_name=STAGE_NAME,
            stage_path=stage_rel_path,
            target_table=PREDICTIONS_TABLE,
            load_id=run_id,
        )

    summary = {"loadId": run_id, "s3OutputPath": s3_output_path, **result}
    log_event(logger, "ml_load_complete", **summary)
    return summary


def _extract_output_path(event: dict) -> str | None:
    """Find the batch transform output path in the Step Functions state."""
    tr = event.get("transformResult")
    if isinstance(tr, dict):
        # Native SM task returns camel-cased fields
        output = tr.get("TransformOutput") or tr.get("transformOutput")
        if output:
            return output.get("S3OutputPath") or output.get("s3OutputPath")
    return None
