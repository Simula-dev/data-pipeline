"""
ML Export Lambda \u2014 UNLOAD marts.ml_inference_input to S3 as CSV.

Uses Redshift UNLOAD command which streams query results directly to S3
\u2014 no data transits through the Lambda. Returns the S3 prefix that
Batch Transform will read from.
"""

from __future__ import annotations

import os

from logger import get_logger, log_event
from redshift_client import RedshiftClient


logger = get_logger("ml_export")

RAW_BUCKET = os.environ["RAW_BUCKET"]
REDSHIFT_S3_ROLE_ARN = os.environ["REDSHIFT_S3_ROLE_ARN"]
ML_INFERENCE_INPUT_TABLE = os.environ.get(
    "ML_INFERENCE_INPUT_TABLE", "marts.ml_inference_input"
)
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_export_invoked", payload=event)

    run_id = (
        event.get("loadResult", {}).get("Payload", {}).get("loadId")
        or getattr(context, "aws_request_id", "local-test")
    )

    s3_prefix_path = f"ml/input/{run_id}/"
    s3_prefix_url = f"s3://{RAW_BUCKET}/{s3_prefix_path}"

    client = RedshiftClient()

    # UNLOAD writes CSV files directly to S3 using the Redshift-attached IAM role
    unload_sql = f"""
        UNLOAD ('SELECT * FROM {ML_INFERENCE_INPUT_TABLE}')
        TO '{s3_prefix_url}'
        IAM_ROLE '{REDSHIFT_S3_ROLE_ARN}'
        FORMAT CSV
        HEADER
        PARALLEL OFF
        ALLOWOVERWRITE
        REGION '{AWS_REGION}'
    """

    try:
        desc = client.execute(unload_sql)
    except Exception as exc:
        log_event(logger, "ml_export_unload_failed", error=str(exc))
        raise

    rows_exported = int(desc.get("ResultRows") or 0)

    if rows_exported == 0:
        log_event(logger, "ml_export_no_rows")
        return {
            "skipped": True,
            "reason": "no rows in inference input table",
            "s3InputPrefix": s3_prefix_url,
            "rowsExported": 0,
            "runId": run_id,
        }

    summary = {
        "skipped": False,
        "s3InputPrefix": s3_prefix_url,
        "rowsExported": rows_exported,
        "runId": run_id,
    }
    log_event(logger, "ml_export_complete", **summary)
    return summary
