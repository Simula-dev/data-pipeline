"""
ML Export Lambda \u2014 SELECT from marts.ml_inference_input, write CSV to S3.

Unlike the Redshift version (which used UNLOAD), PostgreSQL doesn't have a
native UNLOAD-to-S3 command. The Lambda SELECTs the data, writes a CSV file
to S3, and returns the S3 prefix for Batch Transform to read.
"""

from __future__ import annotations

import csv
import io
import os

import boto3

from logger import get_logger, log_event
from postgres_client import PostgresClient


logger = get_logger("ml_export")
s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
ML_INFERENCE_INPUT_TABLE = os.environ.get(
    "ML_INFERENCE_INPUT_TABLE", "marts.ml_inference_input"
)


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_export_invoked", payload=event)

    run_id = (
        event.get("loadResult", {}).get("Payload", {}).get("loadId")
        or getattr(context, "aws_request_id", "local-test")
    )

    s3_key = f"ml/input/{run_id}/data.csv"
    s3_prefix_url = f"s3://{RAW_BUCKET}/ml/input/{run_id}/"

    with PostgresClient() as client:
        rows = client.fetch_all(f"SELECT * FROM {ML_INFERENCE_INPUT_TABLE}")

        if not rows:
            log_event(logger, "ml_export_no_rows")
            return {
                "skipped": True,
                "reason": "no rows in inference input table",
                "s3InputPrefix": s3_prefix_url,
                "rowsExported": 0,
                "runId": run_id,
            }

        # Get column names from a LIMIT 0 query description
        col_rows = client.fetch_all(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema || '.' || table_name = :p1 "
            f"ORDER BY ordinal_position",
            (ML_INFERENCE_INPUT_TABLE,),
        )
        columns = [r[0] for r in col_rows] if col_rows else [f"col_{i}" for i in range(len(rows[0]))]

    # Write CSV to S3
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    summary = {
        "skipped": False,
        "s3InputPrefix": s3_prefix_url,
        "rowsExported": len(rows),
        "runId": run_id,
    }
    log_event(logger, "ml_export_complete", **summary)
    return summary
