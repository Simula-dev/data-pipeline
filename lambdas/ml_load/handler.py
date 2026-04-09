"""
ML Load Lambda \u2014 read Batch Transform output CSV from S3, INSERT into
marts.ml_predictions in PostgreSQL.
"""

from __future__ import annotations

import csv
import io
import json
import os
from urllib.parse import urlparse

import boto3

from logger import get_logger, log_event
from postgres_client import PostgresClient


logger = get_logger("ml_load")
s3_client = boto3.client("s3")

PREDICTIONS_TABLE = os.environ.get("ML_PREDICTIONS_TABLE", "marts.ml_predictions")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_load_invoked", payload=event)

    s3_output_path = _extract_output_path(event)
    if not s3_output_path:
        return {"status": "SKIPPED", "reason": "no transform output path", "rowsLoaded": 0}

    run_id = (
        event.get("mlExport", {}).get("Payload", {}).get("runId")
        or getattr(context, "aws_request_id", "local-test")
    )

    # Parse S3 URI and list objects under the prefix
    parsed = urlparse(s3_output_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".out")]

    if not keys:
        return {"status": "SKIPPED", "reason": "no .out files in transform output", "rowsLoaded": 0}

    total_loaded = 0
    with PostgresClient() as client:
        for key in keys:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read().decode("utf-8")
            reader = csv.reader(io.StringIO(body))
            # Skip header if present
            rows = list(reader)
            if rows and rows[0] and rows[0][0] == "prediction":
                rows = rows[1:]

            for row in rows:
                prediction_value = row[0] if row else None
                if prediction_value is not None:
                    client.execute(
                        f"INSERT INTO {PREDICTIONS_TABLE} (load_id, predicted_at, prediction) "
                        f"VALUES (:p1, NOW(), CAST(:p2 AS jsonb))",
                        (run_id, json.dumps(prediction_value)),
                    )
                    total_loaded += 1
        client.commit()

    summary = {
        "loadId": run_id,
        "s3OutputPath": s3_output_path,
        "rowsLoaded": total_loaded,
        "filesLoaded": len(keys),
        "status": "LOADED",
    }
    log_event(logger, "ml_load_complete", **summary)
    return summary


def _extract_output_path(event: dict) -> str | None:
    tr = event.get("transformResult")
    if isinstance(tr, dict):
        output = tr.get("TransformOutput") or tr.get("transformOutput")
        if output:
            return output.get("S3OutputPath") or output.get("s3OutputPath")
    return None
