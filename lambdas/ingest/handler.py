"""
Ingest Lambda — pulls data from a source and writes raw files to S3.

Replace the stub body with your actual source logic
(REST API call, database export, file transfer, etc.).
"""

import json
import os
import boto3
from datetime import datetime, timezone


s3 = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]


def lambda_handler(event: dict, context) -> dict:
    """
    Expected event shape:
    {
        "source": "my_api",        # logical source name — used as S3 prefix
        "params": { ... }          # source-specific query parameters
    }
    """
    source = event.get("source", "unknown")
    params = event.get("params", {})

    # TODO: replace with real data pull
    raw_data = _fetch_source(source, params)

    # Write to S3: raw/<source>/YYYY/MM/DD/<timestamp>.json
    now = datetime.now(timezone.utc)
    s3_key = (
        f"raw/{source}/"
        f"{now.year}/{now.month:02d}/{now.day:02d}/"
        f"{now.strftime('%H%M%S')}.json"
    )

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=json.dumps(raw_data),
        ContentType="application/json",
    )

    return {
        "statusCode": 200,
        "s3Key": s3_key,
        "source": source,
        "recordCount": len(raw_data) if isinstance(raw_data, list) else 1,
    }


def _fetch_source(source: str, params: dict) -> list | dict:
    """Stub — replace with real extraction logic per source."""
    print(f"Fetching source={source} params={params}")
    return [{"stub": True, "source": source}]
