"""
Ingestion Lambda — entry point.

Invoked by Step Functions with a source configuration payload.
Fetches data via HTTP, writes NDJSON to the raw S3 bucket, and returns
a summary that downstream steps use to find the landed data.

Example event:
    {
        "source_name": "jsonplaceholder_posts",
        "base_url": "https://jsonplaceholder.typicode.com",
        "endpoint": "/posts",
        "pagination_type": "none",
        "records_json_path": ""
    }
"""

from __future__ import annotations

import os
import boto3

from config import SourceConfig
from http_client import HttpClient, HttpError
from logger import get_logger, log_event
from s3_writer import S3RawWriter

logger = get_logger("ingest")
ssm = boto3.client("ssm")

RAW_BUCKET = os.environ["RAW_BUCKET"]


def lambda_handler(event: dict, context) -> dict:
    """
    Main entry point. Must be idempotent per (source_name, date) where possible.
    Returns a summary dict consumed by the next Step Functions state.
    """
    log_event(logger, "ingest_invoked", event=event)

    try:
        config = SourceConfig.from_event(event)
    except (ValueError, KeyError) as exc:
        log_event(logger, "ingest_config_error", error=str(exc))
        raise

    auth_secret = _load_auth_secret(config.auth_secret_ssm)
    client = HttpClient(config, auth_secret=auth_secret)
    writer = S3RawWriter(bucket=RAW_BUCKET)

    run_id = getattr(context, "aws_request_id", None)

    try:
        summary = writer.write_records(
            source_name=config.source_name,
            records=client.iter_records(),
            run_id=run_id,
        )
    except HttpError as exc:
        log_event(
            logger,
            "ingest_http_error",
            status=exc.status,
            url=exc.url,
            body=exc.body,
        )
        raise

    log_event(logger, "ingest_complete", **summary)
    return summary


def _load_auth_secret(ssm_param: str | None) -> str | None:
    """Fetch auth secret from SSM Parameter Store (SecureString)."""
    if not ssm_param:
        return None
    response = ssm.get_parameter(Name=ssm_param, WithDecryption=True)
    return response["Parameter"]["Value"]
