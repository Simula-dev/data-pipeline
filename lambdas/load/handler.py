"""
Load Lambda \u2014 reads NDJSON from S3 and batch-INSERTs into raw.landing (PostgreSQL).

Unlike the Redshift version (which uses COPY FROM S3 via Data API), RDS
PostgreSQL requires the Lambda to read the S3 object, parse each JSON line,
and INSERT via pg8000. This is fine for the data volumes we handle (API
responses, typically <100K rows per file).

Event shape (Step Functions nested):
    { "ingestResult": { "Payload": { "source": ..., "s3Key": ..., "recordCount": N } } }

Or direct invocation:
    { "source": "...", "s3Key": "..." }
"""

from __future__ import annotations

import json
import os

import boto3

from logger import get_logger, log_event
from postgres_client import PostgresClient


logger = get_logger("load")
s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_SCHEMA = os.environ.get("POSTGRES_RAW_SCHEMA", "raw")
LANDING_TABLE = os.environ.get("POSTGRES_LANDING_TABLE", "landing")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "load_invoked", payload=event)

    source, s3_key = _extract_ingest_result(event)
    if not s3_key:
        log_event(logger, "load_skipped_no_file", source=source)
        return {
            "status": "SKIPPED",
            "reason": "no s3Key in ingest result",
            "source": source,
            "rowsLoaded": 0,
        }

    load_id = getattr(context, "aws_request_id", "local-test")

    # Read NDJSON from S3
    log_event(logger, "load_reading_s3", bucket=RAW_BUCKET, key=s3_key)
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    body = obj["Body"].read().decode("utf-8")
    records = [json.loads(line) for line in body.strip().split("\n") if line.strip()]
    log_event(logger, "load_parsed_records", count=len(records))

    if not records:
        return {
            "status": "SKIPPED",
            "reason": "file contained zero records",
            "source": source,
            "rowsLoaded": 0,
        }

    # Batch INSERT into raw.landing
    insert_sql = (
        f"INSERT INTO {RAW_SCHEMA}.{LANDING_TABLE} "
        f"(load_id, source, file_path, ingested_at, data) "
        f"VALUES (:p1, :p2, :p3, NOW(), CAST(:p4 AS jsonb))"
    )

    with PostgresClient() as client:
        params_list = [
            (load_id, source, s3_key, json.dumps(record))
            for record in records
        ]
        rows_loaded = client.execute_many(insert_sql, params_list)
        client.commit()

    summary = {
        "source": source,
        "s3Key": s3_key,
        "loadId": load_id,
        "rowsLoaded": rows_loaded,
        "filesLoaded": 1,
        "status": "LOADED",
    }
    log_event(logger, "load_complete", **summary)
    return summary


def _extract_ingest_result(event: dict) -> tuple[str, str | None]:
    ingest_payload = (
        event.get("ingestResult", {}).get("Payload")
        if isinstance(event.get("ingestResult"), dict)
        else None
    )
    if ingest_payload:
        return (
            ingest_payload.get("source", "unknown"),
            ingest_payload.get("s3Key"),
        )
    return event.get("source", "unknown"), event.get("s3Key")
