"""
Load Lambda \u2014 executes Snowflake COPY INTO for an S3 file landed by the ingest step.

Event shape (from Step Functions):
  Entire state is passed; the ingest result lives at $.ingestResult.Payload.

  {
    "source_name": "...",        # from the original input
    "ingestResult": {
      "Payload": {
        "source": "...",
        "s3Bucket": "...",
        "s3Key": "raw/.../file.ndjson",
        "recordCount": 100
      }
    }
  }

Direct invocation (backfill / testing):
  { "source": "...", "s3Key": "raw/.../file.ndjson" }
"""

from __future__ import annotations

import os

from logger import get_logger, log_event
from snowflake_client import SnowflakeClient, SnowflakeConfig


logger = get_logger("load")

SSM_PREFIX = os.environ.get("SNOWFLAKE_PARAM_PREFIX", "/data-pipeline/snowflake")
STAGE_NAME = os.environ.get("SNOWFLAKE_STAGE", "RAW.S3_RAW_STAGE")
FILE_FORMAT = os.environ.get("SNOWFLAKE_FILE_FORMAT", "RAW.NDJSON_FORMAT")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "load_invoked", event=event)

    source, s3_key = _extract_ingest_result(event)
    if not s3_key:
        # Ingest step yielded zero records \u2014 nothing to load, skip gracefully.
        log_event(logger, "load_skipped_no_file", source=source)
        return {
            "status": "SKIPPED",
            "reason": "no s3Key in ingest result",
            "source": source,
            "rowsLoaded": 0,
        }

    load_id = getattr(context, "aws_request_id", "local-test")
    config = SnowflakeConfig.from_ssm(SSM_PREFIX)

    with SnowflakeClient(config) as client:
        result = client.copy_into_landing(
            stage_name=STAGE_NAME,
            file_format=FILE_FORMAT,
            s3_key=s3_key,
            source=source,
            load_id=load_id,
        )

    summary = {
        "source": source,
        "s3Key": s3_key,
        "loadId": load_id,
        **result,
    }
    log_event(logger, "load_complete", **summary)
    return summary


def _extract_ingest_result(event: dict) -> tuple[str, str | None]:
    """
    Pull source + s3Key from either a Step Functions nested payload
    or a flat direct-invocation event.
    """
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

    # Direct invocation fallback
    return (
        event.get("source", "unknown"),
        event.get("s3Key"),
    )
