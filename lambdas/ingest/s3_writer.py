"""
S3 writer — idempotent, partitioned raw-zone writes.

Key format:
    raw/<source_name>/YYYY/MM/DD/<timestamp>_<content_hash>.ndjson

Writing NDJSON (newline-delimited JSON) instead of a single JSON array
makes Snowflake COPY INTO and line-based tooling (jq, grep) much easier.
"""

from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime, timezone
from typing import Iterable

import boto3

from logger import get_logger, log_event

logger = get_logger(__name__)


class S3RawWriter:
    """Writes iterables of records to the raw S3 bucket as NDJSON."""

    def __init__(self, bucket: str, s3_client=None):
        self.bucket = bucket
        self.s3 = s3_client or boto3.client("s3")

    def write_records(
        self,
        source_name: str,
        records: Iterable[dict],
        run_id: str | None = None,
    ) -> dict:
        """
        Buffer records to NDJSON in memory and upload atomically.

        Returns a summary dict used as Step Functions output.
        """
        buffer = io.BytesIO()
        hasher = hashlib.sha256()
        count = 0

        for record in records:
            line = (json.dumps(record, default=str) + "\n").encode("utf-8")
            buffer.write(line)
            hasher.update(line)
            count += 1

        if count == 0:
            log_event(logger, "ingest_no_records", source=source_name)
            return {
                "source": source_name,
                "recordCount": 0,
                "s3Key": None,
                "s3Bucket": self.bucket,
            }

        content_hash = hasher.hexdigest()[:16]
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%H%M%S")
        run_suffix = f"_{run_id}" if run_id else ""

        s3_key = (
            f"raw/{source_name}/"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{timestamp}_{content_hash}{run_suffix}.ndjson"
        )

        buffer.seek(0)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/x-ndjson",
            Metadata={
                "source": source_name,
                "record_count": str(count),
                "content_hash": content_hash,
                "ingested_at": now.isoformat(),
            },
        )

        log_event(
            logger,
            "ingest_s3_write_complete",
            source=source_name,
            bucket=self.bucket,
            key=s3_key,
            record_count=count,
            bytes=buffer.tell(),
        )

        return {
            "source": source_name,
            "recordCount": count,
            "s3Bucket": self.bucket,
            "s3Key": s3_key,
            "contentHash": content_hash,
        }
