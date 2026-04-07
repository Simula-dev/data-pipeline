"""
Load Lambda \u2014 loads an NDJSON file from S3 into Redshift raw.landing.

Redshift COPY can't inject constant columns directly, so we use a
temporary table pattern:
  1. CREATE TEMP TABLE stage_data (data SUPER)
  2. COPY FROM S3 ... FORMAT AS JSON 'auto ignorecase'
  3. INSERT INTO raw.landing (load_id, source, file_path, ingested_at, data)
       SELECT <load_id>, <source>, <file_path>, GETDATE(), data FROM stage_data
  4. DROP TABLE stage_data

All four statements run in a single Data API session via sequential
execute_statement calls \u2014 they share the same session because Redshift
Data API tracks temp tables per connection.

Event shape (Step Functions nested):
    { "ingestResult": { "Payload": { "source": ..., "s3Key": ..., "recordCount": N } } }

Or direct invocation:
    { "source": "...", "s3Key": "..." }
"""

from __future__ import annotations

import os

from logger import get_logger, log_event
from redshift_client import RedshiftClient, RedshiftError


logger = get_logger("load")

RAW_BUCKET = os.environ["RAW_BUCKET"]
REDSHIFT_S3_ROLE_ARN = os.environ["REDSHIFT_S3_ROLE_ARN"]
RAW_SCHEMA = os.environ.get("REDSHIFT_RAW_SCHEMA", "raw")
LANDING_TABLE = os.environ.get("REDSHIFT_LANDING_TABLE", "landing")


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
    s3_uri = f"s3://{RAW_BUCKET}/{s3_key}"
    full_table = f"{RAW_SCHEMA}.{LANDING_TABLE}"

    # Redshift COPY + metadata injection via temp table
    client = RedshiftClient()

    # The CREATE TEMP TABLE persists only within a single Data API session.
    # We pass session_id across calls by running everything as one batched
    # statement using batch-execute-statement \u2014 but batch doesn't support
    # COPY. Instead we use a single statement with a session (via sql_list).
    #
    # Simpler approach: use batch_execute_statement which shares a session.
    statements = [
        "DROP TABLE IF EXISTS load_stage;",
        "CREATE TEMP TABLE load_stage (data SUPER);",
        (
            f"COPY load_stage (data) FROM '{s3_uri}' "
            f"IAM_ROLE '{REDSHIFT_S3_ROLE_ARN}' "
            f"FORMAT AS JSON 'auto ignorecase' "
            f"REGION AS '{os.environ.get('AWS_REGION', 'us-east-1')}';"
        ),
        (
            f"INSERT INTO {full_table} (load_id, source, file_path, ingested_at, data) "
            f"SELECT '{_quote(load_id)}', '{_quote(source)}', '{_quote(s3_key)}', "
            f"GETDATE(), data FROM load_stage;"
        ),
        "DROP TABLE load_stage;",
    ]

    try:
        _run_batch(client, statements)
        rows_loaded = _count_loaded_rows(client, full_table, load_id)
    except RedshiftError as exc:
        log_event(
            logger,
            "load_redshift_error",
            statement_id=exc.statement_id,
            status=exc.status,
            error=str(exc),
        )
        raise

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
    """Pull source + s3Key from either a Step Functions nested payload or flat direct invocation."""
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


def _run_batch(client: RedshiftClient, statements: list[str]) -> None:
    """
    Execute multiple statements in a single shared session so the temp
    table created by CREATE TEMP TABLE is visible to the COPY/INSERT.
    """
    import boto3
    rs = boto3.client("redshift-data")

    resp = rs.batch_execute_statement(
        WorkgroupName=client.workgroup,
        Database=client.database,
        Sqls=statements,
        WithEvent=False,
    )
    statement_id = resp["Id"]
    log_event(logger, "redshift_batch_submitted", id=statement_id, count=len(statements))

    # Poll the parent statement id for completion of all sub-statements
    import time
    deadline = time.monotonic() + client.default_timeout
    while time.monotonic() < deadline:
        desc = rs.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            log_event(logger, "redshift_batch_finished", id=statement_id)
            return
        if status in ("FAILED", "ABORTED"):
            raise RedshiftError(
                f"Batch {status}: {desc.get('Error', 'no details')}",
                statement_id=statement_id,
                status=status,
                details=desc,
            )
        time.sleep(client.poll_interval)
    raise TimeoutError(f"Batch {statement_id} did not finish in {client.default_timeout}s")


def _count_loaded_rows(client: RedshiftClient, table: str, load_id: str) -> int:
    """Count rows inserted for this specific load_id \u2014 cheap verification query."""
    sql = f"SELECT COUNT(*) FROM {table} WHERE load_id = '{_quote(load_id)}'"
    count = client.fetch_scalar(sql)
    return int(count or 0)


def _quote(value: str) -> str:
    """Escape single quotes for safe SQL literal interpolation."""
    return str(value).replace("'", "''")
