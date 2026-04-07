"""
ML Load Lambda \u2014 COPY Batch Transform output CSV into marts.ml_predictions.

Uses Redshift COPY FROM S3. Similar temp-table pattern to the main load
Lambda: COPY into a staging temp table (single prediction column), then
INSERT into the target with load_id + timestamp.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

from logger import get_logger, log_event
from redshift_client import RedshiftClient, RedshiftError


logger = get_logger("ml_load")

REDSHIFT_S3_ROLE_ARN = os.environ["REDSHIFT_S3_ROLE_ARN"]
PREDICTIONS_TABLE = os.environ.get("ML_PREDICTIONS_TABLE", "marts.ml_predictions")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "ml_load_invoked", payload=event)

    s3_output_path = _extract_output_path(event)
    if not s3_output_path:
        return {"status": "SKIPPED", "reason": "no transform output path", "rowsLoaded": 0}

    run_id = (
        event.get("mlExport", {}).get("Payload", {}).get("runId")
        or getattr(context, "aws_request_id", "local-test")
    )

    client = RedshiftClient()

    statements = [
        "DROP TABLE IF EXISTS pred_stage;",
        "CREATE TEMP TABLE pred_stage (prediction VARCHAR(65535));",
        (
            f"COPY pred_stage (prediction) "
            f"FROM '{s3_output_path}' "
            f"IAM_ROLE '{REDSHIFT_S3_ROLE_ARN}' "
            f"FORMAT AS CSV IGNOREHEADER 1 "
            f"REGION AS '{AWS_REGION}';"
        ),
        (
            f"INSERT INTO {PREDICTIONS_TABLE} (load_id, predicted_at, prediction) "
            f"SELECT '{_quote(run_id)}', GETDATE(), prediction::SUPER "
            f"FROM pred_stage;"
        ),
        "DROP TABLE pred_stage;",
    ]

    try:
        _run_batch(client, statements)
    except RedshiftError as exc:
        log_event(
            logger,
            "ml_load_redshift_error",
            statement_id=exc.statement_id,
            status=exc.status,
            error=str(exc),
        )
        raise

    rows_loaded = _count_loaded_rows(client, PREDICTIONS_TABLE, run_id)

    summary = {
        "loadId": run_id,
        "s3OutputPath": s3_output_path,
        "rowsLoaded": rows_loaded,
        "filesLoaded": 1,
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


def _run_batch(client: RedshiftClient, statements: list[str]) -> None:
    """Execute statements in a single session so the TEMP TABLE is visible to COPY/INSERT."""
    import boto3
    import time
    rs = boto3.client("redshift-data")

    resp = rs.batch_execute_statement(
        WorkgroupName=client.workgroup,
        Database=client.database,
        Sqls=statements,
        WithEvent=False,
    )
    statement_id = resp["Id"]
    log_event(logger, "redshift_batch_submitted", id=statement_id, count=len(statements))

    deadline = time.monotonic() + client.default_timeout
    while time.monotonic() < deadline:
        desc = rs.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
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
    sql = f"SELECT COUNT(*) FROM {table} WHERE load_id = '{_quote(load_id)}'"
    return int(client.fetch_scalar(sql) or 0)


def _quote(value: str) -> str:
    return str(value).replace("'", "''")
