"""
Redshift Data API wrapper.

Uses the `redshift-data` boto3 service (available in Lambda runtime without
any extra dependencies). Handles async execute-poll-fetch for synchronous
callers and unwraps the Data API's typed cell format.
"""

from __future__ import annotations

import os
import time
from typing import Any

import boto3

from logger import get_logger, log_event

logger = get_logger(__name__)
_rs = boto3.client("redshift-data")


class RedshiftClient:
    """
    Synchronous wrapper around the Redshift Data API.

    Environment variables read at construction time:
        REDSHIFT_WORKGROUP   \u2014 Redshift Serverless workgroup name
        REDSHIFT_DATABASE    \u2014 target database
    """

    def __init__(self, poll_interval: float = 1.0, default_timeout: int = 600):
        self.workgroup = os.environ["REDSHIFT_WORKGROUP"]
        self.database = os.environ["REDSHIFT_DATABASE"]
        self.poll_interval = poll_interval
        self.default_timeout = default_timeout

    # ------------------------------------------------------------------ #
    #  Core execute / poll                                                #
    # ------------------------------------------------------------------ #
    def execute(
        self,
        sql: str,
        *,
        parameters: list[dict] | None = None,
        wait: bool = True,
        timeout: int | None = None,
    ) -> dict:
        """
        Submit a SQL statement. If wait=True, block until it finishes and
        return the describe_statement response.
        """
        log_event(logger, "redshift_execute", sql_preview=sql[:200])
        resp = _rs.execute_statement(
            WorkgroupName=self.workgroup,
            Database=self.database,
            Sql=sql,
            Parameters=parameters or [],
            WithEvent=False,
        )
        statement_id = resp["Id"]

        if not wait:
            return {"Id": statement_id, "Status": "SUBMITTED"}

        return self._wait(statement_id, timeout or self.default_timeout)

    def _wait(self, statement_id: str, timeout: int) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            desc = _rs.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == "FINISHED":
                log_event(
                    logger,
                    "redshift_finished",
                    id=statement_id,
                    duration_ms=desc.get("Duration", 0) // 1_000_000,
                    rows=desc.get("ResultRows"),
                )
                return desc
            if status in ("FAILED", "ABORTED"):
                raise RedshiftError(
                    f"Statement {status}: {desc.get('Error', 'no details')}",
                    statement_id=statement_id,
                    status=status,
                    details=desc,
                )
            time.sleep(self.poll_interval)
        raise TimeoutError(f"Redshift statement {statement_id} did not finish in {timeout}s")

    # ------------------------------------------------------------------ #
    #  Convenience fetchers                                               #
    # ------------------------------------------------------------------ #
    def fetch_scalar(self, sql: str, parameters: list[dict] | None = None) -> Any:
        desc = self.execute(sql, parameters=parameters)
        if not desc.get("HasResultSet"):
            return None
        result = _rs.get_statement_result(Id=desc["Id"])
        records = result.get("Records", [])
        if not records or not records[0]:
            return None
        return _unwrap_cell(records[0][0])

    def fetch_all(self, sql: str, parameters: list[dict] | None = None) -> list[tuple]:
        desc = self.execute(sql, parameters=parameters)
        if not desc.get("HasResultSet"):
            return []
        result = _rs.get_statement_result(Id=desc["Id"])
        return [
            tuple(_unwrap_cell(cell) for cell in row)
            for row in result.get("Records", [])
        ]


class RedshiftError(Exception):
    def __init__(self, message: str, *, statement_id: str, status: str, details: dict):
        super().__init__(message)
        self.statement_id = statement_id
        self.status = status
        self.details = details


def _unwrap_cell(cell: dict) -> Any:
    """
    Data API returns cells as dicts with a single type key:
        {'stringValue': 'foo'}
        {'longValue': 42}
        {'doubleValue': 3.14}
        {'booleanValue': True}
        {'isNull': True}
    """
    if not cell or cell.get("isNull"):
        return None
    for key, value in cell.items():
        if key != "isNull":
            return value
    return None
