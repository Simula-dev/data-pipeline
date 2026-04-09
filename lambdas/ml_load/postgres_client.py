"""
PostgreSQL client using pg8000 (pure Python, no C extensions).

Reads credentials from AWS Secrets Manager. The secret is auto-generated
by CDK's DatabaseInstance construct and contains host, port, username,
password, dbname, and engine fields as JSON.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
import pg8000.native

from logger import get_logger, log_event

logger = get_logger(__name__)


class PostgresClient:
    """Context-managed PostgreSQL connection via pg8000."""

    def __init__(self, secret_name: str | None = None):
        self.secret_name = secret_name or os.environ.get(
            "RDS_SECRET_NAME", "data-pipeline/rds/admin"
        )
        self._conn = None

    def __enter__(self) -> "PostgresClient":
        creds = self._load_secret()
        log_event(
            logger,
            "postgres_connect",
            host=creds["host"],
            database=creds.get("dbname", "data_pipeline"),
        )
        self._conn = pg8000.native.Connection(
            host=creds["host"],
            port=int(creds.get("port", 5432)),
            user=creds["username"],
            password=creds["password"],
            database=creds.get("dbname", "data_pipeline"),
            ssl_context=True,
        )
        return self

    def __exit__(self, *exc) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        """Execute a statement (no result set expected)."""
        assert self._conn is not None
        log_event(logger, "postgres_execute", sql_preview=sql[:200])
        self._conn.run(sql, params or ())

    def execute_many(self, sql: str, params_list: list[tuple]) -> int:
        """Execute a parameterized statement for each set of params. Returns row count."""
        assert self._conn is not None
        count = 0
        for params in params_list:
            self._conn.run(sql, params)
            count += 1
        return count

    def fetch_scalar(self, sql: str, params: tuple | None = None) -> Any:
        """Execute a query and return the first column of the first row."""
        assert self._conn is not None
        rows = self._conn.run(sql, params or ())
        if not rows:
            return None
        return rows[0][0]

    def fetch_all(self, sql: str, params: tuple | None = None) -> list[tuple]:
        """Execute a query and return all rows."""
        assert self._conn is not None
        return self._conn.run(sql, params or ())

    def commit(self) -> None:
        assert self._conn is not None
        self._conn.run("COMMIT")

    def _load_secret(self) -> dict:
        sm = boto3.client("secretsmanager")
        resp = sm.get_secret_value(SecretId=self.secret_name)
        return json.loads(resp["SecretString"])
