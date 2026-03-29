"""
Read-only Snowflake client for quality checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3

from logger import get_logger, log_event

logger = get_logger(__name__)
ssm = boto3.client("ssm")


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str
    warehouse: str
    schema: str

    @classmethod
    def from_ssm(cls, prefix: str) -> "SnowflakeConfig":
        names = [
            f"{prefix}/account",
            f"{prefix}/user",
            f"{prefix}/password",
            f"{prefix}/database",
            f"{prefix}/warehouse",
            f"{prefix}/schema",
        ]
        resp = ssm.get_parameters(Names=names, WithDecryption=True)
        v = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
        return cls(
            account=v["account"],
            user=v["user"],
            password=v["password"],
            database=v["database"],
            warehouse=v["warehouse"],
            schema=v["schema"],
        )


class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn = None

    def __enter__(self) -> "SnowflakeClient":
        import snowflake.connector
        log_event(logger, "snowflake_connect", account=self.config.account)
        self._conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            warehouse=self.config.warehouse,
            schema=self.config.schema,
            role="TRANSFORMER",
            autocommit=True,
            session_parameters={"QUERY_TAG": "data-pipeline-quality-gate"},
        )
        return self

    def __exit__(self, *exc) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def fetch_scalar(self, sql: str, params: dict | None = None) -> Any:
        """Execute a query expected to return a single row/column and return the value."""
        assert self._conn is not None
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params or {})
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()

    def fetch_all(self, sql: str, params: dict | None = None) -> list[tuple]:
        assert self._conn is not None
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params or {})
            return cursor.fetchall()
        finally:
            cursor.close()
