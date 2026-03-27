"""
Snowflake client for the ML export Lambda.

Extends the pattern from lambdas/load/snowflake_client.py with an
`unload_to_stage` method that uses Snowflake COPY INTO @stage (reverse of
the load direction) to write query results as CSV directly to S3.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

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
        values = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
        return cls(
            account=values["account"],
            user=values["user"],
            password=values["password"],
            database=values["database"],
            warehouse=values["warehouse"],
            schema=values["schema"],
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
            session_parameters={"QUERY_TAG": "data-pipeline-ml-export"},
        )
        return self

    def __exit__(self, *exc) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def unload_to_stage(
        self,
        *,
        table: str,
        stage_name: str,
        stage_path: str,
    ) -> int:
        """
        COPY the contents of `table` into @stage/stage_path/ as a single CSV.
        Returns the number of rows unloaded.
        """
        assert self._conn is not None

        full_table = f"{self.config.database}.{table}"
        full_stage = f"@{self.config.database}.{stage_name}/{stage_path.lstrip('/')}"

        sql = f"""
            COPY INTO {full_stage}
            FROM {full_table}
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = NONE)
            HEADER = TRUE
            OVERWRITE = TRUE
            SINGLE = TRUE
            MAX_FILE_SIZE = 5368709120
        """

        log_event(logger, "unload_execute", table=full_table, stage=full_stage)
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            # Result: rows_unloaded, input_bytes, output_bytes
            if not rows:
                return 0
            rows_unloaded = int(rows[0][0])
            log_event(logger, "unload_complete", rows_unloaded=rows_unloaded)
            return rows_unloaded
        finally:
            cursor.close()
