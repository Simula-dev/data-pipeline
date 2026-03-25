"""
Thin wrapper around snowflake-connector-python for the load Lambda.

Responsibilities:
  - Fetch credentials from SSM Parameter Store
  - Connect with sensible defaults (autocommit off, json result format)
  - Execute COPY INTO against the RAW.LANDING table
  - Return rows-loaded count and detailed per-file status
"""

from __future__ import annotations

import os
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
    role: str | None = None

    @classmethod
    def from_ssm(cls, prefix: str) -> "SnowflakeConfig":
        """Load all Snowflake config from SSM Parameter Store."""
        names = [
            f"{prefix}/account",
            f"{prefix}/user",
            f"{prefix}/password",
            f"{prefix}/database",
            f"{prefix}/warehouse",
            f"{prefix}/schema",
        ]
        response = ssm.get_parameters(Names=names, WithDecryption=True)

        # Map short name \u2192 value
        values = {
            p["Name"].split("/")[-1]: p["Value"]
            for p in response["Parameters"]
        }

        missing = set(n.split("/")[-1] for n in names) - values.keys()
        if missing:
            raise ValueError(
                f"Missing SSM parameters under {prefix}: {sorted(missing)}"
            )

        # Optional role parameter
        try:
            role_resp = ssm.get_parameter(Name=f"{prefix}/role", WithDecryption=True)
            role = role_resp["Parameter"]["Value"]
        except ssm.exceptions.ParameterNotFound:
            role = None

        return cls(
            account=values["account"],
            user=values["user"],
            password=values["password"],
            database=values["database"],
            warehouse=values["warehouse"],
            schema=values["schema"],
            role=role,
        )


class SnowflakeClient:
    """Context-managed Snowflake connection."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn = None

    def __enter__(self) -> "SnowflakeClient":
        # Import inside __enter__ so unit tests can mock without the connector installed
        import snowflake.connector

        log_event(
            logger,
            "snowflake_connect",
            account=self.config.account,
            user=self.config.user,
            warehouse=self.config.warehouse,
            database=self.config.database,
        )
        self._conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            warehouse=self.config.warehouse,
            schema=self.config.schema,
            role=self.config.role,
            autocommit=True,
            client_session_keep_alive=False,
            session_parameters={
                "QUERY_TAG": "data-pipeline-loader",
            },
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def copy_into_landing(
        self,
        *,
        stage_name: str,
        file_format: str,
        s3_key: str,
        source: str,
        load_id: str,
    ) -> dict[str, Any]:
        """
        Execute COPY INTO RAW.LANDING for a single S3 NDJSON file.

        Args:
            stage_name: Fully-qualified stage, e.g. RAW.S3_RAW_STAGE
            file_format: Fully-qualified file format, e.g. RAW.NDJSON_FORMAT
            s3_key: S3 key relative to the stage root (from the ingest step)
            source: Logical source name (from the ingest step)
            load_id: Unique id for this load execution (Lambda request id)

        Returns:
            { "rowsLoaded": int, "filesLoaded": int, "status": "LOADED"|"PARTIAL"|"FAILED" }
        """
        assert self._conn is not None, "Call inside `with SnowflakeClient(...)`"

        sql = f"""
            COPY INTO {self.config.database}.RAW.LANDING (
                load_id, source, file_path, ingested_at, data
            )
            FROM (
                SELECT
                    %(load_id)s,
                    %(source)s,
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP(),
                    $1
                FROM @{self.config.database}.{stage_name}
                (FILE_FORMAT => '{self.config.database}.{file_format}')
            )
            FILES = (%(s3_key)s)
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
            FORCE = FALSE
        """

        params = {
            "load_id": load_id,
            "source": source,
            "s3_key": s3_key,
        }

        log_event(
            logger,
            "copy_into_execute",
            s3_key=s3_key,
            source=source,
            load_id=load_id,
        )

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            # COPY INTO result columns:
            # file, status, rows_parsed, rows_loaded, error_limit,
            # errors_seen, first_error, first_error_line, first_error_character,
            # first_error_column_name
            if not rows:
                return {"rowsLoaded": 0, "filesLoaded": 0, "status": "NO_FILES"}

            total_rows_loaded = sum(r[3] for r in rows)
            total_errors = sum(r[5] for r in rows)
            statuses = {r[1] for r in rows}

            if total_errors > 0:
                status = "PARTIAL"
            elif "LOADED" in statuses:
                status = "LOADED"
            else:
                status = "|".join(sorted(statuses))

            log_event(
                logger,
                "copy_into_complete",
                rows_loaded=total_rows_loaded,
                files_loaded=len(rows),
                errors=total_errors,
                status=status,
            )

            return {
                "rowsLoaded": int(total_rows_loaded),
                "filesLoaded": len(rows),
                "errors": int(total_errors),
                "status": status,
                "perFile": [
                    {
                        "file": r[0],
                        "status": r[1],
                        "rowsParsed": r[2],
                        "rowsLoaded": r[3],
                        "errors": r[5],
                    }
                    for r in rows
                ],
            }
        finally:
            cursor.close()
