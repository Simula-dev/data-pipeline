"""
Snowflake client for ml_load Lambda \u2014 COPY INTO MARTS.ML_PREDICTIONS.
"""

from __future__ import annotations

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
            session_parameters={"QUERY_TAG": "data-pipeline-ml-load"},
        )
        return self

    def __exit__(self, *exc) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def copy_predictions_into_marts(
        self,
        *,
        stage_name: str,
        stage_path: str,
        target_table: str,
        load_id: str,
    ) -> dict:
        """
        Read the Batch Transform output CSV from the stage and insert
        predictions into MARTS.ML_PREDICTIONS.
        """
        assert self._conn is not None

        full_stage = f"@{self.config.database}.{stage_name}/{stage_path.lstrip('/')}"
        full_table = f"{self.config.database}.{target_table}"

        sql = f"""
            COPY INTO {full_table} (load_id, predicted_at, prediction)
            FROM (
                SELECT
                    %(load_id)s,
                    CURRENT_TIMESTAMP(),
                    $1
                FROM {full_stage}
                (FILE_FORMAT => (
                    TYPE = CSV
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ))
            )
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
            FORCE = FALSE
        """

        log_event(logger, "copy_predictions_execute", stage=full_stage, table=full_table)

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, {"load_id": load_id})
            rows = cursor.fetchall()
            if not rows:
                return {"rowsLoaded": 0, "filesLoaded": 0, "status": "NO_FILES"}

            total_rows_loaded = sum(r[3] for r in rows)
            return {
                "rowsLoaded": int(total_rows_loaded),
                "filesLoaded": len(rows),
                "status": "LOADED",
            }
        finally:
            cursor.close()
