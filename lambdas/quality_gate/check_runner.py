"""
Quality check runner.

Each supported check type is implemented as a function taking a check
definition dict + SnowflakeClient and returning a CheckResult.

New check types are added by registering a function in CHECK_HANDLERS.
"""

from __future__ import annotations

import operator
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from logger import get_logger, log_event
from snowflake_client import SnowflakeClient

logger = get_logger(__name__)


@dataclass
class CheckResult:
    name: str
    type: str
    severity: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
#  Check implementations                                                       #
# --------------------------------------------------------------------------- #

def _row_count_min(check: dict, client: SnowflakeClient) -> CheckResult:
    table = check["table"]
    min_rows = int(check.get("min_rows", 1))

    actual = client.fetch_scalar(f"SELECT COUNT(*) FROM {table}")
    actual = int(actual or 0)

    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "error"),
        passed=actual >= min_rows,
        details={"actual_rows": actual, "min_rows": min_rows, "table": table},
    )


def _freshness(check: dict, client: SnowflakeClient) -> CheckResult:
    table = check["table"]
    ts_col = check.get("timestamp_column", "ingested_at")
    max_age_hours = int(check.get("max_age_hours", 24))

    sql = (
        f"SELECT TIMESTAMPDIFF('hour', MAX({ts_col}), CURRENT_TIMESTAMP()) "
        f"FROM {table}"
    )
    hours_since_last = client.fetch_scalar(sql)

    if hours_since_last is None:
        return CheckResult(
            name=check["name"],
            type=check["type"],
            severity=check.get("severity", "error"),
            passed=False,
            details={"table": table, "reason": "table empty or timestamp all null"},
        )

    hours_since_last = int(hours_since_last)
    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "error"),
        passed=hours_since_last <= max_age_hours,
        details={
            "table": table,
            "timestamp_column": ts_col,
            "hours_since_last": hours_since_last,
            "max_age_hours": max_age_hours,
        },
    )


def _null_rate(check: dict, client: SnowflakeClient) -> CheckResult:
    table = check["table"]
    column = check["column"]
    max_rate = float(check.get("max_null_rate", 0.0))

    sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT_IF({column} IS NULL) AS null_rows
        FROM {table}
    """
    rows = client.fetch_all(sql)
    total, nulls = int(rows[0][0]), int(rows[0][1])
    null_rate = (nulls / total) if total > 0 else 0.0

    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "error"),
        passed=null_rate <= max_rate,
        details={
            "table": table,
            "column": column,
            "total_rows": total,
            "null_rows": nulls,
            "null_rate": round(null_rate, 6),
            "max_null_rate": max_rate,
        },
    )


def _uniqueness(check: dict, client: SnowflakeClient) -> CheckResult:
    table = check["table"]
    columns = check["columns"]
    cols_csv = ", ".join(columns)

    sql = f"""
        SELECT COUNT(*) AS dup_groups
        FROM (
            SELECT {cols_csv}, COUNT(*) AS c
            FROM {table}
            GROUP BY {cols_csv}
            HAVING COUNT(*) > 1
        )
    """
    dup_groups = int(client.fetch_scalar(sql) or 0)

    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "error"),
        passed=dup_groups == 0,
        details={
            "table": table,
            "columns": columns,
            "duplicate_groups": dup_groups,
        },
    )


# Comparison operators for custom_sql checks
_COMPARISONS: dict[str, Callable[[Any, Any], bool]] = {
    "eq":  operator.eq,
    "ne":  operator.ne,
    "lt":  operator.lt,
    "lte": operator.le,
    "gt":  operator.gt,
    "gte": operator.ge,
}


def _custom_sql(check: dict, client: SnowflakeClient) -> CheckResult:
    sql = check["sql"]
    comparison = check.get("comparison", "eq")
    expected = check.get("expected_value", 0)

    actual = client.fetch_scalar(sql)

    comp_fn = _COMPARISONS.get(comparison)
    if comp_fn is None:
        return CheckResult(
            name=check["name"],
            type=check["type"],
            severity=check.get("severity", "error"),
            passed=False,
            error=f"Unknown comparison operator: {comparison}",
        )

    try:
        passed = comp_fn(actual, expected)
    except TypeError as exc:
        return CheckResult(
            name=check["name"],
            type=check["type"],
            severity=check.get("severity", "error"),
            passed=False,
            error=f"Comparison failed: {exc}",
            details={"actual": actual, "expected": expected},
        )

    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "error"),
        passed=passed,
        details={
            "actual": actual,
            "expected": expected,
            "comparison": comparison,
        },
    )


CHECK_HANDLERS: dict[str, Callable[[dict, SnowflakeClient], CheckResult]] = {
    "row_count_min": _row_count_min,
    "freshness": _freshness,
    "null_rate": _null_rate,
    "uniqueness": _uniqueness,
    "custom_sql": _custom_sql,
}


def run_check(client: SnowflakeClient, check: dict) -> CheckResult:
    """Dispatch to the appropriate handler, wrapping unexpected errors."""
    check_type = check.get("type")
    handler = CHECK_HANDLERS.get(check_type)

    if handler is None:
        return CheckResult(
            name=check.get("name", "<unnamed>"),
            type=check_type or "<unknown>",
            severity=check.get("severity", "error"),
            passed=False,
            error=f"Unknown check type: {check_type}",
        )

    try:
        return handler(check, client)
    except Exception as exc:  # noqa: BLE001
        log_event(logger, "check_execution_error", name=check.get("name"), error=str(exc))
        return CheckResult(
            name=check.get("name", "<unnamed>"),
            type=check_type,
            severity=check.get("severity", "error"),
            passed=False,
            error=str(exc),
        )
