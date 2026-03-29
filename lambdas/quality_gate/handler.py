"""
Data Quality Gate Lambda.

Runs the checks defined in checks.json against Snowflake. Returns a summary
dict used by Step Functions to decide whether to route to NotifySuccess or
NotifyFailure:

    {
        "passed": bool,          # true if zero error-severity failures
        "errorCount": int,
        "warnCount": int,
        "checks": [ { name, type, severity, passed, details, error }, ... ]
    }

Warning-severity failures do NOT block the pipeline. Only error-severity
failures flip `passed` to false.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from check_runner import CheckResult, run_check
from logger import get_logger, log_event


logger = get_logger("quality_gate")

SSM_PREFIX = os.environ.get("SNOWFLAKE_PARAM_PREFIX", "/data-pipeline/snowflake")
CHECKS_PATH = Path(__file__).parent / "checks.json"


def lambda_handler(event: dict, context) -> dict:
    log_event(logger, "quality_gate_invoked", event=event)

    checks = _load_checks(CHECKS_PATH)
    log_event(logger, "checks_loaded", count=len(checks))

    # Local import so tests can stub without the connector installed
    from snowflake_client import SnowflakeClient, SnowflakeConfig

    config = SnowflakeConfig.from_ssm(SSM_PREFIX)
    results: list[CheckResult] = []

    with SnowflakeClient(config) as client:
        for check in checks:
            result = run_check(client, check)
            results.append(result)
            log_event(
                logger,
                "check_completed",
                name=result.name,
                passed=result.passed,
                severity=result.severity,
                error=result.error,
            )

    error_failures = [r for r in results if not r.passed and r.severity == "error"]
    warn_failures  = [r for r in results if not r.passed and r.severity == "warn"]

    summary = {
        "passed": len(error_failures) == 0,
        "totalChecks": len(results),
        "errorCount": len(error_failures),
        "warnCount": len(warn_failures),
        "checks": [r.to_dict() for r in results],
    }

    log_event(
        logger,
        "quality_gate_complete",
        passed=summary["passed"],
        errorCount=summary["errorCount"],
        warnCount=summary["warnCount"],
    )
    return summary


def _load_checks(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"checks.json not found at {path}")
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"checks.json must be a JSON array, got {type(data).__name__}")
    return data
