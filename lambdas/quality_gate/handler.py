"""
Quality Gate Lambda — queries Snowflake mart tables and validates data quality thresholds.

Returns passed=True if all checks pass, passed=False otherwise.
Step Functions routes to NotifyFailure on passed=False.
"""

import os
import json
import boto3

ssm = boto3.client("ssm")
PARAM_PREFIX = os.environ.get("SNOWFLAKE_PARAM_PREFIX", "/data-pipeline/snowflake")
QUALITY_THRESHOLD = float(os.environ.get("QUALITY_THRESHOLD", "0.99"))


def lambda_handler(event: dict, context) -> dict:
    """
    Runs a series of quality checks against Snowflake.
    Returns:
        { "passed": bool, "checks": [ { "name": str, "passed": bool, "detail": str } ] }
    """
    # TODO: initialize real Snowflake connector using SSM credentials
    # creds = _get_snowflake_creds()
    # conn = snowflake.connector.connect(**creds)

    checks = _run_checks()
    all_passed = all(c["passed"] for c in checks)

    print(json.dumps({"passed": all_passed, "checks": checks}))
    return {"passed": all_passed, "checks": checks}


def _run_checks() -> list[dict]:
    """
    Stub quality checks. Replace with real Snowflake queries.
    Each check queries a mart table and validates a condition.
    """
    return [
        {
            "name": "row_count_non_zero",
            "passed": True,
            "detail": "Stub: assume non-zero rows",
        },
        {
            "name": "null_rate_below_threshold",
            "passed": True,
            "detail": f"Stub: null rate < {1 - QUALITY_THRESHOLD:.0%}",
        },
    ]


def _get_snowflake_creds() -> dict:
    """Fetch Snowflake connection params from SSM Parameter Store."""
    params = ssm.get_parameters(
        Names=[
            f"{PARAM_PREFIX}/account",
            f"{PARAM_PREFIX}/user",
            f"{PARAM_PREFIX}/password",
            f"{PARAM_PREFIX}/database",
            f"{PARAM_PREFIX}/warehouse",
            f"{PARAM_PREFIX}/schema",
        ],
        WithDecryption=True,
    )
    return {p["Name"].split("/")[-1]: p["Value"] for p in params["Parameters"]}
