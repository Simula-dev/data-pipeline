"""
Unit tests for the notify Lambda message formatter.

Validates extract_stats parses the nested Step Functions state shape,
build_message produces readable output, and duration calculation handles
ISO 8601 with and without trailing Z.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

NOTIFY_DIR = Path(__file__).resolve().parents[1] / "lambdas" / "notify"
sys.path.insert(0, str(NOTIFY_DIR))

from formatter import (  # noqa: E402
    build_message,
    build_subject,
    compute_duration_seconds,
    extract_stats,
    _format_duration,
)


# --------------------------------------------------------------------------- #
#  extract_stats                                                               #
# --------------------------------------------------------------------------- #

def test_extract_stats_success_path():
    state = {
        "source_name": "github_trending_repos",
        "ingestResult": {"Payload": {"source": "github_trending_repos", "recordCount": 300}},
        "loadResult":   {"Payload": {"rowsLoaded": 295, "status": "LOADED"}},
        "ml_enabled":   True,
        "mlLoadResult": {"Payload": {"rowsLoaded": 295}},
        "qualityResult": {
            "passed": True,
            "totalChecks": 6,
            "errorCount": 0,
            "warnCount": 1,
        },
    }
    stats = extract_stats(state)
    assert stats["source"] == "github_trending_repos"
    assert stats["rowsIngested"] == 300
    assert stats["rowsLoaded"] == 295
    assert stats["loadStatus"] == "LOADED"
    assert stats["mlEnabled"] is True
    assert stats["mlRowsLoaded"] == 295
    assert stats["qualityPassed"] is True
    assert stats["qualityTotal"] == 6
    assert stats["qualityErrors"] == 0
    assert stats["qualityWarns"] == 1
    assert stats["errorInfo"] is None


def test_extract_stats_empty_state_returns_defaults():
    stats = extract_stats({})
    assert stats["source"] == "unknown"
    assert stats["rowsIngested"] == 0
    assert stats["rowsLoaded"] == 0
    assert stats["mlEnabled"] is False


def test_extract_stats_failure_path_with_error():
    state = {
        "source_name": "api_x",
        "ingestResult": {"Payload": {"recordCount": 50}},
        "errorInfo": {"Error": "HttpError", "Cause": "403 forbidden"},
    }
    stats = extract_stats(state)
    assert stats["source"] == "api_x"
    assert stats["errorInfo"]["Error"] == "HttpError"


# --------------------------------------------------------------------------- #
#  Duration                                                                    #
# --------------------------------------------------------------------------- #

def test_compute_duration_handles_z_suffix():
    # 5 minutes ago
    start = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = compute_duration_seconds(start)
    assert 290 <= duration <= 310  # allow jitter


def test_compute_duration_handles_offset_format():
    start = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    duration = compute_duration_seconds(start)
    assert 25 <= duration <= 35


def test_compute_duration_returns_zero_on_missing_or_bad_input():
    assert compute_duration_seconds(None) == 0.0
    assert compute_duration_seconds("not a date") == 0.0
    assert compute_duration_seconds("") == 0.0


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (5.2, "5.2s"),
        (90, "1m 30s"),
        (3700, "1h 1m 40s"),
    ],
)
def test_format_duration(seconds, expected):
    assert _format_duration(seconds) == expected


# --------------------------------------------------------------------------- #
#  Message + subject                                                           #
# --------------------------------------------------------------------------- #

def test_build_subject_truncates_to_100_chars():
    long_source = "x" * 200
    stats = {"source": long_source}
    subject = build_subject("SUCCESS", "abcdef1234567890", stats)
    assert len(subject) <= 100


def test_build_message_includes_core_fields():
    stats = {
        "source": "github_trending_repos",
        "rowsIngested": 1234,
        "rowsLoaded": 1230,
        "loadStatus": "LOADED",
        "mlEnabled": False,
        "mlRowsLoaded": 0,
        "qualityPassed": True,
        "qualityTotal": 5,
        "qualityErrors": 0,
        "qualityWarns": 0,
        "errorInfo": None,
    }
    message = build_message(
        status="SUCCESS",
        execution_id="exec-123",
        execution_arn="arn:aws:states:us-east-1:123:execution:sm:exec-123",
        start_time=None,
        state_machine_name="data-pipeline-orchestrator",
        region="us-east-1",
        stats=stats,
    )
    assert "SUCCESS" in message
    assert "github_trending_repos" in message
    assert "1,234" in message     # formatted with thousands separator
    assert "PASSED" in message
    assert "console.aws.amazon.com" in message


def test_build_message_failure_includes_error_info():
    stats = {
        "source": "s",
        "rowsIngested": 0, "rowsLoaded": 0, "loadStatus": "n/a",
        "mlEnabled": False, "mlRowsLoaded": 0,
        "qualityPassed": None, "qualityTotal": 0, "qualityErrors": 0, "qualityWarns": 0,
        "errorInfo": "HTTP 500 from upstream API",
    }
    message = build_message(
        status="FAILURE",
        execution_id="exec-999",
        execution_arn=None,
        start_time=None,
        state_machine_name="data-pipeline-orchestrator",
        region="us-east-1",
        stats=stats,
    )
    assert "FAILURE" in message
    assert "HTTP 500" in message


def test_build_message_ml_disabled_shows_marker():
    stats = {
        "source": "s", "rowsIngested": 1, "rowsLoaded": 1, "loadStatus": "LOADED",
        "mlEnabled": False, "mlRowsLoaded": 0,
        "qualityPassed": True, "qualityTotal": 1, "qualityErrors": 0, "qualityWarns": 0,
        "errorInfo": None,
    }
    message = build_message(
        status="SUCCESS",
        execution_id="e",
        execution_arn=None,
        start_time=None,
        state_machine_name="n",
        region="us-east-1",
        stats=stats,
    )
    assert "disabled" in message
