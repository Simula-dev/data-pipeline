"""
Unit tests for the quality gate check runner.

Snowflake connection is mocked \u2014 tests exercise the check dispatch logic,
comparison operators, result aggregation, and error handling.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add quality_gate Lambda dir to sys.path so `import check_runner` resolves
QG_DIR = Path(__file__).resolve().parents[1] / "lambdas" / "quality_gate"
sys.path.insert(0, str(QG_DIR))

from check_runner import (  # noqa: E402
    CheckResult,
    _row_count_min,
    _freshness,
    _null_rate,
    _uniqueness,
    _custom_sql,
    run_check,
)


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _client(scalar=None, all_rows=None):
    """Return a mock SnowflakeClient that produces canned results."""
    client = MagicMock()
    if scalar is not None:
        client.fetch_scalar.return_value = scalar
    if all_rows is not None:
        client.fetch_all.return_value = all_rows
    return client


# --------------------------------------------------------------------------- #
#  row_count_min                                                               #
# --------------------------------------------------------------------------- #

def test_row_count_passes_when_above_threshold():
    check = {"name": "t", "type": "row_count_min", "table": "RAW.LANDING", "min_rows": 10}
    result = _row_count_min(check, _client(scalar=50))
    assert result.passed is True
    assert result.details["actual_rows"] == 50


def test_row_count_fails_when_below_threshold():
    check = {"name": "t", "type": "row_count_min", "table": "RAW.LANDING", "min_rows": 10}
    result = _row_count_min(check, _client(scalar=5))
    assert result.passed is False
    assert result.details["actual_rows"] == 5


def test_row_count_handles_empty_table():
    check = {"name": "t", "type": "row_count_min", "table": "X", "min_rows": 1}
    result = _row_count_min(check, _client(scalar=0))
    assert result.passed is False


# --------------------------------------------------------------------------- #
#  freshness                                                                   #
# --------------------------------------------------------------------------- #

def test_freshness_passes_when_recent():
    check = {"name": "t", "type": "freshness", "table": "RAW.LANDING", "max_age_hours": 24}
    result = _freshness(check, _client(scalar=3))
    assert result.passed is True
    assert result.details["hours_since_last"] == 3


def test_freshness_fails_when_stale():
    check = {"name": "t", "type": "freshness", "table": "RAW.LANDING", "max_age_hours": 24}
    result = _freshness(check, _client(scalar=48))
    assert result.passed is False


def test_freshness_fails_when_null_timestamp():
    check = {"name": "t", "type": "freshness", "table": "X", "max_age_hours": 24}
    result = _freshness(check, _client(scalar=None))
    assert result.passed is False
    assert "empty" in result.details["reason"] or "null" in result.details["reason"]


# --------------------------------------------------------------------------- #
#  null_rate                                                                   #
# --------------------------------------------------------------------------- #

def test_null_rate_zero_passes():
    check = {"name": "t", "type": "null_rate", "table": "X", "column": "c", "max_null_rate": 0.0}
    result = _null_rate(check, _client(all_rows=[(1000, 0)]))
    assert result.passed is True
    assert result.details["null_rate"] == 0.0


def test_null_rate_above_threshold_fails():
    check = {"name": "t", "type": "null_rate", "table": "X", "column": "c", "max_null_rate": 0.05}
    # 100 nulls / 1000 total = 0.10 > 0.05
    result = _null_rate(check, _client(all_rows=[(1000, 100)]))
    assert result.passed is False
    assert result.details["null_rate"] == 0.1


def test_null_rate_empty_table_returns_zero_rate():
    check = {"name": "t", "type": "null_rate", "table": "X", "column": "c", "max_null_rate": 0.0}
    result = _null_rate(check, _client(all_rows=[(0, 0)]))
    assert result.details["null_rate"] == 0.0
    assert result.passed is True


# --------------------------------------------------------------------------- #
#  uniqueness                                                                  #
# --------------------------------------------------------------------------- #

def test_uniqueness_passes_no_duplicates():
    check = {"name": "t", "type": "uniqueness", "table": "X", "columns": ["id"]}
    result = _uniqueness(check, _client(scalar=0))
    assert result.passed is True


def test_uniqueness_fails_with_duplicates():
    check = {"name": "t", "type": "uniqueness", "table": "X", "columns": ["id", "ts"]}
    result = _uniqueness(check, _client(scalar=3))
    assert result.passed is False
    assert result.details["duplicate_groups"] == 3


# --------------------------------------------------------------------------- #
#  custom_sql                                                                  #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "comparison,actual,expected,should_pass",
    [
        ("eq", 0, 0, True),
        ("eq", 1, 0, False),
        ("gt", 5, 3, True),
        ("gt", 2, 3, False),
        ("gte", 3, 3, True),
        ("lt", 1, 2, True),
        ("lte", 2, 2, True),
        ("ne", 1, 0, True),
    ],
)
def test_custom_sql_comparisons(comparison, actual, expected, should_pass):
    check = {
        "name": "t", "type": "custom_sql",
        "sql": "SELECT ...",
        "comparison": comparison,
        "expected_value": expected,
    }
    result = _custom_sql(check, _client(scalar=actual))
    assert result.passed is should_pass


def test_custom_sql_unknown_comparison_fails_with_error():
    check = {"name": "t", "type": "custom_sql", "sql": "SELECT 1", "comparison": "bogus"}
    result = _custom_sql(check, _client(scalar=1))
    assert result.passed is False
    assert result.error and "comparison" in result.error.lower()


# --------------------------------------------------------------------------- #
#  Dispatcher (run_check)                                                      #
# --------------------------------------------------------------------------- #

def test_unknown_check_type_returns_failure_not_exception():
    result = run_check(_client(), {"name": "x", "type": "nonsense"})
    assert result.passed is False
    assert "Unknown check type" in result.error


def test_check_exception_caught_and_returned_as_failure():
    client = MagicMock()
    client.fetch_scalar.side_effect = RuntimeError("connection lost")
    result = run_check(client, {"name": "x", "type": "row_count_min", "table": "Y", "min_rows": 1})
    assert result.passed is False
    assert "connection lost" in result.error
