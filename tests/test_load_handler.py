"""
Unit tests for the load Lambda.

The snowflake-connector-python package is not installed in the test env
(it's only needed inside the Lambda at runtime). We patch the SnowflakeClient
to avoid importing the real connector, focusing on event parsing, dispatch
logic, and result shape.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# Add the load Lambda dir to sys.path so `import handler` resolves
LOAD_DIR = Path(__file__).resolve().parents[1] / "lambdas" / "load"
sys.path.insert(0, str(LOAD_DIR))


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PARAM_PREFIX", "/test/sf")
    monkeypatch.setenv("SNOWFLAKE_STAGE", "RAW.S3_RAW_STAGE")
    monkeypatch.setenv("SNOWFLAKE_FILE_FORMAT", "RAW.NDJSON_FORMAT")


def _fresh_handler_module():
    """Re-import handler after patching env / sys.modules so env vars are picked up."""
    if "handler" in sys.modules:
        del sys.modules["handler"]
    if "snowflake_client" in sys.modules:
        del sys.modules["snowflake_client"]
    import handler  # noqa: WPS433
    return handler


class _Ctx:
    aws_request_id = "req-abc123"


def test_skipped_when_ingest_yielded_no_file(monkeypatch):
    handler = _fresh_handler_module()

    event = {
        "ingestResult": {
            "Payload": {
                "source": "empty_api",
                "recordCount": 0,
                "s3Key": None,
            }
        }
    }

    result = handler.lambda_handler(event, _Ctx())

    assert result["status"] == "SKIPPED"
    assert result["rowsLoaded"] == 0
    assert result["source"] == "empty_api"


def test_load_from_step_functions_event(monkeypatch):
    handler = _fresh_handler_module()

    # Stub SnowflakeConfig.from_ssm and SnowflakeClient so no real AWS/Snowflake calls happen
    fake_result = {
        "rowsLoaded": 42,
        "filesLoaded": 1,
        "errors": 0,
        "status": "LOADED",
        "perFile": [{"file": "raw/x.ndjson", "status": "LOADED", "rowsParsed": 42, "rowsLoaded": 42, "errors": 0}],
    }

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = None
    mock_client.copy_into_landing.return_value = fake_result

    monkeypatch.setattr(handler, "SnowflakeConfig", MagicMock())
    monkeypatch.setattr(handler, "SnowflakeClient", MagicMock(return_value=mock_client))

    event = {
        "source_name": "github_repos",
        "ingestResult": {
            "Payload": {
                "source": "github_repos",
                "s3Bucket": "raw-bucket",
                "s3Key": "raw/github_repos/2026/04/08/120000_abcdef0123456789.ndjson",
                "recordCount": 42,
            }
        },
    }

    result = handler.lambda_handler(event, _Ctx())

    assert result["rowsLoaded"] == 42
    assert result["filesLoaded"] == 1
    assert result["status"] == "LOADED"
    assert result["loadId"] == "req-abc123"
    assert result["source"] == "github_repos"
    assert result["s3Key"].endswith(".ndjson")

    # Verify the client was called with the right args
    mock_client.copy_into_landing.assert_called_once()
    call_kwargs = mock_client.copy_into_landing.call_args.kwargs
    assert call_kwargs["source"] == "github_repos"
    assert call_kwargs["load_id"] == "req-abc123"
    assert call_kwargs["stage_name"] == "RAW.S3_RAW_STAGE"
    assert call_kwargs["file_format"] == "RAW.NDJSON_FORMAT"


def test_load_from_direct_invocation(monkeypatch):
    handler = _fresh_handler_module()

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = None
    mock_client.copy_into_landing.return_value = {
        "rowsLoaded": 5, "filesLoaded": 1, "errors": 0, "status": "LOADED", "perFile": []
    }

    monkeypatch.setattr(handler, "SnowflakeConfig", MagicMock())
    monkeypatch.setattr(handler, "SnowflakeClient", MagicMock(return_value=mock_client))

    event = {"source": "manual_backfill", "s3Key": "raw/manual/file.ndjson"}
    result = handler.lambda_handler(event, _Ctx())

    assert result["source"] == "manual_backfill"
    assert result["rowsLoaded"] == 5


def test_partial_load_surfaces_status(monkeypatch):
    handler = _fresh_handler_module()

    mock_client = MagicMock()
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = None
    mock_client.copy_into_landing.return_value = {
        "rowsLoaded": 95, "filesLoaded": 1, "errors": 5, "status": "PARTIAL", "perFile": []
    }

    monkeypatch.setattr(handler, "SnowflakeConfig", MagicMock())
    monkeypatch.setattr(handler, "SnowflakeClient", MagicMock(return_value=mock_client))

    event = {"source": "s", "s3Key": "raw/s/x.ndjson"}
    result = handler.lambda_handler(event, _Ctx())

    assert result["status"] == "PARTIAL"
    assert result["errors"] == 5
