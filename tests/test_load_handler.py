"""
Unit tests for the load Lambda (RDS PostgreSQL via pg8000).

We mock the PostgresClient and boto3 S3 client so tests don't hit real AWS.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


LOAD_DIR = str(Path(__file__).resolve().parents[1] / "lambdas" / "load")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET", "test-raw-bucket")
    monkeypatch.setenv("RDS_SECRET_NAME", "data-pipeline/rds/admin")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


def _fresh_handler_module():
    while LOAD_DIR in sys.path:
        sys.path.remove(LOAD_DIR)
    sys.path.insert(0, LOAD_DIR)

    for mod in ("handler", "postgres_client", "logger"):
        sys.modules.pop(mod, None)

    import handler
    return handler


class _Ctx:
    aws_request_id = "req-abc123"


# --------------------------------------------------------------------------- #
#  Event dispatch                                                              #
# --------------------------------------------------------------------------- #

def test_skipped_when_ingest_yielded_no_file():
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

    # Mock S3 read
    ndjson_body = "\n".join([
        json.dumps({"id": i, "name": f"item_{i}"}) for i in range(42)
    ])
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=ndjson_body.encode()))
    }
    monkeypatch.setattr(handler, "s3", mock_s3)

    # Mock PostgresClient
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)
    mock_client.execute_many.return_value = 42
    mock_client.commit = MagicMock()

    monkeypatch.setattr(handler, "PostgresClient", MagicMock(return_value=mock_client))

    event = {
        "source_name": "github_repos",
        "ingestResult": {
            "Payload": {
                "source": "github_repos",
                "s3Bucket": "raw-bucket",
                "s3Key": "raw/github_repos/2026/04/08/120000_abcdef.ndjson",
                "recordCount": 42,
            }
        },
    }
    result = handler.lambda_handler(event, _Ctx())

    assert result["rowsLoaded"] == 42
    assert result["status"] == "LOADED"
    assert result["loadId"] == "req-abc123"
    assert result["source"] == "github_repos"

    # Verify S3 was read
    mock_s3.get_object.assert_called_once()

    # Verify execute_many was called with 42 param tuples
    mock_client.execute_many.assert_called_once()
    call_args = mock_client.execute_many.call_args
    assert len(call_args[0][1]) == 42  # 42 rows


def test_load_from_direct_invocation(monkeypatch):
    handler = _fresh_handler_module()

    ndjson_body = json.dumps({"x": 1}) + "\n" + json.dumps({"x": 2})
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=ndjson_body.encode()))
    }
    monkeypatch.setattr(handler, "s3", mock_s3)

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=None)
    mock_client.execute_many.return_value = 2
    mock_client.commit = MagicMock()
    monkeypatch.setattr(handler, "PostgresClient", MagicMock(return_value=mock_client))

    event = {"source": "manual_backfill", "s3Key": "raw/manual/file.ndjson"}
    result = handler.lambda_handler(event, _Ctx())

    assert result["source"] == "manual_backfill"
    assert result["rowsLoaded"] == 2


def test_empty_file_returns_skipped(monkeypatch):
    handler = _fresh_handler_module()

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b"\n\n"))
    }
    monkeypatch.setattr(handler, "s3", mock_s3)

    event = {"source": "empty", "s3Key": "raw/empty/file.ndjson"}
    result = handler.lambda_handler(event, _Ctx())

    assert result["status"] == "SKIPPED"
    assert result["rowsLoaded"] == 0
