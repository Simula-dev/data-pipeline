"""
Unit tests for the load Lambda.

The load Lambda uses the Redshift Data API via boto3. We stub the
`boto3.client('redshift-data')` calls so tests don't hit real AWS.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


LOAD_DIR = str(Path(__file__).resolve().parents[1] / "lambdas" / "load")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET", "test-raw-bucket")
    monkeypatch.setenv("REDSHIFT_WORKGROUP", "data-pipeline")
    monkeypatch.setenv("REDSHIFT_DATABASE", "data_pipeline")
    monkeypatch.setenv("REDSHIFT_S3_ROLE_ARN", "arn:aws:iam::123:role/test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")


def _fresh_handler_module():
    """
    Re-import the load Lambda's handler module in isolation.

    Other test files also add their Lambda dirs to sys.path and may have
    cached module names \u2014 force load dir to the front of sys.path and
    clear the shared module names before re-importing.
    """
    while LOAD_DIR in sys.path:
        sys.path.remove(LOAD_DIR)
    sys.path.insert(0, LOAD_DIR)

    for mod in ("handler", "redshift_client", "logger"):
        sys.modules.pop(mod, None)

    import handler  # noqa: WPS433
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

    # Mock the boto3 redshift-data client used by _run_batch and RedshiftClient
    mock_rs = MagicMock()
    # batch_execute_statement returns a statement id
    mock_rs.batch_execute_statement.return_value = {"Id": "batch-123"}
    # describe_statement returns FINISHED immediately for the batch poll
    mock_rs.describe_statement.return_value = {"Status": "FINISHED"}
    # fetch_scalar path: execute_statement + describe_statement + get_statement_result
    mock_rs.execute_statement.return_value = {"Id": "select-456"}
    mock_rs.get_statement_result.return_value = {
        "Records": [[{"longValue": 42}]]
    }
    # Make the describe call return FINISHED with HasResultSet=True for the COUNT query
    def _describe(Id):
        if Id == "batch-123":
            return {"Status": "FINISHED"}
        return {"Status": "FINISHED", "HasResultSet": True, "Id": Id}
    mock_rs.describe_statement.side_effect = _describe

    # Patch boto3.client to return our mock whenever redshift-data is requested
    with patch.object(handler, "RedshiftClient") as MockClient:
        client_inst = MagicMock()
        client_inst.workgroup = "data-pipeline"
        client_inst.database = "data_pipeline"
        client_inst.poll_interval = 0.01
        client_inst.default_timeout = 60
        client_inst.fetch_scalar.return_value = 42
        MockClient.return_value = client_inst

        with patch("boto3.client", return_value=mock_rs):
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
    assert result["status"] == "LOADED"
    assert result["loadId"] == "req-abc123"
    assert result["source"] == "github_repos"
    assert result["s3Key"].endswith(".ndjson")

    # Verify batch_execute_statement was called with 5 SQLs (DROP/CREATE/COPY/INSERT/DROP)
    mock_rs.batch_execute_statement.assert_called_once()
    batch_kwargs = mock_rs.batch_execute_statement.call_args.kwargs
    assert len(batch_kwargs["Sqls"]) == 5
    assert "COPY load_stage" in batch_kwargs["Sqls"][2]
    assert "INSERT INTO raw.landing" in batch_kwargs["Sqls"][3]


def test_load_from_direct_invocation():
    handler = _fresh_handler_module()

    mock_rs = MagicMock()
    mock_rs.batch_execute_statement.return_value = {"Id": "batch-999"}
    mock_rs.describe_statement.return_value = {"Status": "FINISHED", "HasResultSet": True}

    with patch.object(handler, "RedshiftClient") as MockClient:
        client_inst = MagicMock()
        client_inst.workgroup = "data-pipeline"
        client_inst.database = "data_pipeline"
        client_inst.poll_interval = 0.01
        client_inst.default_timeout = 60
        client_inst.fetch_scalar.return_value = 5
        MockClient.return_value = client_inst

        with patch("boto3.client", return_value=mock_rs):
            event = {"source": "manual_backfill", "s3Key": "raw/manual/file.ndjson"}
            result = handler.lambda_handler(event, _Ctx())

    assert result["source"] == "manual_backfill"
    assert result["rowsLoaded"] == 5


def test_load_raises_on_batch_failure():
    handler = _fresh_handler_module()

    mock_rs = MagicMock()
    mock_rs.batch_execute_statement.return_value = {"Id": "batch-fail"}
    mock_rs.describe_statement.return_value = {
        "Status": "FAILED",
        "Error": "permission denied for table raw.landing",
    }

    with patch.object(handler, "RedshiftClient") as MockClient:
        client_inst = MagicMock()
        client_inst.workgroup = "data-pipeline"
        client_inst.database = "data_pipeline"
        client_inst.poll_interval = 0.01
        client_inst.default_timeout = 60
        MockClient.return_value = client_inst

        with patch("boto3.client", return_value=mock_rs):
            event = {"source": "x", "s3Key": "raw/x/file.ndjson"}
            with pytest.raises(Exception, match="permission denied"):
                handler.lambda_handler(event, _Ctx())
