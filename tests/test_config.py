"""Unit tests for SourceConfig."""

import pytest

from config import AuthType, PaginationType, SourceConfig


def test_minimal_event_builds_config():
    event = {
        "source_name": "test_api",
        "base_url": "https://api.example.com/",
    }
    cfg = SourceConfig.from_event(event)

    assert cfg.source_name == "test_api"
    assert cfg.base_url == "https://api.example.com"  # trailing slash stripped
    assert cfg.endpoint == "/"
    assert cfg.auth_type == AuthType.NONE
    assert cfg.pagination_type == PaginationType.NONE


def test_full_event_builds_config():
    event = {
        "source_name": "paginated_api",
        "base_url": "https://api.example.com",
        "endpoint": "/v2/records",
        "method": "GET",
        "auth_type": "bearer",
        "auth_secret_ssm": "/data-pipeline/secrets/test_token",
        "pagination_type": "page",
        "page_size": 50,
        "max_pages": 10,
        "records_json_path": "results.items",
        "requests_per_second": 2.0,
    }
    cfg = SourceConfig.from_event(event)

    assert cfg.source_name == "paginated_api"
    assert cfg.endpoint == "/v2/records"
    assert cfg.auth_type == AuthType.BEARER
    assert cfg.auth_secret_ssm == "/data-pipeline/secrets/test_token"
    assert cfg.pagination_type == PaginationType.PAGE
    assert cfg.page_size == 50
    assert cfg.max_pages == 10
    assert cfg.records_json_path == "results.items"
    assert cfg.requests_per_second == 2.0


def test_missing_required_fields_raises():
    with pytest.raises(ValueError, match="source_name"):
        SourceConfig.from_event({"base_url": "https://api.example.com"})

    with pytest.raises(ValueError, match="base_url"):
        SourceConfig.from_event({"source_name": "x"})


def test_invalid_auth_type_raises():
    with pytest.raises(ValueError):
        SourceConfig.from_event({
            "source_name": "x",
            "base_url": "https://api.example.com",
            "auth_type": "oauth5_quantum",
        })
