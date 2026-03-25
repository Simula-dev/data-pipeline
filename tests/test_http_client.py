"""
Unit tests for HttpClient.

We patch HttpClient._request to avoid real HTTP, focusing tests on
pagination + auth logic rather than urllib3 internals.
"""

from unittest.mock import patch

import pytest

from config import AuthType, PaginationType, SourceConfig
from http_client import HttpClient, _get_nested


def _make_config(**overrides) -> SourceConfig:
    base = {
        "source_name": "test",
        "base_url": "https://api.example.com",
        "endpoint": "/items",
        "records_json_path": "data",
        "requests_per_second": 100.0,  # disable throttling in tests
    }
    base.update(overrides)
    return SourceConfig.from_event(base)


def test_get_nested_resolves_dotted_path():
    payload = {"outer": {"inner": {"items": [1, 2, 3]}}}
    assert _get_nested(payload, "outer.inner.items") == [1, 2, 3]
    assert _get_nested(payload, "outer.missing") is None
    assert _get_nested(payload, "") == payload


def test_no_pagination_yields_all_records():
    cfg = _make_config()
    client = HttpClient(cfg)

    with patch.object(client, "_request", return_value={"data": [{"id": 1}, {"id": 2}]}):
        records = list(client.iter_records())

    assert records == [{"id": 1}, {"id": 2}]


def test_page_pagination_stops_on_short_page():
    cfg = _make_config(pagination_type="page", page_size=3, max_pages=10)
    client = HttpClient(cfg)

    pages = [
        {"data": [{"id": 1}, {"id": 2}, {"id": 3}]},   # full page, fetch next
        {"data": [{"id": 4}, {"id": 5}]},               # short page, stop
    ]

    with patch.object(client, "_request", side_effect=pages) as mock_request:
        records = list(client.iter_records())

    assert [r["id"] for r in records] == [1, 2, 3, 4, 5]
    assert mock_request.call_count == 2


def test_page_pagination_respects_max_pages():
    cfg = _make_config(pagination_type="page", page_size=2, max_pages=2)
    client = HttpClient(cfg)

    infinite_page = {"data": [{"id": 1}, {"id": 2}]}
    with patch.object(client, "_request", return_value=infinite_page) as mock_request:
        records = list(client.iter_records())

    assert len(records) == 4  # 2 pages * 2 records
    assert mock_request.call_count == 2


def test_cursor_pagination_stops_when_cursor_missing():
    cfg = _make_config(pagination_type="cursor", cursor_field="next_cursor")
    client = HttpClient(cfg)

    pages = [
        {"data": [{"id": 1}], "next_cursor": "abc"},
        {"data": [{"id": 2}], "next_cursor": None},
    ]
    with patch.object(client, "_request", side_effect=pages):
        records = list(client.iter_records())

    assert [r["id"] for r in records] == [1, 2]


def test_offset_pagination_stops_on_short_page():
    cfg = _make_config(pagination_type="offset", page_size=3, max_pages=5)
    client = HttpClient(cfg)

    pages = [
        {"data": [{"id": 1}, {"id": 2}, {"id": 3}]},
        {"data": [{"id": 4}]},  # short page, stop
    ]
    with patch.object(client, "_request", side_effect=pages):
        records = list(client.iter_records())

    assert len(records) == 4


def test_bearer_auth_header_added():
    cfg = _make_config(auth_type="bearer")
    client = HttpClient(cfg, auth_secret="mytoken")
    headers = client._auth_headers()
    assert headers == {"Authorization": "Bearer mytoken"}


def test_api_key_header_auth():
    cfg = _make_config(auth_type="api_key", auth_header_name="X-API-Key")
    client = HttpClient(cfg, auth_secret="secretkey")
    headers = client._auth_headers()
    assert headers == {"X-API-Key": "secretkey"}


def test_basic_auth_base64_encoded():
    cfg = _make_config(auth_type="basic")
    client = HttpClient(cfg, auth_secret="user:pass")
    headers = client._auth_headers()
    # base64 of 'user:pass' = 'dXNlcjpwYXNz'
    assert headers == {"Authorization": "Basic dXNlcjpwYXNz"}


def test_no_auth_returns_empty_headers():
    cfg = _make_config(auth_type="none")
    client = HttpClient(cfg, auth_secret=None)
    assert client._auth_headers() == {}


def test_non_list_records_field_raises():
    cfg = _make_config()
    client = HttpClient(cfg)
    with patch.object(client, "_request", return_value={"data": "not a list"}):
        with pytest.raises(ValueError, match="Expected list"):
            list(client.iter_records())
