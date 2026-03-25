"""
HTTP client for ingestion.

Uses urllib3 (bundled with boto3, no external Lambda layer needed).
Implements:
  - Retry with exponential backoff on 5xx / 429 / connection errors
  - Configurable rate limiting (requests-per-second)
  - Pagination: page-based, offset-based, and cursor-based
  - Bearer / API-key-header / Basic / no-auth
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any, Iterator

import urllib3
from urllib3.util.retry import Retry

from config import AuthType, PaginationType, SourceConfig
from logger import get_logger, log_event


logger = get_logger(__name__)


class HttpClient:
    """Synchronous HTTP client with retry + pagination support."""

    def __init__(self, config: SourceConfig, auth_secret: str | None = None):
        self.config = config
        self.auth_secret = auth_secret

        retry = Retry(
            total=5,
            backoff_factor=1.0,                   # 1s, 2s, 4s, 8s, 16s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True,
        )
        self._pool = urllib3.PoolManager(
            timeout=urllib3.Timeout(total=config.timeout_seconds),
            retries=retry,
        )

        self._min_interval = 1.0 / max(config.requests_per_second, 0.01)
        self._last_request_ts = 0.0

    # ------------------------------------------------------------------ #
    #  Auth header construction                                           #
    # ------------------------------------------------------------------ #
    def _auth_headers(self) -> dict[str, str]:
        cfg = self.config
        if cfg.auth_type == AuthType.NONE or not self.auth_secret:
            return {}
        if cfg.auth_type == AuthType.BEARER:
            return {"Authorization": f"Bearer {self.auth_secret}"}
        if cfg.auth_type == AuthType.API_KEY_HEADER:
            if not cfg.auth_header_name:
                raise ValueError("auth_header_name required for API_KEY_HEADER")
            return {cfg.auth_header_name: self.auth_secret}
        if cfg.auth_type == AuthType.BASIC:
            encoded = base64.b64encode(self.auth_secret.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        raise ValueError(f"Unsupported auth type: {cfg.auth_type}")

    # ------------------------------------------------------------------ #
    #  Rate limiting                                                      #
    # ------------------------------------------------------------------ #
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    # ------------------------------------------------------------------ #
    #  Single request                                                     #
    # ------------------------------------------------------------------ #
    def _request(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        self._throttle()

        headers = {
            "Accept": "application/json",
            "User-Agent": "data-pipeline-ingest/1.0",
            **self.config.headers,
            **self._auth_headers(),
        }

        log_event(logger, "http_request_sent", url=url, params=params)

        response = self._pool.request(
            self.config.method,
            url,
            fields=params if self.config.method == "GET" else None,
            body=json.dumps(params).encode() if self.config.method != "GET" else None,
            headers=headers,
        )

        log_event(
            logger,
            "http_response_received",
            url=url,
            status=response.status,
            bytes=len(response.data),
        )

        if response.status >= 400:
            raise HttpError(
                status=response.status,
                url=url,
                body=response.data.decode("utf-8", errors="replace")[:500],
            )

        return json.loads(response.data.decode("utf-8"))

    # ------------------------------------------------------------------ #
    #  Pagination iterator                                                #
    # ------------------------------------------------------------------ #
    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Yield records one at a time, handling pagination transparently."""
        cfg = self.config
        url = f"{cfg.base_url}{cfg.endpoint}"
        params = dict(cfg.query_params)
        pages_seen = 0

        if cfg.pagination_type == PaginationType.PAGE:
            params.setdefault("page", 1)
            params.setdefault("per_page", cfg.page_size)
        elif cfg.pagination_type == PaginationType.OFFSET:
            params.setdefault("offset", 0)
            params.setdefault("limit", cfg.page_size)

        while pages_seen < cfg.max_pages:
            payload = self._request(url, params)
            records = _get_nested(payload, cfg.records_json_path)

            if not isinstance(records, list):
                raise ValueError(
                    f"Expected list at '{cfg.records_json_path}', "
                    f"got {type(records).__name__}"
                )

            for record in records:
                yield record

            pages_seen += 1

            if cfg.pagination_type == PaginationType.NONE or not records:
                break
            if cfg.pagination_type == PaginationType.PAGE:
                if len(records) < cfg.page_size:
                    break
                params["page"] += 1
            elif cfg.pagination_type == PaginationType.OFFSET:
                if len(records) < cfg.page_size:
                    break
                params["offset"] += cfg.page_size
            elif cfg.pagination_type == PaginationType.CURSOR:
                cursor = payload.get(cfg.cursor_field)
                if not cursor:
                    break
                params["cursor"] = cursor

        log_event(logger, "ingest_pagination_complete", pages=pages_seen)


class HttpError(Exception):
    def __init__(self, status: int, url: str, body: str):
        self.status = status
        self.url = url
        self.body = body
        super().__init__(f"HTTP {status} from {url}: {body}")


def _get_nested(payload: dict[str, Any], dotted_path: str) -> Any:
    """Resolve 'a.b.c' against a nested dict. Returns payload if path is empty."""
    if not dotted_path:
        return payload
    value: Any = payload
    for key in dotted_path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value
