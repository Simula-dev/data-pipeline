"""
Ingestion source configuration.

A SourceConfig describes *how* to pull data from a single external source:
base URL, auth, pagination style, rate-limit hints. It is built from the
Step Functions event payload and/or SSM parameters.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class AuthType(str, Enum):
    NONE = "none"
    BEARER = "bearer"           # Authorization: Bearer <token>
    API_KEY_HEADER = "api_key"  # <header_name>: <token>
    BASIC = "basic"             # Authorization: Basic <b64(user:pass)>


class PaginationType(str, Enum):
    NONE = "none"
    PAGE = "page"        # ?page=1, ?page=2...
    OFFSET = "offset"    # ?offset=0&limit=100
    CURSOR = "cursor"    # response contains next_cursor field


@dataclass
class SourceConfig:
    """Declarative config for a single ingestion source."""

    source_name: str
    base_url: str
    endpoint: str = "/"
    method: str = "GET"
    auth_type: AuthType = AuthType.NONE

    # SSM parameter name where the secret lives (SecureString)
    auth_secret_ssm: Optional[str] = None
    auth_header_name: Optional[str] = None  # required for API_KEY_HEADER

    query_params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    pagination_type: PaginationType = PaginationType.NONE
    page_size: int = 100
    max_pages: int = 50           # safety cap
    cursor_field: str = "next_cursor"

    # Rate limiting
    requests_per_second: float = 5.0
    timeout_seconds: float = 30.0

    # Response shape
    records_json_path: str = "data"  # top-level key containing record array

    @classmethod
    def from_event(cls, event: dict[str, Any]) -> "SourceConfig":
        """Build a SourceConfig from a Step Functions event payload."""
        if "source_name" not in event or "base_url" not in event:
            raise ValueError(
                "Event must include 'source_name' and 'base_url' fields"
            )

        return cls(
            source_name=event["source_name"],
            base_url=event["base_url"].rstrip("/"),
            endpoint=event.get("endpoint", "/"),
            method=event.get("method", "GET"),
            auth_type=AuthType(event.get("auth_type", "none")),
            auth_secret_ssm=event.get("auth_secret_ssm"),
            auth_header_name=event.get("auth_header_name"),
            query_params=event.get("query_params", {}),
            headers=event.get("headers", {}),
            pagination_type=PaginationType(event.get("pagination_type", "none")),
            page_size=int(event.get("page_size", 100)),
            max_pages=int(event.get("max_pages", 50)),
            cursor_field=event.get("cursor_field", "next_cursor"),
            requests_per_second=float(event.get("requests_per_second", 5.0)),
            timeout_seconds=float(event.get("timeout_seconds", 30.0)),
            records_json_path=event.get("records_json_path", "data"),
        )
