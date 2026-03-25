"""
Structured JSON logger for Lambda.

Emits one JSON object per log line so CloudWatch Insights can query
fields directly (e.g. `fields @timestamp, level, event, source`).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge any `extra={...}` fields the caller passed in
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            payload.update(record.extra_fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def get_logger(name: str = "ingest") -> logging.Logger:
    """Return a configured JSON logger. Safe to call multiple times."""
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """
    Emit a structured log event.

    Usage:
        log_event(logger, "http_request_sent", url=url, method="GET")
    """
    logger.info(event, extra={"extra_fields": {"event": event, **fields}})
