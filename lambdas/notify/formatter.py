"""
Notification message formatter.

Extracts stats from the full Step Functions state and renders a
human-readable SNS message + subject line.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def extract_stats(state: dict) -> dict:
    """
    Pull the relevant numbers out of the Step Functions state.

    The state shape is whatever the pipeline has accumulated by the time
    Notify runs: ingestResult, loadResult, dbtResult, mlExport, mlLoadResult,
    qualityResult, and the original input event fields.
    """
    def _payload(key: str) -> dict:
        block = state.get(key) or {}
        return block.get("Payload") or {} if isinstance(block, dict) else {}

    ingest = _payload("ingestResult")
    load = _payload("loadResult")
    ml_load = _payload("mlLoadResult")
    quality = state.get("qualityResult") or {}

    return {
        "source": state.get("source_name") or ingest.get("source") or "unknown",
        "rowsIngested": int(ingest.get("recordCount", 0) or 0),
        "rowsLoaded": int(load.get("rowsLoaded", 0) or 0),
        "loadStatus": load.get("status", "n/a"),
        "mlEnabled": bool(state.get("ml_enabled", False)),
        "mlRowsLoaded": int(ml_load.get("rowsLoaded", 0) or 0),
        "qualityPassed": quality.get("passed"),
        "qualityTotal": int(quality.get("totalChecks", 0) or 0),
        "qualityErrors": int(quality.get("errorCount", 0) or 0),
        "qualityWarns": int(quality.get("warnCount", 0) or 0),
        "errorInfo": state.get("errorInfo"),
    }


def compute_duration_seconds(start_iso: str | None) -> float:
    if not start_iso:
        return 0.0
    try:
        normalized = start_iso.replace("Z", "+00:00")
        start = datetime.fromisoformat(normalized)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - start).total_seconds()
    except (ValueError, TypeError):
        return 0.0


def build_message(
    *,
    status: str,
    execution_id: str,
    execution_arn: str | None,
    start_time: str | None,
    state_machine_name: str,
    region: str,
    stats: dict,
) -> str:
    """Build the SNS message body as plain text."""
    duration = compute_duration_seconds(start_time)
    duration_str = _format_duration(duration)

    lines: list[str] = [
        f"Pipeline:  {state_machine_name}",
        f"Execution: {execution_id}",
        f"Status:    {status}",
        f"Duration:  {duration_str}",
        f"Start:     {start_time or 'unknown'}",
        "",
        "--- Pipeline Stats ---",
        f"  Source:          {stats['source']}",
        f"  Rows ingested:   {stats['rowsIngested']:,}",
        f"  Rows loaded:     {stats['rowsLoaded']:,}  ({stats['loadStatus']})",
    ]

    if stats["mlEnabled"]:
        lines.append(f"  ML predictions:  {stats['mlRowsLoaded']:,}")
    else:
        lines.append("  ML predictions:  (disabled)")

    if stats["qualityPassed"] is not None:
        gate = "PASSED" if stats["qualityPassed"] else "FAILED"
        lines.append(
            f"  Quality gate:    {gate}  "
            f"({stats['qualityTotal']} checks, "
            f"{stats['qualityErrors']} errors, "
            f"{stats['qualityWarns']} warnings)"
        )

    if status == "FAILURE" and stats.get("errorInfo"):
        lines.extend(["", "--- Error ---", str(stats["errorInfo"])[:1000]])

    if execution_arn and region:
        console_url = _execution_console_url(execution_arn, region)
        lines.extend(["", f"Console: {console_url}"])

    return "\n".join(lines)


def build_subject(status: str, execution_id: str, stats: dict) -> str:
    # SNS email subject cap is 100 chars
    short_id = execution_id[:8] if execution_id else "?"
    source = stats.get("source", "?")[:30]
    subject = f"[Pipeline {status}] {source} ({short_id})"
    return subject[:100]


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {secs}s"


def _execution_console_url(execution_arn: str, region: str) -> str:
    # arn:aws:states:<region>:<account>:execution:<state-machine-name>:<execution-name>
    return (
        f"https://{region}.console.aws.amazon.com/states/home"
        f"?region={region}#/v2/executions/details/{execution_arn}"
    )
