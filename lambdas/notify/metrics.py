"""
CloudWatch Embedded Metric Format (EMF) emitter.

EMF lets a Lambda publish custom metrics just by logging a specific JSON
structure to stdout. CloudWatch Logs parses it and creates the metrics
automatically \u2014 zero `put_metric_data` API calls, zero cost overhead.

See: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format.html
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

NAMESPACE = "DataPipeline"


def emit_pipeline_metrics(
    *,
    status: str,
    stats: dict,
    duration_seconds: float,
) -> None:
    """
    Log an EMF payload with pipeline execution metrics.

    Metrics published:
      - PipelineExecutions  (Count)          \u2014 dimensioned by Status
      - PipelineDuration    (Seconds)        \u2014 dimensioned by Status
      - RowsIngested        (Count)          \u2014 dimensioned by Source
      - RowsLoaded          (Count)          \u2014 dimensioned by Source
      - MLRowsLoaded        (Count)          \u2014 dimensioned by Source
      - QualityCheckErrors  (Count)          \u2014 dimensioned by Status
      - QualityCheckWarns   (Count)          \u2014 dimensioned by Status
    """
    source = stats.get("source", "unknown")
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    payload = {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": NAMESPACE,
                    "Dimensions": [["Status"]],
                    "Metrics": [
                        {"Name": "PipelineExecutions", "Unit": "Count"},
                        {"Name": "PipelineDuration",   "Unit": "Seconds"},
                        {"Name": "QualityCheckErrors", "Unit": "Count"},
                        {"Name": "QualityCheckWarns",  "Unit": "Count"},
                    ],
                },
                {
                    "Namespace": NAMESPACE,
                    "Dimensions": [["Source"]],
                    "Metrics": [
                        {"Name": "RowsIngested", "Unit": "Count"},
                        {"Name": "RowsLoaded",   "Unit": "Count"},
                        {"Name": "MLRowsLoaded", "Unit": "Count"},
                    ],
                },
            ],
        },
        # Dimension values
        "Status": status,
        "Source": source,
        # Metric values
        "PipelineExecutions": 1,
        "PipelineDuration": round(duration_seconds, 3),
        "RowsIngested": int(stats.get("rowsIngested", 0) or 0),
        "RowsLoaded": int(stats.get("rowsLoaded", 0) or 0),
        "MLRowsLoaded": int(stats.get("mlRowsLoaded", 0) or 0),
        "QualityCheckErrors": int(stats.get("qualityErrors", 0) or 0),
        "QualityCheckWarns": int(stats.get("qualityWarns", 0) or 0),
    }

    # CloudWatch Logs reads this directly from Lambda stdout
    print(json.dumps(payload))
