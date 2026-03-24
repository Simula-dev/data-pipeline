"""
Notify Lambda — standalone notification helper (used outside Step Functions if needed).
Step Functions publishes directly to SNS via SnsPublish task; this Lambda
can be invoked independently for ad-hoc alerts or monitoring integrations.
"""

import os
import json
import boto3

sns = boto3.client("sns")
TOPIC_ARN = os.environ.get("NOTIFY_TOPIC_ARN", "")


def lambda_handler(event: dict, context) -> dict:
    status = event.get("status", "UNKNOWN")
    message = event.get("message", json.dumps(event))

    response = sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=f"[Data Pipeline] {status}",
        Message=message,
    )

    return {"statusCode": 200, "messageId": response["MessageId"]}
