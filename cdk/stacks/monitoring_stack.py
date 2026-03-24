"""
MonitoringStack — SNS notification topic and CloudWatch dashboard for pipeline health.
"""

from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    CfnParameter,
)
from constructs import Construct


class MonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Alert email — pass via CDK context: --context alert_email=you@example.com
        alert_email = self.node.try_get_context("alert_email")

        # --- SNS topic for pipeline success / failure notifications ---
        self.notify_topic = sns.Topic(
            self,
            "PipelineNotifyTopic",
            topic_name="data-pipeline-notifications",
            display_name="Data Pipeline Alerts",
        )

        if alert_email:
            self.notify_topic.add_subscription(
                subscriptions.EmailSubscription(alert_email)
            )

        # --- CloudWatch dashboard ---
        dashboard = cw.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name="data-pipeline",
        )

        dashboard.add_widgets(
            cw.TextWidget(
                markdown="# Data Pipeline Health\nState machine executions, Lambda errors, ECS task status.",
                width=24,
                height=2,
            )
        )
