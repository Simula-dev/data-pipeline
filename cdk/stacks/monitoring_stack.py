"""
MonitoringStack \u2014 SNS notification topic, notify Lambda, CloudWatch alarms, and dashboard.

The notify Lambda is invoked by Step Functions at the two terminal states
(NotifySuccess / NotifyFailure). It formats a human-readable message,
publishes to SNS, and emits CloudWatch metrics via EMF. The alarms and
widgets defined here reference those metrics.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    RemovalPolicy,
)
from constructs import Construct


class MonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        alert_email = self.node.try_get_context("alert_email")

        # ------------------------------------------------------------------ #
        #  SNS topic \u2014 alert destination for pipeline + CloudWatch alarms    #
        # ------------------------------------------------------------------ #
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

        # ------------------------------------------------------------------ #
        #  Notify Lambda \u2014 formats messages, publishes to SNS, emits metrics #
        # ------------------------------------------------------------------ #
        notify_role = iam.Role(
            self,
            "NotifyLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        self.notify_topic.grant_publish(notify_role)

        self.notify_function = _lambda.Function(
            self,
            "NotifyFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/notify"),
            role=notify_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "NOTIFY_TOPIC_ARN": self.notify_topic.topic_arn,
            },
        )

        # ------------------------------------------------------------------ #
        #  Metric references \u2014 namespace emitted by the notify Lambda (EMF)  #
        # ------------------------------------------------------------------ #
        NAMESPACE = "DataPipeline"

        success_executions = cw.Metric(
            namespace=NAMESPACE,
            metric_name="PipelineExecutions",
            dimensions_map={"Status": "SUCCESS"},
            statistic="Sum",
            period=Duration.minutes(5),
            label="Success",
        )

        failure_executions = cw.Metric(
            namespace=NAMESPACE,
            metric_name="PipelineExecutions",
            dimensions_map={"Status": "FAILURE"},
            statistic="Sum",
            period=Duration.minutes(5),
            label="Failure",
        )

        duration_p95 = cw.Metric(
            namespace=NAMESPACE,
            metric_name="PipelineDuration",
            dimensions_map={"Status": "SUCCESS"},
            statistic="p95",
            period=Duration.minutes(15),
            label="p95 duration (success)",
        )

        duration_avg = cw.Metric(
            namespace=NAMESPACE,
            metric_name="PipelineDuration",
            dimensions_map={"Status": "SUCCESS"},
            statistic="Average",
            period=Duration.minutes(15),
            label="Average duration",
        )

        quality_errors = cw.Metric(
            namespace=NAMESPACE,
            metric_name="QualityCheckErrors",
            dimensions_map={"Status": "FAILURE"},
            statistic="Sum",
            period=Duration.minutes(5),
            label="Quality check errors",
        )

        # SEARCH expression rolls up rows ingested across every Source dimension value
        rows_ingested_search = cw.MathExpression(
            expression="SEARCH('{DataPipeline,Source} MetricName=\"RowsIngested\"', 'Sum', 300)",
            label="Rows ingested (by source)",
            period=Duration.minutes(5),
        )

        # ------------------------------------------------------------------ #
        #  Alarms                                                              #
        # ------------------------------------------------------------------ #
        failure_alarm = cw.Alarm(
            self,
            "PipelineFailureAlarm",
            alarm_name="data-pipeline-failure",
            alarm_description="Pipeline failed at least once in the last 5 minutes",
            metric=failure_executions,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        failure_alarm.add_alarm_action(cw_actions.SnsAction(self.notify_topic))

        duration_alarm = cw.Alarm(
            self,
            "PipelineDurationAlarm",
            alarm_name="data-pipeline-duration-sla",
            alarm_description="Pipeline p95 duration exceeded 1 hour",
            metric=duration_p95,
            threshold=3600,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        duration_alarm.add_alarm_action(cw_actions.SnsAction(self.notify_topic))

        quality_alarm = cw.Alarm(
            self,
            "QualityGateErrorAlarm",
            alarm_name="data-pipeline-quality-errors",
            alarm_description="Quality gate raised one or more error-severity failures",
            metric=quality_errors,
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        quality_alarm.add_alarm_action(cw_actions.SnsAction(self.notify_topic))

        # ------------------------------------------------------------------ #
        #  Dashboard                                                           #
        # ------------------------------------------------------------------ #
        dashboard = cw.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name="data-pipeline",
            period_override=cw.PeriodOverride.AUTO,
        )

        dashboard.add_widgets(
            cw.TextWidget(
                markdown=(
                    "# Data Pipeline\n"
                    "Orchestrated by Step Functions \u2014 Ingest \u2192 Snowflake \u2192 dbt \u2192 SageMaker \u2192 Quality Gate\n"
                    "\nMetrics emitted from the notify Lambda via CloudWatch EMF."
                ),
                width=24,
                height=3,
            )
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Pipeline executions (5m)",
                left=[success_executions, failure_executions],
                stacked=False,
                width=12,
                height=6,
            ),
            cw.GraphWidget(
                title="Pipeline duration (seconds)",
                left=[duration_p95, duration_avg],
                width=12,
                height=6,
            ),
        )

        dashboard.add_widgets(
            cw.SingleValueWidget(
                title="Total executions (24h)",
                metrics=[
                    cw.Metric(
                        namespace=NAMESPACE,
                        metric_name="PipelineExecutions",
                        dimensions_map={"Status": "SUCCESS"},
                        statistic="Sum",
                        period=Duration.hours(24),
                        label="Success",
                    ),
                    cw.Metric(
                        namespace=NAMESPACE,
                        metric_name="PipelineExecutions",
                        dimensions_map={"Status": "FAILURE"},
                        statistic="Sum",
                        period=Duration.hours(24),
                        label="Failure",
                    ),
                ],
                width=8,
                height=4,
            ),
            cw.SingleValueWidget(
                title="Quality gate errors (24h)",
                metrics=[
                    cw.Metric(
                        namespace=NAMESPACE,
                        metric_name="QualityCheckErrors",
                        dimensions_map={"Status": "FAILURE"},
                        statistic="Sum",
                        period=Duration.hours(24),
                    )
                ],
                width=8,
                height=4,
            ),
            cw.AlarmStatusWidget(
                title="Pipeline alarms",
                alarms=[failure_alarm, duration_alarm, quality_alarm],
                width=8,
                height=4,
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Rows ingested by source",
                left=[rows_ingested_search],
                width=24,
                height=6,
            )
        )
