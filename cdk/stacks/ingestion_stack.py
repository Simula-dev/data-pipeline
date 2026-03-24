"""
IngestionStack — S3 raw landing bucket + Lambda functions for ingest, Snowflake load, and quality gate.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    RemovalPolicy,
)
from constructs import Construct


class IngestionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- S3 raw landing zone ---
        self.raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name=f"data-pipeline-raw-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="archive-after-90-days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(90),
                        )
                    ],
                )
            ],
        )

        # Shared execution role for pipeline Lambdas
        lambda_role = iam.Role(
            self,
            "PipelineLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        self.raw_bucket.grant_read_write(lambda_role)

        # Snowflake credentials stored in SSM — grant read access
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-pipeline/snowflake/*"
                ],
            )
        )

        # --- Lambda: ingest raw data into S3 ---
        self.ingest_function = _lambda.Function(
            self,
            "IngestFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ingest"),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
            },
        )

        # --- Lambda: load S3 data into Snowflake (COPY INTO) ---
        self.load_function = _lambda.Function(
            self,
            "LoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/load"),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
            },
        )

        # --- Lambda: data quality gate after dbt run ---
        self.quality_gate_function = _lambda.Function(
            self,
            "QualityGateFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/quality_gate"),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
                "QUALITY_THRESHOLD": "0.99",
            },
        )
