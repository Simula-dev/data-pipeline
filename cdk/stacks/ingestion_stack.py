"""
IngestionStack \u2014 S3 raw landing bucket + Lambda functions for ingest, load, ml, and quality gate.

All Lambdas use the Redshift Data API (redshift-data service) via boto3
which is already in the Lambda runtime. No Docker bundling, no C extensions,
no Lambda Layers \u2014 just pure Python + stdlib + boto3.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        redshift_workgroup_name: str,
        redshift_database_name: str,
        redshift_s3_role_arn: str,
        **kwargs,
    ) -> None:
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

        # --- Shared execution role for pipeline Lambdas ---
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

        # API keys (for HTTP ingest sources) live in SSM under /data-pipeline/secrets/*
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-pipeline/*"
                ],
            )
        )

        # Redshift Data API permissions \u2014 load/ml/quality Lambdas call these
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "redshift-data:ExecuteStatement",
                    "redshift-data:BatchExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult",
                    "redshift-data:CancelStatement",
                ],
                resources=["*"],  # Data API doesn't support resource-level perms
            )
        )
        # Redshift Data API also requires GetCredentials against the workgroup
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["redshift-serverless:GetCredentials"],
                resources=[
                    f"arn:aws:redshift-serverless:{self.region}:{self.account}:workgroup/*"
                ],
            )
        )

        # Common env vars for all Lambdas that talk to Redshift
        redshift_env = {
            "REDSHIFT_WORKGROUP": redshift_workgroup_name,
            "REDSHIFT_DATABASE": redshift_database_name,
            "REDSHIFT_S3_ROLE_ARN": redshift_s3_role_arn,
        }

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

        # --- Lambda: load S3 data into Redshift (COPY via temp table) ---
        self.load_function = _lambda.Function(
            self,
            "LoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/load"),
            role=lambda_role,
            timeout=Duration.minutes(15),
            memory_size=512,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                **redshift_env,
            },
        )

        # --- Lambda: ml_export \u2014 UNLOAD marts.ml_inference_input to S3 ---
        self.ml_export_function = _lambda.Function(
            self,
            "MLExportFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ml_export"),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "ML_INFERENCE_INPUT_TABLE": "marts.ml_inference_input",
                **redshift_env,
            },
        )

        # --- Lambda: ml_load \u2014 COPY batch transform output into marts.ml_predictions ---
        self.ml_load_function = _lambda.Function(
            self,
            "MLLoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ml_load"),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "ML_PREDICTIONS_TABLE": "marts.ml_predictions",
                **redshift_env,
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
            memory_size=512,
            environment={
                **redshift_env,
            },
        )

        CfnOutput(
            self,
            "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Raw S3 bucket name",
        )
