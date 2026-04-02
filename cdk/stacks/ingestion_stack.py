"""
IngestionStack — S3 raw landing bucket + Lambda functions for ingest, Snowflake load, and quality gate.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    BundlingOptions,
    RemovalPolicy,
    CfnOutput,
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
        # Bundled in Docker because snowflake-connector-python has C extensions
        # that must be compiled for Lambda's Linux x86_64 runtime.
        self.load_function = _lambda.Function(
            self,
            "LoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambdas/load",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    # user=root works around a Docker Desktop Windows WSL2
                    # UID mismatch on bind mounts; files end up readable by
                    # Lambda runtime regardless of host ownership.
                    user="root",
                    command=[
                        "bash",
                        "-c",
                        (
                            "pip install --no-cache-dir -r requirements.txt "
                            "-t /asset-output && "
                            "cp -au . /asset-output"
                        ),
                    ],
                ),
            ),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
                "SNOWFLAKE_STAGE": "RAW.S3_RAW_STAGE",
                "SNOWFLAKE_FILE_FORMAT": "RAW.NDJSON_FORMAT",
            },
        )

        # --- IAM role for Snowflake storage integration ---
        # Snowflake assumes this role via STS to read from the raw bucket.
        # After `cdk deploy`, run `DESC INTEGRATION S3_RAW_INTEGRATION` in
        # Snowflake to get STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID,
        # then update this role's trust policy via the AWS console or SDK.
        self.snowflake_integration_role = iam.Role(
            self,
            "SnowflakeStorageIntegrationRole",
            role_name="data-pipeline-snowflake-integration",
            assumed_by=iam.AccountPrincipal(self.account),  # updated post-deploy
            description="Assumed by Snowflake via STS to access the raw bucket",
        )
        self.raw_bucket.grant_read(self.snowflake_integration_role)
        self.snowflake_integration_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation", "s3:ListBucket"],
                resources=[self.raw_bucket.bucket_arn],
            )
        )

        CfnOutput(
            self,
            "SnowflakeIntegrationRoleArn",
            value=self.snowflake_integration_role.role_arn,
            description="Paste into sql/setup/02_storage_integration.sql",
        )
        CfnOutput(
            self,
            "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Raw S3 bucket name \u2014 used in stage URL",
        )

        # --- Lambda: ml_export \u2014 unload inference input to S3 as CSV ---
        self.ml_export_function = _lambda.Function(
            self,
            "MLExportFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambdas/ml_export",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    user="root",  # see LoadFunction for rationale
                    command=[
                        "bash", "-c",
                        "pip install --no-cache-dir -r requirements.txt -t /asset-output && "
                        "cp -au . /asset-output",
                    ],
                ),
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
                "SNOWFLAKE_STAGE": "RAW.S3_RAW_STAGE",
                "ML_INFERENCE_INPUT_TABLE": "MARTS.ML_INFERENCE_INPUT",
            },
        )

        # --- Lambda: ml_load \u2014 COPY INTO MARTS.ML_PREDICTIONS ---
        self.ml_load_function = _lambda.Function(
            self,
            "MLLoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambdas/ml_load",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    user="root",  # see LoadFunction for rationale
                    command=[
                        "bash", "-c",
                        "pip install --no-cache-dir -r requirements.txt -t /asset-output && "
                        "cp -au . /asset-output",
                    ],
                ),
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
                "SNOWFLAKE_STAGE": "RAW.S3_RAW_STAGE",
                "ML_PREDICTIONS_TABLE": "MARTS.ML_PREDICTIONS",
            },
        )

        # --- Lambda: data quality gate after dbt run ---
        self.quality_gate_function = _lambda.Function(
            self,
            "QualityGateFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambdas/quality_gate",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    user="root",  # see LoadFunction for rationale
                    command=[
                        "bash", "-c",
                        "pip install --no-cache-dir -r requirements.txt -t /asset-output && "
                        "cp -au . /asset-output",
                    ],
                ),
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
            },
        )
