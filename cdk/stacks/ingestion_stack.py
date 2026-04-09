"""
IngestionStack \u2014 S3 raw landing bucket + Lambda functions.

Lambdas that access the database (load, ml_export, ml_load, quality_gate)
are VPC-attached so they can reach the RDS PostgreSQL instance in the
private subnet. They use pg8000 (pure Python PostgreSQL driver) bundled
via Docker at synth time.

The ingest Lambda does NOT need VPC access (it only writes to S3).
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    BundlingOptions,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct

from cdk.local_bundling import LocalPipBundling


class IngestionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        pipeline_security_group: ec2.ISecurityGroup,
        rds_secret: secretsmanager.ISecret,
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

        # --- Shared execution role for VPC-attached Lambdas ---
        lambda_role = iam.Role(
            self,
            "PipelineLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                # Required for VPC-attached Lambdas (ENI management)
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
        self.raw_bucket.grant_read_write(lambda_role)

        # SSM access for API keys (ingest sources)
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-pipeline/*"
                ],
            )
        )

        # Secrets Manager access for RDS credentials
        rds_secret.grant_read(lambda_role)

        # Common env vars for VPC-attached Lambdas
        db_env = {
            "RAW_BUCKET": self.raw_bucket.bucket_name,
            "RDS_SECRET_NAME": "data-pipeline/rds/admin",
        }

        # Bundling: local pip install first (no Docker needed for pure-Python deps),
        # Docker fallback if local fails.
        def _pg8000_bundling(source_dir: str):
            return BundlingOptions(
                image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                local=LocalPipBundling(source_dir),
                user="root",
                command=[
                    "bash", "-c",
                    "pip install --no-cache-dir -r requirements.txt -t /asset-output && "
                    "cp -au . /asset-output",
                ],
            )

        # VPC config shared by all database Lambdas
        vpc_config = {
            "vpc": vpc,
            "vpc_subnets": ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            "security_groups": [pipeline_security_group],
        }

        # --- Lambda: ingest raw data into S3 (NOT VPC-attached) ---
        self.ingest_function = _lambda.Function(
            self,
            "IngestFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ingest"),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={"RAW_BUCKET": self.raw_bucket.bucket_name},
        )

        # --- Lambda: load S3 data into PostgreSQL ---
        self.load_function = _lambda.Function(
            self,
            "LoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/load", bundling=_pg8000_bundling("lambdas/load")),
            role=lambda_role,
            timeout=Duration.minutes(15),
            memory_size=512,
            environment=db_env,
            **vpc_config,
        )

        # --- Lambda: ml_export (SELECT from marts, write CSV to S3) ---
        self.ml_export_function = _lambda.Function(
            self,
            "MLExportFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ml_export", bundling=_pg8000_bundling("lambdas/ml_export")),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                **db_env,
                "ML_INFERENCE_INPUT_TABLE": "marts.ml_inference_input",
            },
            **vpc_config,
        )

        # --- Lambda: ml_load (read CSV from S3, INSERT into marts) ---
        self.ml_load_function = _lambda.Function(
            self,
            "MLLoadFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ml_load", bundling=_pg8000_bundling("lambdas/ml_load")),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                **db_env,
                "ML_PREDICTIONS_TABLE": "marts.ml_predictions",
            },
            **vpc_config,
        )

        # --- Lambda: data quality gate ---
        self.quality_gate_function = _lambda.Function(
            self,
            "QualityGateFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/quality_gate", bundling=_pg8000_bundling("lambdas/quality_gate")),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "RDS_SECRET_NAME": "data-pipeline/rds/admin",
            },
            **vpc_config,
        )

        CfnOutput(
            self,
            "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Raw S3 bucket name",
        )
