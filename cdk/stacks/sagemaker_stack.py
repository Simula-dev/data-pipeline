"""
SageMakerStack \u2014 IAM role, S3 prefixes, and model package group for ML workflows.

Resources:
  - Execution role SageMaker uses for training jobs + batch transform
  - Model package group (registry of trained model versions)
  - S3 prefixes on the raw bucket: ml/training/, ml/input/, ml/output/
  - SSM parameter placeholder for the active model name (Step Functions reads this)
"""

from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sagemaker as sagemaker,
    aws_ssm as ssm,
    CfnOutput,
)
from constructs import Construct


class SageMakerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        raw_bucket: s3.Bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Execution role used by training jobs and batch transform ---
        self.execution_role = iam.Role(
            self,
            "SageMakerExecutionRole",
            role_name="data-pipeline-sagemaker",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSageMakerFullAccess"
                ),
            ],
            description="Execution role for SageMaker training and batch transform",
        )
        raw_bucket.grant_read_write(
            self.execution_role, objects_key_pattern="ml/*"
        )

        # --- Model package group (acts as the model registry) ---
        self.model_package_group = sagemaker.CfnModelPackageGroup(
            self,
            "ModelPackageGroup",
            model_package_group_name="data-pipeline-model",
            model_package_group_description=(
                "Versioned registry of trained models for the data pipeline"
            ),
        )

        # --- SSM parameter: active model name (read by the pipeline) ---
        # Placeholder value; user updates after training their first model.
        self.active_model_param = ssm.StringParameter(
            self,
            "ActiveModelParam",
            parameter_name="/data-pipeline/ml/model_name",
            string_value="NONE",
            description=(
                "Name of the SageMaker model to use for batch transform. "
                "Set to 'NONE' to disable ML steps in the pipeline."
            ),
        )

        # --- CDK outputs for CLI scripts ---
        CfnOutput(
            self,
            "SageMakerRoleArn",
            value=self.execution_role.role_arn,
            description="Pass to scripts/train_sagemaker.py as --role-arn",
        )
        CfnOutput(
            self,
            "ModelPackageGroupName",
            value=self.model_package_group.model_package_group_name,
        )
