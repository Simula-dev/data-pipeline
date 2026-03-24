"""
ComputeStack — ECS Fargate cluster and task definition for running dbt Core transformations.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


class ComputeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # VPC — 2 AZs, public + private subnets
        self.vpc = ec2.Vpc(
            self,
            "PipelineVpc",
            max_azs=2,
            nat_gateways=1,
        )

        # ECS Fargate cluster
        self.cluster = ecs.Cluster(
            self,
            "DbtCluster",
            vpc=self.vpc,
            cluster_name="data-pipeline-dbt",
            container_insights=True,
        )

        # Task execution role
        execution_role = iam.Role(
            self,
            "DbtTaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        # Task role — permissions the dbt container needs at runtime
        task_role = iam.Role(
            self,
            "DbtTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-pipeline/snowflake/*"
                ],
            )
        )

        # Log group for dbt container output
        log_group = logs.LogGroup(
            self,
            "DbtLogGroup",
            log_group_name="/data-pipeline/dbt",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # Fargate task definition
        self.dbt_task_definition = ecs.FargateTaskDefinition(
            self,
            "DbtTaskDefinition",
            cpu=1024,       # 1 vCPU
            memory_limit_mib=2048,
            execution_role=execution_role,
            task_role=task_role,
        )

        # dbt container — image built from ./dbt/Dockerfile and pushed to ECR
        self.dbt_task_definition.add_container(
            "DbtContainer",
            # Replace with your ECR image URI after first push
            image=ecs.ContainerImage.from_registry(
                f"{self.account}.dkr.ecr.{self.region}.amazonaws.com/data-pipeline-dbt:latest"
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="dbt",
                log_group=log_group,
            ),
            environment={
                "DBT_PROFILES_DIR": "/app/dbt",
                "SNOWFLAKE_PARAM_PREFIX": "/data-pipeline/snowflake",
            },
            # Command overridden at runtime by Step Functions
            command=["dbt", "run", "--profiles-dir", "/app/dbt"],
        )
