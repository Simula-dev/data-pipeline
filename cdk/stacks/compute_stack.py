"""
ComputeStack \u2014 ECS Fargate cluster and task definition for running dbt Core transformations.

Key design:
  - DockerImageAsset builds dbt/Dockerfile during `cdk deploy` and pushes
    to a CDK-managed ECR repo. No manual ECR workflow required.
  - Snowflake credentials are injected into the container at start time
    via ECS `secrets` integration with SSM Parameter Store.
  - The same image is reused by Step Functions for `dbt run`, `dbt test`,
    and `dbt source freshness` \u2014 the command is overridden per-step.
"""

from aws_cdk import (
    Stack,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs,
    aws_ssm as ssm,
    aws_ecr_assets as ecr_assets,
    RemovalPolicy,
)
from constructs import Construct


class ComputeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --------------------------------------------------------------- #
        #  Networking                                                      #
        # --------------------------------------------------------------- #
        self.vpc = ec2.Vpc(
            self,
            "PipelineVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # --------------------------------------------------------------- #
        #  ECS cluster                                                     #
        # --------------------------------------------------------------- #
        self.cluster = ecs.Cluster(
            self,
            "DbtCluster",
            vpc=self.vpc,
            cluster_name="data-pipeline-dbt",
            container_insights=True,
        )

        # --------------------------------------------------------------- #
        #  dbt Docker image \u2014 built from ./dbt during `cdk deploy`          #
        # --------------------------------------------------------------- #
        dbt_image_asset = ecr_assets.DockerImageAsset(
            self,
            "DbtImage",
            directory="dbt",
            platform=ecr_assets.Platform.LINUX_AMD64,
            file="Dockerfile",
        )

        # --------------------------------------------------------------- #
        #  Import Snowflake credentials from SSM Parameter Store           #
        # --------------------------------------------------------------- #
        # Reference existing SSM params by name. ECS will fetch at container
        # start and inject as environment variables. Both String and
        # SecureString types work with ecs.Secret.from_ssm_parameter().
        def _import_param(logical_id: str, name: str) -> ssm.IParameter:
            return ssm.StringParameter.from_string_parameter_name(
                self, logical_id, string_parameter_name=name
            )

        sf_account   = _import_param("SFAccountParam",   "/data-pipeline/snowflake/account")
        sf_user      = _import_param("SFUserParam",      "/data-pipeline/snowflake/user")
        sf_password  = _import_param("SFPasswordParam",  "/data-pipeline/snowflake/password")
        sf_database  = _import_param("SFDatabaseParam",  "/data-pipeline/snowflake/database")
        sf_warehouse = _import_param("SFWarehouseParam", "/data-pipeline/snowflake/warehouse")

        # --------------------------------------------------------------- #
        #  Task roles                                                      #
        # --------------------------------------------------------------- #
        execution_role = iam.Role(
            self,
            "DbtTaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
            description="Pulls image from ECR and fetches SSM secrets at task start",
        )
        # ECS execution role also needs read on the SSM params (to inject them)
        for param in (sf_account, sf_user, sf_password, sf_database, sf_warehouse):
            param.grant_read(execution_role)

        task_role = iam.Role(
            self,
            "DbtTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="Runtime permissions for the dbt container",
        )
        # dbt itself may need to read additional SSM params (e.g. custom vars)
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ssm:GetParameter", "ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-pipeline/*"
                ],
            )
        )

        # --------------------------------------------------------------- #
        #  CloudWatch log group for container output                       #
        # --------------------------------------------------------------- #
        log_group = logs.LogGroup(
            self,
            "DbtLogGroup",
            log_group_name="/data-pipeline/dbt",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --------------------------------------------------------------- #
        #  Fargate task definition                                         #
        # --------------------------------------------------------------- #
        self.dbt_task_definition = ecs.FargateTaskDefinition(
            self,
            "DbtTaskDefinition",
            cpu=1024,
            memory_limit_mib=2048,
            execution_role=execution_role,
            task_role=task_role,
        )

        self.dbt_task_definition.add_container(
            "DbtContainer",
            image=ecs.ContainerImage.from_docker_image_asset(dbt_image_asset),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="dbt",
                log_group=log_group,
            ),
            environment={
                "DBT_PROFILES_DIR": "/app/dbt",
            },
            secrets={
                "SNOWFLAKE_ACCOUNT":   ecs.Secret.from_ssm_parameter(sf_account),
                "SNOWFLAKE_USER":      ecs.Secret.from_ssm_parameter(sf_user),
                "SNOWFLAKE_PASSWORD":  ecs.Secret.from_ssm_parameter(sf_password),
                "SNOWFLAKE_DATABASE":  ecs.Secret.from_ssm_parameter(sf_database),
                "SNOWFLAKE_WAREHOUSE": ecs.Secret.from_ssm_parameter(sf_warehouse),
            },
            # Default command \u2014 Step Functions overrides per step
            command=["dbt", "build", "--profiles-dir", "/app/dbt"],
        )
