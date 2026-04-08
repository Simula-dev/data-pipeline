"""
ComputeStack \u2014 ECS Fargate cluster + dbt task definition for Redshift.

Uses VPC from NetworkStack and dbt security group + Redshift credentials
from RedshiftStack (both passed explicitly to create CloudFormation
cross-stack references, which makes CDK deploy the stacks in the right
order).
"""

from aws_cdk import (
    Stack,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs,
    aws_ssm as ssm,
    aws_ecr_assets as ecr_assets,
    aws_secretsmanager as secretsmanager,
    RemovalPolicy,
)
from constructs import Construct


class ComputeStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        dbt_security_group: ec2.ISecurityGroup,
        redshift_admin_secret: secretsmanager.ISecret,
        redshift_workgroup_param: ssm.IStringParameter,
        redshift_database_param: ssm.IStringParameter,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = vpc
        self.dbt_security_group = dbt_security_group

        # --------------------------------------------------------------- #
        #  ECS cluster                                                     #
        # --------------------------------------------------------------- #
        self.cluster = ecs.Cluster(
            self,
            "DbtCluster",
            vpc=vpc,
            cluster_name="data-pipeline-dbt",
            container_insights=True,
        )

        # --------------------------------------------------------------- #
        #  dbt Docker image \u2014 built during `cdk deploy`                    #
        # --------------------------------------------------------------- #
        dbt_image_asset = ecr_assets.DockerImageAsset(
            self,
            "DbtImage",
            directory="dbt",
            platform=ecr_assets.Platform.LINUX_AMD64,
            file="Dockerfile",
        )

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
            description="Pulls image from ECR and fetches secrets at task start",
        )
        # Grants for secret/SSM reads (ECS Agent fetches these at container start)
        redshift_admin_secret.grant_read(execution_role)
        redshift_workgroup_param.grant_read(execution_role)
        redshift_database_param.grant_read(execution_role)

        task_role = iam.Role(
            self,
            "DbtTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="Runtime permissions for the dbt container",
        )
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

        # Redshift Serverless endpoint is deterministic
        redshift_host = f"data-pipeline.{self.account}.{self.region}.redshift-serverless.amazonaws.com"

        self.dbt_task_definition.add_container(
            "DbtContainer",
            image=ecs.ContainerImage.from_docker_image_asset(dbt_image_asset),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="dbt",
                log_group=log_group,
            ),
            environment={
                "DBT_PROFILES_DIR": "/app/dbt",
                "REDSHIFT_HOST": redshift_host,
            },
            secrets={
                "REDSHIFT_USER":     ecs.Secret.from_secrets_manager(redshift_admin_secret, field="username"),
                "REDSHIFT_PASSWORD": ecs.Secret.from_secrets_manager(redshift_admin_secret, field="password"),
                "REDSHIFT_DATABASE": ecs.Secret.from_ssm_parameter(redshift_database_param),
            },
            command=["dbt", "build", "--profiles-dir", "/app/dbt"],
        )
