"""
ComputeStack \u2014 ECS Fargate cluster + dbt task definition for Redshift.

Networking:
  - 3-AZ VPC (Redshift Serverless requires 3+ subnets across AZs)
  - Private subnets with NAT egress for outbound internet (package installs)
  - Fargate tasks run in private subnets

dbt container:
  - Built via DockerImageAsset from ./dbt/Dockerfile
  - Credentials injected at container start via ECS `secrets` integration
    with AWS Secrets Manager (Redshift admin password) and SSM (host, db)
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
    @property
    def availability_zones(self) -> list[str]:
        return [f"{self.region}a", f"{self.region}b", f"{self.region}c"]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --------------------------------------------------------------- #
        #  Networking \u2014 3-AZ VPC                                          #
        # --------------------------------------------------------------- #
        self.vpc = ec2.Vpc(
            self,
            "PipelineVpc",
            max_azs=3,
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
        #  dbt Fargate security group                                      #
        #  RedshiftStack takes this as a param and grants 5439 ingress.    #
        # --------------------------------------------------------------- #
        self.dbt_security_group = ec2.SecurityGroup(
            self,
            "DbtSecurityGroup",
            vpc=self.vpc,
            description="dbt Fargate tasks \u2014 egress to Redshift + internet",
            allow_all_outbound=True,
        )

        # --------------------------------------------------------------- #
        #  Import Redshift creds (by well-known name, not cross-stack ref) #
        #  Avoids a circular dependency with RedshiftStack, which takes    #
        #  this stack's VPC + dbt SG as inputs.                            #
        # --------------------------------------------------------------- #
        admin_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "RedshiftAdminSecret", "data-pipeline/redshift/admin"
        )

        def _import_ssm(logical_id: str, name: str) -> ssm.IParameter:
            return ssm.StringParameter.from_string_parameter_name(
                self, logical_id, string_parameter_name=name
            )

        rs_workgroup = _import_ssm("RSWorkgroupParam", "/data-pipeline/redshift/workgroup")
        rs_database  = _import_ssm("RSDatabaseParam",  "/data-pipeline/redshift/database")

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
        # Grant reads on the secrets / SSM params that get injected
        admin_secret.grant_read(execution_role)
        for param in (rs_workgroup, rs_database):
            param.grant_read(execution_role)

        task_role = iam.Role(
            self,
            "DbtTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="Runtime permissions for the dbt container",
        )
        # dbt uses Redshift Data API for some ops (optional) + reads SSM for vars
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

        # Redshift Serverless endpoint is deterministic:
        #   <workgroup>.<account>.<region>.redshift-serverless.amazonaws.com
        # Workgroup name is fixed in RedshiftStack.
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
                # Secrets Manager JSON secret \u2014 pull individual keys via field
                "REDSHIFT_USER":     ecs.Secret.from_secrets_manager(admin_secret, field="username"),
                "REDSHIFT_PASSWORD": ecs.Secret.from_secrets_manager(admin_secret, field="password"),
                "REDSHIFT_DATABASE": ecs.Secret.from_ssm_parameter(rs_database),
            },
            command=["dbt", "build", "--profiles-dir", "/app/dbt"],
        )
