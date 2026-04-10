"""
RDSStack \u2014 PostgreSQL instance on RDS free tier (db.t3.micro).

Replaces RedshiftStack. Uses Secrets Manager for the admin password
(auto-rotatable) and exposes a "pipeline" security group that both
Lambdas and dbt Fargate attach to for 5432 access.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_rds as rds,
    aws_ssm as ssm,
    aws_secretsmanager as secretsmanager,
    CfnOutput,
)
from constructs import Construct


class RDSStack(Stack):
    @property
    def availability_zones(self) -> list[str]:
        return [f"{self.region}a", f"{self.region}b", f"{self.region}c"]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------ #
        #  Pipeline security group \u2014 attached by Lambdas + dbt Fargate       #
        # ------------------------------------------------------------------ #
        self.pipeline_security_group = ec2.SecurityGroup(
            self,
            "PipelineSecurityGroup",
            vpc=vpc,
            description="Pipeline workloads - Lambdas and dbt Fargate",
            allow_all_outbound=True,
        )

        # ------------------------------------------------------------------ #
        #  RDS security group \u2014 allows 5432 from pipeline SG                 #
        # ------------------------------------------------------------------ #
        rds_sg = ec2.SecurityGroup(
            self,
            "RDSSecurityGroup",
            vpc=vpc,
            description="RDS PostgreSQL - accepts connections from pipeline SG",
            allow_all_outbound=False,
        )
        rds_sg.add_ingress_rule(
            peer=self.pipeline_security_group,
            connection=ec2.Port.tcp(5432),
            description="PostgreSQL from pipeline Lambdas and dbt Fargate",
        )

        # ------------------------------------------------------------------ #
        #  RDS PostgreSQL instance (free tier: db.t3.micro, 20GB gp2)        #
        # ------------------------------------------------------------------ #
        self.db_instance = rds.DatabaseInstance(
            self,
            "PostgresInstance",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_16_4,
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO,
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[rds_sg],
            database_name="data_pipeline",
            credentials=rds.Credentials.from_generated_secret(
                "admin",
                secret_name="data-pipeline/rds/admin",
            ),
            allocated_storage=20,
            max_allocated_storage=20,
            storage_type=rds.StorageType.GP2,
            multi_az=False,
            publicly_accessible=False,
            backup_retention=Duration.days(1),  # free tier limit
            deletion_protection=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_minor_version_upgrade=True,
        )

        self.admin_secret = self.db_instance.secret

        # ------------------------------------------------------------------ #
        #  SSM parameters for service discovery                               #
        # ------------------------------------------------------------------ #
        self.host_param = ssm.StringParameter(
            self,
            "RDSHostParam",
            parameter_name="/data-pipeline/rds/host",
            string_value=self.db_instance.db_instance_endpoint_address,
            description="RDS PostgreSQL endpoint hostname",
        )
        self.port_param = ssm.StringParameter(
            self,
            "RDSPortParam",
            parameter_name="/data-pipeline/rds/port",
            string_value=self.db_instance.db_instance_endpoint_port,
            description="RDS PostgreSQL port",
        )
        self.database_param = ssm.StringParameter(
            self,
            "RDSDatabaseParam",
            parameter_name="/data-pipeline/rds/database",
            string_value="data_pipeline",
            description="RDS database name",
        )

        # ------------------------------------------------------------------ #
        #  Grant raw bucket read to the Lambda role (for S3 reads in load)    #
        # ------------------------------------------------------------------ #
        # (Lambdas already have S3 access via ingestion_stack lambda_role,
        #  but the bucket grant is there. This stack just needs to expose
        #  the SG and secret for other stacks.)

        # ------------------------------------------------------------------ #
        #  Outputs                                                             #
        # ------------------------------------------------------------------ #
        CfnOutput(self, "RDSEndpoint", value=self.db_instance.db_instance_endpoint_address)
        CfnOutput(self, "AdminSecretArn", value=self.admin_secret.secret_arn)
        CfnOutput(self, "PipelineSecurityGroupId", value=self.pipeline_security_group.security_group_id)
