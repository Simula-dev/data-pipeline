"""
RedshiftStack \u2014 Redshift Serverless namespace + workgroup + IAM role for S3 COPY.

Redshift Serverless charges per RPU-second of query execution (no idle
cost). The admin user is created with a secure random password stored
in AWS Secrets Manager and mirrored to SSM for the dbt container.

Lambdas interact with the cluster via the Redshift Data API (`redshift-data`
service), which doesn't require a persistent connection or network access
\u2014 just IAM permissions. This removes the need for any Python DB connector
in the Lambda code and eliminates Docker bundling.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_redshiftserverless as redshiftserverless,
    aws_ssm as ssm,
    aws_secretsmanager as secretsmanager,
    CfnOutput,
    RemovalPolicy,
)
from constructs import Construct


class RedshiftStack(Stack):
    # Redshift Serverless requires subnets in at least 3 availability zones.
    # Override the stack's AZs for synth-without-credentials parity with
    # ComputeStack.
    @property
    def availability_zones(self) -> list[str]:
        return [f"{self.region}a", f"{self.region}b", f"{self.region}c"]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        raw_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- dbt Fargate security group (owned here so ComputeStack can  ---
        # --- depend on RedshiftStack without causing a circular dep)      ---
        self.dbt_security_group = ec2.SecurityGroup(
            self,
            "DbtSecurityGroup",
            vpc=vpc,
            description="dbt Fargate tasks \u2014 egress to Redshift + internet",
            allow_all_outbound=True,
        )

        # ------------------------------------------------------------------ #
        #  IAM role for Redshift \u2014 used by COPY/UNLOAD to read/write S3      #
        # ------------------------------------------------------------------ #
        redshift_s3_role = iam.Role(
            self,
            "RedshiftS3Role",
            role_name="data-pipeline-redshift-s3",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            description="Attached to the workgroup; used by COPY FROM S3 and UNLOAD TO S3",
        )
        raw_bucket.grant_read_write(redshift_s3_role)
        redshift_s3_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetBucketLocation", "s3:ListBucket"],
                resources=[raw_bucket.bucket_arn],
            )
        )
        self.s3_role = redshift_s3_role

        # ------------------------------------------------------------------ #
        #  Admin password \u2014 auto-generated, stored in Secrets Manager         #
        # ------------------------------------------------------------------ #
        admin_secret = secretsmanager.Secret(
            self,
            "RedshiftAdminSecret",
            secret_name="data-pipeline/redshift/admin",
            description="Redshift Serverless admin credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "admin"}',
                generate_string_key="password",
                exclude_punctuation=True,  # Redshift password char restrictions
                password_length=32,
            ),
        )
        self.admin_secret = admin_secret

        # ------------------------------------------------------------------ #
        #  Security group \u2014 allows dbt Fargate to reach the workgroup         #
        # ------------------------------------------------------------------ #
        self.security_group = ec2.SecurityGroup(
            self,
            "RedshiftSecurityGroup",
            vpc=vpc,
            description="Redshift Serverless workgroup \u2014 pipeline access only",
            allow_all_outbound=True,
        )
        # Grant ingress from the dbt Fargate security group created above
        self.security_group.add_ingress_rule(
            peer=self.dbt_security_group,
            connection=ec2.Port.tcp(5439),
            description="Redshift 5439 from dbt Fargate tasks",
        )

        # ------------------------------------------------------------------ #
        #  Redshift Serverless namespace (logical database + IAM)             #
        # ------------------------------------------------------------------ #
        namespace = redshiftserverless.CfnNamespace(
            self,
            "Namespace",
            namespace_name="data-pipeline",
            db_name="data_pipeline",
            admin_username="admin",
            admin_user_password=admin_secret.secret_value_from_json("password").unsafe_unwrap(),
            iam_roles=[redshift_s3_role.role_arn],
            default_iam_role_arn=redshift_s3_role.role_arn,
            log_exports=["userlog", "connectionlog", "useractivitylog"],
        )
        self.namespace = namespace

        # ------------------------------------------------------------------ #
        #  Redshift Serverless workgroup (compute + networking)               #
        # ------------------------------------------------------------------ #
        # Grab private subnet IDs from the shared VPC
        private_subnet_ids = [s.subnet_id for s in vpc.private_subnets]

        workgroup = redshiftserverless.CfnWorkgroup(
            self,
            "Workgroup",
            workgroup_name="data-pipeline",
            namespace_name=namespace.namespace_name,
            base_capacity=8,  # minimum RPU \u2014 scales up under load
            publicly_accessible=False,
            subnet_ids=private_subnet_ids,
            security_group_ids=[self.security_group.security_group_id],
            enhanced_vpc_routing=False,
        )
        workgroup.add_dependency(namespace)
        self.workgroup = workgroup

        # ------------------------------------------------------------------ #
        #  SSM parameters \u2014 read by dbt Fargate (ComputeStack references     #
        #  these directly to create cross-stack deploy dependencies)         #
        # ------------------------------------------------------------------ #
        self.workgroup_param = ssm.StringParameter(
            self,
            "RedshiftWorkgroupParam",
            parameter_name="/data-pipeline/redshift/workgroup",
            string_value=workgroup.workgroup_name,
            description="Redshift Serverless workgroup name",
        )
        self.database_param = ssm.StringParameter(
            self,
            "RedshiftDatabaseParam",
            parameter_name="/data-pipeline/redshift/database",
            string_value="data_pipeline",
            description="Redshift database name",
        )
        ssm.StringParameter(
            self,
            "RedshiftS3RoleArnParam",
            parameter_name="/data-pipeline/redshift/s3_role_arn",
            string_value=redshift_s3_role.role_arn,
            description="IAM role ARN Redshift uses for COPY/UNLOAD",
        )

        # ------------------------------------------------------------------ #
        #  Outputs                                                             #
        # ------------------------------------------------------------------ #
        CfnOutput(
            self,
            "WorkgroupName",
            value=workgroup.workgroup_name,
            description="Redshift Serverless workgroup",
        )
        CfnOutput(
            self,
            "AdminSecretArn",
            value=admin_secret.secret_arn,
            description="Secrets Manager ARN for Redshift admin password",
        )
        CfnOutput(
            self,
            "RedshiftS3RoleArn",
            value=redshift_s3_role.role_arn,
            description="IAM role attached to the workgroup for S3 access",
        )
