"""
BastionStack - small EC2 in the VPC for developer access to RDS.

Purpose: enables `aws ssm start-session --target <instance-id>
--document-name AWS-StartPortForwardingSessionToRemoteHost` to tunnel
from a developer laptop through this instance to the RDS endpoint
on port 5432. From that point, local tools (dbt, psql, DBeaver) see
RDS at localhost:5432.

Design decisions:
- t3.micro for free tier eligibility (or t4g.nano for ~$3/mo after free tier)
- No SSH key, no public IP, no inbound ports open. Access is via SSM.
- Amazon Linux 2023 (SSM agent preinstalled and auto-updated)
- Attached to the pipeline security group so it can reach RDS on 5432
- IAM role with AmazonSSMManagedInstanceCore (just enough to register with SSM)
"""

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    CfnOutput,
)
from constructs import Construct


class BastionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        pipeline_security_group: ec2.ISecurityGroup,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # IAM role - just enough for SSM to manage the instance
        role = iam.Role(
            self,
            "BastionRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
            description="Bastion EC2 role - SSM access only",
        )

        # t3.micro is free tier eligible (750 hours/month for first 12 months).
        # After that, t4g.nano is the cheapest (~$3/mo) if you leave it running.
        # For zero cost: stop the instance when not in use.
        instance_type = ec2.InstanceType.of(
            ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
        )

        # Amazon Linux 2023 - SSM agent preinstalled, gets automatic updates
        machine_image = ec2.MachineImage.latest_amazon_linux2023()

        # No SSH key, no inbound rules. Bastion joins the pipeline SG so
        # RDS already accepts its traffic on 5432 (existing self-referencing
        # ingress rule on RDS's security group).
        self.instance = ec2.Instance(
            self,
            "Bastion",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            instance_type=instance_type,
            machine_image=machine_image,
            role=role,
            security_group=pipeline_security_group,
            instance_name="data-pipeline-dev-bastion",
            require_imdsv2=True,
        )

        CfnOutput(
            self,
            "BastionInstanceId",
            value=self.instance.instance_id,
            description="Pass to scripts/dev-tunnel.py",
        )
