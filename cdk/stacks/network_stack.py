"""
NetworkStack \u2014 shared VPC for the data pipeline.

Separated from ComputeStack because RedshiftStack needs to reference the
VPC, and ComputeStack needs to reference Redshift-created resources
(admin secret + SSM params). Putting the VPC in its own stack breaks
that circular dependency.

3 availability zones because Redshift Serverless requires subnets in at
least 3 AZs.
"""

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
)
from constructs import Construct


class NetworkStack(Stack):
    # Static AZ override so `cdk synth` works without AWS credentials.
    @property
    def availability_zones(self) -> list[str]:
        return [f"{self.region}a", f"{self.region}b", f"{self.region}c"]

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
