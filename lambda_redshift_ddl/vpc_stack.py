from aws_cdk import (
    NestedStack,
    aws_ec2 as ec2
)
from constructs import Construct

class VpcStack(NestedStack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc_name = self.node.try_get_context("vpc:vpc-name")
        if vpc_name is not None:
            self._vpc = ec2.Vpc.from_lookup(self, f"{construct_id}Vpc", vpc_name=vpc_name)
        else:
            self._vpc = ec2.Vpc(self, f"{construct_id}Vpc",
                cidr=self.node.try_get_context("vpc:cidr")
            )

    @property
    def vpc(self):
        return self._vpc
