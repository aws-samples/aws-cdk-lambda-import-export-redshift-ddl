from aws_cdk import (
    NestedStack,
    RemovalPolicy,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    aws_redshift as redshift
)
from constructs import Construct

class RedshiftStack(NestedStack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Generate resources required to create a Redshift CfnCluster
        self._security_group = ec2.SecurityGroup(self, f"{construct_id}SecurityGroup",
            vpc=vpc
        )

        self._secret = secretsmanager.Secret(self, f"{construct_id}Secret",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_characters="'\"\\/@ "
            ),
            removal_policy=RemovalPolicy.DESTROY
        )

        self._cluster_subnet_group = redshift.CfnClusterSubnetGroup(self, f"{construct_id}SubnetGroup",
            description="Redshift cluster subnet group",
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets]
        )

        self._cluster = redshift.CfnCluster(self, f"{construct_id}RedshiftCluster",
            cluster_type=self.node.try_get_context("redshift:cluster-type"),
            db_name="dev",
            master_username="admin",
            master_user_password=self._secret.secret_value.to_string(),
            cluster_subnet_group_name=self._cluster_subnet_group.ref,
            vpc_security_group_ids=[self._security_group.security_group_id],
            publicly_accessible=False,
            node_type=self.node.try_get_context("redshift:node-type")
        )

    @property
    def cluster(self):
        return self._cluster

    @property
    def cluster_subnet_ids(self):
        return self._cluster_subnet_group.subnet_ids
    
    @property
    def secret(self):
        return self._secret

    @property
    def security_group(self):
        return self._security_group
