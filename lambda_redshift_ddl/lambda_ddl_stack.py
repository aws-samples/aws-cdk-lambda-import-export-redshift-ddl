from aws_cdk import (
    NestedStack,
    Duration,
    RemovalPolicy,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager
)
from constructs import Construct

class LambdaDdlStack(NestedStack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket where the DDL will be stored 
        bucket = s3.Bucket(self, f"{construct_id}DDLBucket",
            versioned=True, # Keep track of past DDL versions
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # # Lambda layer for psycopg2
        psycopg2_layer = lambda_.LayerVersion(self, f"{construct_id}Psycopg2Py38Layer",
            code=lambda_.Code.from_asset("./resources/lambda_layer/layer.zip")
        )

        self._save_ddl_function = lambda_.Function(self, f"{construct_id}SaveDDLFunction",
            code=lambda_.Code.from_asset("./resources/lambda_function/save_ddl"),
            handler="index.handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            layers=[psycopg2_layer],
            environment={
                "DDL_BUCKET": bucket.bucket_name
            },
            memory_size=128,
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=vpc.private_subnets
            )
        )
        bucket.grant_write(self._save_ddl_function)

        self._execute_ddl_function = lambda_.Function(self, f"{construct_id}ExecuteDDLFunction",
            code=lambda_.Code.from_asset("./resources/lambda_function/execute_ddl"),
            handler="index.handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            layers=[psycopg2_layer],
            memory_size=128,
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=vpc.private_subnets
            )
        )
        bucket.grant_read(self._execute_ddl_function)

    def grant_secret_read_access(self, secret: secretsmanager.ISecret):
        secret.grant_read(self._save_ddl_function)
        secret.grant_read(self._execute_ddl_function)

    def grant_security_group_access(self, sg: ec2.ISecurityGroup, port: ec2.Port = ec2.Port.tcp(5439)):
        self._save_ddl_function.connections.allow_to(sg, port, f"Allow DDL Lambda security group access to {sg.security_group_id}")
        self._execute_ddl_function.connections.allow_to(sg, port, f"Allow DDL Lambda security group access to {sg.security_group_id}")
