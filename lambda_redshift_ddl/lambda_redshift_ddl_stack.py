from aws_cdk import (
    Stack
)
from constructs import Construct

from lambda_redshift_ddl.vpc_stack import VpcStack
from lambda_redshift_ddl.lambda_ddl_stack import LambdaDdlStack
from lambda_redshift_ddl.redshift_stack import RedshiftStack

class LambdaRedshiftDdlStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        vpc_stack = VpcStack(self, "VpcStack")
        lambda_ddl_stack = LambdaDdlStack(self, "LambdaDdlStack", vpc=vpc_stack.vpc)

        legacy_redshift_stack = RedshiftStack(self, "LegacyRedshiftStack", vpc=vpc_stack.vpc)
        lambda_ddl_stack.grant_secret_read_access(legacy_redshift_stack.secret)
        lambda_ddl_stack.grant_security_group_access(legacy_redshift_stack.security_group)

        new_redshift_stack = RedshiftStack(self, "NewRedshiftStack", vpc=vpc_stack.vpc)
        lambda_ddl_stack.grant_secret_read_access(new_redshift_stack.secret)
        lambda_ddl_stack.grant_security_group_access(new_redshift_stack.security_group)
