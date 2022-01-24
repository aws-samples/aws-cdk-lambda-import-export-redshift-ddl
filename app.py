#!/usr/bin/env python3

import aws_cdk as cdk

from lambda_redshift_ddl.lambda_redshift_ddl_stack import LambdaRedshiftDdlStack

app = cdk.App()
vpc_stack = LambdaRedshiftDdlStack(app, "LambdaRedshiftDdlStack")
app.synth()
