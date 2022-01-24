import logging
import os
import boto3
import psycopg2
from psycopg2.extensions import AsIs
from botocore.exceptions import ClientError
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secretsmanager = boto3.client("secretsmanager")
s3 = boto3.client("s3")

def handler(event, context):
    host = event["connection"]["host"]
    port = event["connection"]["port"]
    db = event["connection"]["db"]
    user = event["connection"]["user"]
    password_secret_arn = event["connection"]["password_secret_arn"]
    ddl_s3_uris = event["ddl_s3_uris"]

    # Optional sanity checking of input can be done here

    logger.info(f"Connecting to Redshift cluster {host}:{port}/{db} as user {user}")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=get_secret_value(password_secret_arn)
    )

    for ddl_s3_uri in ddl_s3_uris:
        parsed_s3_uri = urlparse(ddl_s3_uri, allow_fragments=False)
        s3_bucket = parsed_s3_uri.netloc
        s3_key = parsed_s3_uri.path.lstrip('/')

        execute_ddl(conn, s3_bucket, s3_key)

    return {'message': 'Success'}

def get_secret_value(secret_arn: str):
    try:
        logger.debug(f"Retrieving secret value from {secret_arn}")

        get_secret_value_response = secretsmanager.get_secret_value(SecretId=secret_arn)
    except ClientError as e:
        logger.error(f"The requested secret could not be retrieved: {secret_arn}")
        raise
    else:
        if "SecretString" in get_secret_value_response:
            return get_secret_value_response["SecretString"]
        else:
            return get_secret_value_response["SecretBinary"]

def execute_ddl(conn, s3_bucket, s3_key):
    logger.info(f"Executing DDL from s3://{s3_bucket}/{s3_key}")

    s3_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    ddl_query = s3_obj["Body"].read().decode("utf-8")

    ddl_filename = os.path.basename(s3_key)
    schema = ddl_filename.partition("_ddl.sql")[0] # File name format is <schema>_ddl.sql
    create_schema(conn, schema)

    with conn, conn.cursor() as curs:
        logger.debug(f"Executing query: {ddl_query}")
        curs.execute(ddl_query)

def create_schema(conn, schema):
    logger.info(f"Creating schema if one doesn't already exist for {schema}")

    with conn, conn.cursor() as curs:
        with open("create_schema.sql", mode="r", encoding="utf-8") as sql_file:
            query = curs.mogrify(sql_file.read(), { "schemaname": AsIs(schema) })
        logger.debug(f"Executing query: {query}")
        curs.execute(query)
