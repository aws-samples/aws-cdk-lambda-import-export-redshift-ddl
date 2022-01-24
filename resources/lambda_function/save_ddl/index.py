import logging
import os
import psycopg2
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secretsmanager = boto3.client("secretsmanager")
s3 = boto3.client("s3")

s3_bucket = os.environ["DDL_BUCKET"]

def handler(event, context):
    host = event["connection"]["host"]
    port = event["connection"]["port"]
    db = event["connection"]["db"]
    user = event["connection"]["user"]
    password_secret_arn = event["connection"]["password_secret_arn"]
    schemas = event["schemas"]

    # Optional sanity checking of input can be done here

    logger.info(f"Connecting to Redshift cluster {host}:{port}/{db} as user {user}")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=get_secret_value(password_secret_arn)
    )

    response = {}
    for schema in schemas:
        ddl = generate_ddl(conn, schema)

        s3_key = f"{host}/{db}/{schema}_ddl.sql"
        s3_uri = save_ddl_to_s3(ddl, s3_key)

        response[schema] = s3_uri

    return response

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

def generate_ddl(conn, schema):
    logger.info(f"Generating DDL for {schema}")

    with conn, conn.cursor() as curs:
        with open("generate_tbl_ddl.sql", mode="r", encoding="utf-8") as sql_file:
            query = curs.mogrify(sql_file.read(), { "schemaname": schema })
        logger.debug(f"Executing query: {query}")
        curs.execute(query)
        query_result = curs.fetchall()

    # Query result is returned as list of tuples of size 1 (ddl column only) - consolidate to str for return
    return "\n".join([row_tuple[0] for row_tuple in query_result])

def save_ddl_to_s3(ddl, s3_key):
    s3_uri = f"s3://{s3_bucket}/{s3_key}"
    logging.info(f"Saving DDL to {s3_uri}")

    s3.put_object(Body=ddl, Bucket=s3_bucket, Key=s3_key)
    
    return s3_uri
