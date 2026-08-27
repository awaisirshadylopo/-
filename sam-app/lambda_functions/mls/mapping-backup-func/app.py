import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import psycopg2
import os
import json
import traceback
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def setup_db_connection(secret):
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )

    return conn


def store_df_to_s3(df, bucket_name, source_id, file_prefix, folder_timestamp):
    """
    Store a pandas DataFrame to S3 as a CSV file in a timestamped folder

    Args:
        df (pandas.DataFrame): The DataFrame to store
        bucket_name (str): The S3 bucket name
        file_prefix (str): Prefix for the file name in S3

    Returns:
        str: S3 path where file was stored
    """
    # try:
    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Create buffer to store CSV
    csv_buffer = StringIO()

    # Write DataFrame to CSV buffer
    df.to_csv(csv_buffer, index=False)

    prefix = f"{source_id}_{file_prefix}_{folder_timestamp}"
    # Create S3 file path with folder
    s3_file_path = f"{folder_timestamp}/{file_prefix}/{prefix}.csv"

    # Upload to S3
    logger.info(f"Uploading file to S3: {s3_file_path}")
    s3_client.put_object(
        Bucket=bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue()
    )
    logger.info(f"Successfully uploaded file to S3: {s3_file_path}")


def lambda_handler(event, context):
    bucket_name = os.environ.get("BucketName")
    secret_name = os.environ.get("rdsDatabase")
    secrets = fetch_secrets(secret_name)
    connection = setup_db_connection(secrets)
    cursor = connection.cursor()
    try:
        # Generate timestamp for folder name
        folder_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tables = ["etl.mappings", "etl.mapping_joins"]

        for table in tables:
            query = f"SELECT distinct source_id FROM {table}"
            cursor.execute(query)
            results = cursor.fetchall()
            source_id_list = [i[0] for i in results]
            for source_id in source_id_list:
                query = f"SELECT * FROM {table} where source_id = {source_id}"
                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])  # type: ignore
                file_prefix = table.replace(".", "_")
                store_df_to_s3(
                    df, bucket_name, source_id, file_prefix, folder_timestamp
                )

        return {"statusCode": 200, "body": "Backup completed successfully"}
    except Exception as e:
        logger.error(f"Error during backup: {str(e)}")
        logger.error(traceback.format_exc())
        return {"statusCode": 500, "body": f"Error during backup: {str(e)}"}
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logger.info("Database connection closed")
