"""Commercialmls API Active Data Download"""

# Importing required libraries
import requests
import pandas as pd
import boto3
import psycopg2
from psycopg2 import extras
import logging
import traceback
import os
import json
from datetime import datetime, timezone
import time
import io

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("Commercialmls-Active-Lambda")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


# make connection with PostgreSQL
def setup_db_connection(db_secret):
    db_username = db_secret.get("username")
    db_password = db_secret.get("password")
    db_host = db_secret.get("host")
    db_name = db_secret.get("dbname")
    db_port = db_secret.get("port")
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        response_dict_success = {"status": "Success"}
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


# clean values from data frame
def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


# remove special characters from data-frame/nested lists
def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


# load data into prestage tables
def prestage_tables_insertion(
    data_df,
    source_id,
    source_name,
    resource_name,
    table_name,
    rds_cursor,
    rds_connection,
):

    data_df.fillna(pd.NaT)
    data_df.fillna("")
    data_df = data_df.apply(lambda col: col.map(remove_characters))
    data_df = data_df.apply(lambda col: col.map(clean_value))
    data_df = data_df.drop_duplicates()

    column_names = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}' and column_name not in ('id')""".format(
        table_name
    )
    rds_cursor.execute(column_names)

    table_column_names = [column[0] for column in rds_cursor.fetchall()]

    data_df.columns = data_df.columns.str.lower()
    df_cols = list(data_df.columns)

    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)

    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN IF NOT EXISTS {1} TEXT""".format(
                table_name, n
            )
            rds_cursor.execute(alter_query)
            rds_connection.commit()
            # Sync metadata for new columns
            insert_query = f""" INSERT INTO dev.field_metadata 
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name) 
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}'); 
                """
            rds_cursor.execute(insert_query)
            rds_connection.commit()

    cols = ",".join(list(data_df.columns))
    data_values = [tuple(row) for row in data_df.values]

    del data_df  # release memory

    insert_query = """
    INSERT INTO idx_stage.{0} ({1}) VALUES %s
    """.format(
        table_name, cols
    )
    extras.execute_values(rds_cursor, insert_query, data_values)
    rds_connection.commit()

    log_msg = {
        "source_id": source_id,
        "table": f"idx_stage.{table_name}",
        "insert count": len(data_values),
    }
    logger.info(log_msg)


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, resource_name
):
    # Construct filename and folder path
    filename = f"{source_name}_{resource_name}.parquet"
    folder_path = f"{source_type}/{source_id}_{source_name}/{resource_name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload.columns = df_upload.columns.map(lambda x: x.replace(".", "_"))

    # Step 3: Clean values — convert to str, then handle null-like strings
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    # Convert DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")

    # Optional: Ensure folders exist (not strictly required in S3 since it's flat storage)
    # You can create a zero-byte object as folder markers if needed
    def ensure_folder_structure(s3, bucket, path):
        parts = path.strip("/").split("/")
        cumulative_path = ""
        for part in parts:
            cumulative_path += part + "/"
            s3.put_object(Bucket=bucket, Key=cumulative_path)  # Creates "folder"

    ensure_folder_structure(s3, bucket_name, folder_path)

    # Upload parquet file
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())


# add predetermined columns' values in all prestage tables
def adding_extra_columns(
    data_df, url_endpoint, source_id, batch_id, batch_creation_date
):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    data_df.insert(0, "source_last_update_date", formatted_datetime)
    data_df.insert(0, "source_creation_date", formatted_datetime)
    data_df.insert(0, "y_last_update_date", batch_creation_date)
    data_df.insert(0, "y_creation_date", batch_creation_date)
    data_df.insert(0, "resource_name", url_endpoint)
    data_df.insert(0, "batch_id", int(batch_id))
    data_df.insert(0, "source_id", int(source_id))
    return data_df


# Request API for all resources present in class_metadata
def request_source(source_id, source_name, login_url, url_endpoint, password):

    try:
        selection_fields = "*"
        response_format = "JSON"

        # 1 request in 10 minutes; date filter do not work
        request_url = f"{login_url}/{url_endpoint}?fields={selection_fields}&token={password}&format={response_format}"

        response = requests.get(url=request_url)

        if response.status_code != 200:
            raise Exception(f"{response.status_code}: {response.text}")

        data = response.json()  # list of dicts
        data_df = pd.json_normalize(data)

        data_df.insert(0, "request_url", request_url)

        return True, data_df

    except Exception as e:
        log_msg = {"source_id": source_id, "source_name": source_name, "Error": str(e)}
        logger.error(log_msg)
        return False, pd.DataFrame()


# call download
def download_func(source_data, rds_cursor, rds_connection):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    last_modification_date = source_data["last_modification_date"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    flow_type = source_data["flow_type"]
    run_host = source_data["run_host"]
    source_type = source_data["source_info"]["source_type"]
    loginurl = source_data["auth"]["loginUrl"]
    password = source_data["auth"]["password"]
    runtime_count = source_data["runtime_count"]

    query = f" select resource_name from dev.class_metadata where source_id = {source_id} and download_flag = 't'; "
    rds_cursor.execute(query)
    url_endpoints = [end_point[0] for end_point in rds_cursor.fetchall()]

    for url_endpoint in url_endpoints:
        status_flag, data_df = request_source(
            source_id, source_name, loginurl, url_endpoint, password
        )

        if status_flag is True:
            data_df = adding_extra_columns(
                data_df, url_endpoint, source_id, batch_id, batch_creation_date
            )

            Upload_data_into_S3_DataLake(
                data_df, source_id, source_type, source_name, batch_id, url_endpoint
            )

            data_df.drop(columns=["request_url"], inplace=True, axis=1)

            prestage_tables_insertion(
                data_df,
                source_id,
                source_name,
                url_endpoint,
                "ps_commercialmls_listing",
                rds_cursor,
                rds_connection,
            )

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "source_type": source_type,
        "mls_board": source_data["source_info"]["mls_board"],
        "batch_creation_date": batch_creation_date,
        "last_refresh_date": last_modification_date,
        "status": status_flag,
        "run_host": run_host,
        "flow_type": flow_type,
    }
    return final_response


def lambda_handler(event, context):

    source_data = event

    try:

        if source_data["download_flag"]:

            # db_connections
            rdsDatabase = os.environ.get("rdsDatabase")
            rds_secrets = fetch_secrets(rdsDatabase)
            rds_connection = setup_db_connection(rds_secrets)
            rds_cursor = rds_connection.cursor()

            # download
            final_response = download_func(source_data, rds_cursor, rds_connection)

            return final_response

        else:
            # validation
            """
            Source allows only 1 request in 10 minutes; hence validation's output is hard-coded.
            """
            rds_cursor, rds_connection = (None, None)

            current_datetime = datetime.now()
            source_data["latest_listing_date"] = current_datetime.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            source_data["last_modification_date"] = "1990-01-01 00:00:00"
            source_data["row_count"] = 1

            respecs_flag = source_data["batch_execution_params"]["respecs_flag"]
            flow_type = "respecs" if respecs_flag is True else "full_load"

            source_data["flow_type"] = flow_type
            source_data["download_flag"] = True

            return source_data

    except Exception as e:

        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At Line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)

        return source_data

    finally:
        if rds_cursor:
            rds_cursor.close()
        if rds_connection:
            rds_connection.close()
