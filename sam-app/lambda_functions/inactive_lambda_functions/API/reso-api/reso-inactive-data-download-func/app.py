# Reso in-active data download lambda
import os
import logging
import json
import traceback
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
def setup_db_connection(secret, sqlExecLimit):
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        options=f"-c statement_timeout={sqlExecLimit}",
    )
    return conn


def create_token(url, username, password, client_id, client_secret):
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "password",
        "username": username,
        "password": password,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        log_msg = {"message": "token generation failed", "response": response}
        logger.error(log_msg)


def execute_query(source_id, source_name, query, cursor, connection):
    log_msg = {"source_id": source_id, "source_name": source_name, "query": query}
    logger.info(log_msg)

    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    source_id, source_name, batch_id, loginUrl, token, source_info, cursor, connection
):
    etl_status_update = """UPDATE stage.etl_batches SET load_missing_lst_status = 'in-progress' 
                           WHERE source_id = {0} AND batch_id = {1}""".format(
        source_id, batch_id
    )
    execute_query(source_id, source_name, etl_status_update, cursor, connection)

    status_column = source_info["status_column"]
    sold_status = source_info["sold_status"]

    # chunk_size = 50
    chunk_size = source_info["chunk_size"]
    query = f"SELECT source_listing_id FROM listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id = {source_id};"
    cursor.execute(query)
    total_count = cursor.rowcount
    source_listing_id = [i[0] for i in cursor.fetchall()]

    listingkeys_chunks = [
        source_listing_id[i : i + chunk_size]
        for i in range(0, len(source_listing_id), chunk_size)
    ]

    download_data_list = []
    status_flag = True
    skip = 0
    source_total_count = 0

    for listings in listingkeys_chunks:
        eq_or_clause = " or ".join([f"ListingKey eq '{key}'" for key in listings])
        value = f"({eq_or_clause})"
        top = source_info["top"]

        params = {
            "$filter": value,
            "$top": top,
            "$count": "true",
            "$select": f"ListingKey,{status_column}",
        }

        if source_id == 639:
            params["Class"] = "ALL"

        headers = {"Authorization": f"Bearer {token}"}
        loginUrl = loginUrl.replace("$metadata", "Property")

        response = None
        try:
            response = requests.get(
                url=loginUrl, params=params, headers=headers, timeout=60
            )
            response.raise_for_status()
            data = json.loads(response.text)

            source_count = data["@odata.count"]
            source_total_count += source_count

            download_data_list.extend(data["value"])
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            status_flag = False
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "download_status": status_flag,
                "Error": e,
                "Response": str(response.text if response else "No Response"),
            }
            logger.error(log_msg)
            return status_flag

    df = pd.DataFrame(download_data_list)

    df["source_id"] = source_id
    df["batch_id"] = batch_id
    df.drop(["@odata.id", "ListingKeyNumeric"], axis=1, inplace=True, errors="ignore")
    df = df.rename(
        columns={f"{status_column}": "status", "ListingKey": "source_listing_id"}
    )

    source_count_update = """ update stage.etl_batches set source_t_counts = {0} where source_id = {1} and batch_id = {2};""".format(
        source_total_count, source_id, batch_id
    )
    execute_query(source_id, source_name, source_count_update, cursor, connection)

    downloaded_count = len(df)
    download_count_update = """UPDATE stage.etl_batches 
                               SET downloaded_d_counts = {0} 
                               WHERE source_id = {1} AND batch_id = {2};""".format(
        downloaded_count, source_id, batch_id
    )
    execute_query(source_id, source_name, download_count_update, cursor, connection)

    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))

    insert_query = """INSERT INTO stage.direct_idx_id ({}) VALUES %s""".format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    return status_flag


def lambda_handler(event, context):
    listing_secret = os.environ.get("listingDatabase")
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_conn = setup_db_connection(listing_secrets, sqlExecLimit)
    listing_cursor = listing_conn.cursor()

    source_id = event["source_id"]
    source_name = event["source_name"]
    source_info = event["source_info"]
    auth = event["auth"]
    batch_id = event["batch_id"]
    loginUrl = auth["loginUrl"]
    token = auth["password"]
    download_status = False

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "source_type": source_info["source_type"],
        "mls_board": source_info["mls_board"],
        "run_host": event["run_host"],
        "inactive_threshold": event["inactive_threshold"],
        "download_status": download_status,
        "success": False,
    }

    try:
        previous_data_deletion = (
            "DELETE FROM stage.direct_idx_id WHERE source_id = {0}".format(source_id)
        )
        execute_query(
            source_id, source_name, previous_data_deletion, listing_cursor, listing_conn
        )

        if source_id in (968, 639):
            tokenUrl = auth["tokenUrl"]
            username = auth["username"]
            password = auth["password"]
            client_id = auth["client_id"]
            client_secret = auth["client_secret"]

            token = create_token(tokenUrl, username, password, client_id, client_secret)

        download_status = api_call_and_load_tables(
            source_id,
            source_name,
            batch_id,
            loginUrl,
            token,
            source_info,
            listing_cursor,
            listing_conn,
        )

        final_response["download_status"] = download_status
        final_response["success"] = download_status
        final_response["auth"] = auth
        final_response["source_info"] = source_info

        return final_response

    except Exception as e:
        log_msg = {
            "Error": e,
            "Error At line": traceback.format_exc(),
            "Payload": final_response,
        }
        logger.error(log_msg)
        return final_response

    finally:
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
