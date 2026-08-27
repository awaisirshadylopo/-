import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime
from psycopg2 import extras
import os
import io
import sys
from io import StringIO
import numpy as np

import traceback
from itertools import chain
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
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


def create_token(client_id, client_secret):
    # OAuth token endpoint URL
    url = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
    }

    auth = (str(client_id), str(client_secret))
    response = requests.post(url, headers=headers, data=data, auth=auth)

    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        # Log token generation failure
        logs = {"Token Generation": "Failed", "Status Code": response.status_code}
        logger.error({"message": "received", "event": logs})


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    mls_board,
    source_type,
    orignating_sys_name,
    token,
    source_id,
    source_name,
    batch_id,
    run_host,
    cursor,
    connection,
    api_limit,
    inactive_threshold,
):

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    status_flag = True

    base_url = "https://api-prod.corelogic.com/trestle/odata/Property"
    top = api_limit  # Number of records to fetch in each request
    skip = 0  # Number of records to skip initially
    trestle_list_data = []
    total_count = 0
    selectec_columns = ""
    if source_id == 876999:
        selectec_columns = "ListingKey,ListingId"  # ListOffice,CoListOffice,BuyerOffice,CoBuyerOffice,ListOffice,CoListOffice,BuyerOffice,CoBuyerOffice"
    else:
        selectec_columns = "ListingKey,StandardStatus"

    while True:
        value = "OriginatingSystemName eq {} and PropertyType ne 'ResidentialLease' and StandardStatus ne 'Closed'".format(
            orignating_sys_name
        )
        params = {
            "$filter": value,
            "$count": "true",
            "$top": top,
            "$skip": skip,
            # "$select": "ListingKey,StandardStatus"
            "$select": selectec_columns,
        }

        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            trestle_list_data.append(data["value"])
            total_count = data["@odata.count"]

            # If we have received all records, break out of the loop
            if skip + top >= total_count:
                break

            # # Increment the skip parameter to fetch the next batch
            skip += top

        except requests.exceptions.RequestException as e:
            # Log exception
            status_flag = False
            msg = {
                "source_id": source_id,
                "Status Code": response.status_code,
                "Response": response.text,
                "Error": str(e),
            }
            logger.error({"message": "received", "event": msg})

            return {
                "source_id": source_id,
                "mls_board": mls_board,
                "source_type": source_type,
                "batch_id": batch_id,
                "source_response": response.text,
                "download_status": status_flag,
                "run_host": run_host,
            }

            break

    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    status_update(source_count, cursor, connection)
    flat_list = list(chain(*trestle_list_data))
    df = pd.DataFrame(flat_list)
    df["source_id"] = source_id
    df["batch_id"] = batch_id
    if source_id == 87006:
        df = df.rename(
            columns={"ListingKey": "source_listing_id", "ListingId": "status"}
        )
    else:
        df = df.rename(
            columns={"StandardStatus": "status", "ListingKey": "source_listing_id"}
        )
    # df = df.rename(columns={'StandardStatus': 'status', 'ListingKey': 'source_listing_id'})
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()
    download_count = """ select count(distinct source_listing_id) from stage.direct_idx_id where source_id = {};""".format(
        source_id
    )
    cursor.execute(download_count)
    download_count = cursor.fetchone()
    download_count = download_count[0]
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        download_count, batch_id
    )
    status_update(d_count, cursor, connection)
    status_flag = True

    return {
        "source_id": source_id,
        "mls_board": mls_board,
        "source_type": source_type,
        "batch_id": batch_id,
        "download_status": status_flag,
        "run_host": run_host,
        "inactive_threshold": inactive_threshold,
        "total_count": total_count,
    }


def lambda_handler(event, context):
    # TODO implement

    logger.info({"message": "received", "event": event})

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"]["mls_board"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    originating_system_name = event["source_info"]["originating_system_name"]
    api_limit = event["source_info"]["limit"]
    inactive_threshold = event["inactive_threshold"]

    client_id = event["auth"]["user"]
    client_secret = event["auth"]["password"]

    try:

        secret_name = os.environ.get("rdsDatabase")
        listing_secret = os.environ.get("listingDatabase")
        # secrets = fetch_secrets(secret_name)
        listing_secrets = fetch_secrets(listing_secret)
        # connection = setup_db_connection(secrets)
        listing_conn = setup_db_connection(listing_secrets)
        # cursor = connection.cursor()
        listing_cursor = listing_conn.cursor()

        if listing_conn:
            token = create_token(client_id, client_secret)
            # batch_creation_date = event['batch_creation_date']
            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
            listing_cursor.execute(delete_query)
            listing_conn.commit()

            final_response = api_call_and_load_tables(
                mls_board,
                source_type,
                originating_system_name,
                token,
                source_id,
                source_name,
                batch_id,
                run_host,
                listing_cursor,
                listing_conn,
                api_limit,
                inactive_threshold,
            )
            logger.info({"message": "received", "event": final_response})

            final_response["source_name"] = event["source_name"]
            final_response["auth"] = event["auth"]
            final_response["source_info"] = event["source_info"]
            final_response["success"] = event["success"]

            return final_response

    except Exception as e:

        final_response = {
            "source_id": source_id,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
        }
        final_response["success"] = event["success"]
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "Payload": final_response,
        }
        logger.error({"message": "received", "event": log_msg})

        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
