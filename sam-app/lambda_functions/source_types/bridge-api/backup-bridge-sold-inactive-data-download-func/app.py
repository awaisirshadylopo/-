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
from helper import LogData, LogMessage, log_message
import traceback
from itertools import chain


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
        log_data = LogData(event=logs)
        log_message(LogMessage("ERROR", "received", log_data))


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    source_type,
    source_id,
    source_name,
    batch_id,
    run_host,
    cursor,
    connection,
    api_limit,
    inactive_threshold,
    loginurl,
    password,
):

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress-sold' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    chunk_size = 50
    chunks = []
    # p_active_query = "select mls_number from public.listing_p_active where source_id = {};".format(source_id)
    # cursor.execute(p_active_query)
    # p_active_listings = cursor.fetchall()
    # p_active_listings = [l[0] for l in p_active_listings]

    # chunks.extend([p_active_listings[i:i + chunk_size] for i in range(0, len(p_active_listings), chunk_size)])
    # chunks = [p_active_listings[i:i + chunk_size] for i in range(0, len(p_active_listings), chunk_size)]

    status_flag = True

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = api_limit  # Number of records to fetch in each request
    skip = 0  # Number of records to skip initially
    trestle_list_data = []
    total_count = 0

    while True:

        value = "StandardStatus eq 'Closed' and IDXParticipationYN eq false"
        params = {
            "$filter": value,
            "$count": "true",
            "$top": top,
            "$skip": skip,
            "$orderby": "BridgeModificationTimestamp asc",
            "$select": "ListingKey,StandardStatus,BridgeModificationTimestamp",
        }

        headers = {"Authorization": f"Bearer {password}"}

        try:
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            for d in data["value"]:
                trestle_list_data.append(d)
            total_count = data["@odata.count"]
            if total_count == 0:
                return {
                    "source_id": source_id,
                    "mls_board": "mls_board",
                    "source_type": source_type,
                    "batch_id": batch_id,
                    "download_status": status_flag,
                    "run_host": run_host,
                    "inactive_threshold": inactive_threshold,
                    "total_count": total_count,
                }

            if skip >= 10000:
                while True:
                    last_modification_timestamp = trestle_list_data[-1][
                        "BridgeModificationTimestamp"
                    ]
                    value = f"BridgeModificationTimestamp ge {last_modification_timestamp} and StandardStatus eq 'Closed' and IDXParticipationYN eq false"
                    params = {
                        "$filter": value,
                        "$count": "true",
                        "$top": top,
                        "$skip": 0,
                        "$orderby": "BridgeModificationTimestamp asc",
                        "$select": "ListingKey,StandardStatus,BridgeModificationTimestamp",
                    }
                    try:
                        response = requests.get(
                            url=base_url, params=params, headers=headers
                        )
                        response.raise_for_status()
                        data = json.loads(response.text)
                        for d in data["value"]:
                            trestle_list_data.append(d)
                        inner_count = data["@odata.count"]

                        if top >= inner_count:
                            break
                    except requests.exceptions.RequestException as e:
                        status_flag = False
                        log_data = LogData(event=e)
                        log_message(LogMessage("ERROR", "received", log_data))
                        return {
                            "source_id": source_id,
                            "mls_board": "mls_board",
                            "source_type": source_type,
                            "batch_id": batch_id,
                            "download_status": status_flag,
                            "run_host": run_host,
                        }

            # If we have received all records, break out of the loop
            if skip + top >= total_count or skip == 10000:
                break

            # Increment the skip parameter to fetch the next batch
            skip += top

        except requests.exceptions.RequestException as e:
            # Log exception
            status_flag = False
            log_data = LogData(event=e)
            log_message(LogMessage("ERROR", "received", log_data))
            return {
                "source_id": source_id,
                "mls_board": "mls_board",
                "source_type": source_type,
                "batch_id": batch_id,
                "download_status": status_flag,
                "run_host": run_host,
            }

    log_msg = {
        "Status": status_flag,
        "Source_id": source_id,
        "total_count": total_count,
    }
    log_data = LogData(event=log_msg)
    log_message(LogMessage("INFO", "received", log_data))

    # source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(total_count,batch_id)
    # status_update(source_count,cursor,connection)

    df = pd.DataFrame(trestle_list_data)

    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.drop(columns=["@odata.id", "FeedTypes", "BridgeModificationTimestamp"])
    df = df.drop_duplicates()
    df = df.rename(
        columns={"StandardStatus": "status", "ListingKey": "source_listing_id"}
    )
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
    # d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(download_count,batch_id)
    # status_update(d_count,cursor,connection)
    status_flag = True

    return {
        "source_id": source_id,
        "mls_board": "mls_board",
        "source_type": source_type,
        "batch_id": batch_id,
        "download_status": status_flag,
        "run_host": run_host,
        "inactive_threshold": inactive_threshold,
        "total_count": total_count,
    }


def lambda_handler(event, context):
    # TODO implement

    log_data = LogData(event=event)
    log_message(LogMessage("INFO", "received", log_data))
    # print('sold_column', sold_column, type(sold_column))
    secret_name = os.environ.get("rdsDatabase")
    listing_secret = os.environ.get("listingDatabase")
    # sqlExecut
    # secrets = fetch_secrets(secret_name)
    listing_secrets = fetch_secrets(listing_secret)
    # connection = setup_db_connection(secrets)
    listing_conn = setup_db_connection(listing_secrets)
    # cursor = connection.cursor()
    listing_cursor = listing_conn.cursor()

    try:
        run_host = event["run_host"]
        source_id = event["source_id"]
        batch_id = event["batch_id"]
        api_limit = 200
        inactive_threshold = event["inactive_threshold"]
        sold_column = event.get("sold_column")

        # Fetching records from source
        select_query_2 = f"SELECT id, name, auth, originating_system_name, source_info,inactive_threshold FROM public.source WHERE id = {source_id} "

        listing_cursor.execute(select_query_2)
        rows = listing_cursor.fetchall()

        # Creating a list of dictionaries from the fetched records
        dict_list = []
        for row in rows:
            tup = {
                "source_id": row[0],
                "source_name": row[1],
                "auth": row[2],
                "source_info": row[4],
                "run_host": run_host,
                "inactive_threshold": row[5],
                "success": False,
            }
            dict_list.append(tup)

        source_type = dict_list[0]["source_info"]["source_type"]
        source_name = dict_list[0]["source_name"]
        loginurl = dict_list[0]["auth"]["loginUrl"]
        password = dict_list[0]["auth"]["password"]

        if not sold_column:
            return event
        elif "navicamls" in loginurl:
            event["is_navicamls"] = True
            return event

        if listing_conn:
            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
            listing_cursor.execute(delete_query)
            listing_conn.commit()

            final_response = api_call_and_load_tables(
                source_type,
                source_id,
                source_name,
                batch_id,
                run_host,
                listing_cursor,
                listing_conn,
                api_limit,
                inactive_threshold,
                loginurl,
                password,
            )

            final_response["source_name"] = event["source_name"]
            final_response["success"] = event["success"]
            final_response["sold_column"] = event["sold_column"]

            log_data = LogData(event=final_response)
            log_message(LogMessage("INFO", "received", log_data))

            final_response["auth"] = event["auth"]

            return final_response

    except Exception as e:
        final_response = {
            "source_id": source_id,
            "mls_board": "mls_board",
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
        }
        final_response["success"] = event["success"]
        final_response["sold_column"] = event["sold_column"]
        log_msg = {
            "Error": e,
            "Error At line": traceback.format_exc(),
            "Payload": final_response,
        }
        log_data = LogData(event=log_msg)
        log_message(LogMessage("ERROR", "received", log_data))

        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
