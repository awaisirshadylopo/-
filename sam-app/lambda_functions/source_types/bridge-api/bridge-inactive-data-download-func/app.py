import json
import boto3
import pandas as pd
import requests
import psycopg2
import time
from datetime import datetime, timedelta, timezone
from psycopg2 import extras
import os

import traceback
from itertools import chain
import logging

logger = logging.getLogger("Bridge-Inactive-Data-Download")
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


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    source_type,
    source_info,
    source_id,
    source_name,
    batch_id,
    rds_cursor,
    cursor,
    connection,
    loginurl,
    password,
):

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    chunk_size = 200
    chunks = []
    p_active_query = "select mls_number from public.listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id = {} and mls_number is not null;".format(
        source_id
    )
    cursor.execute(p_active_query)
    p_active_listings = cursor.fetchall()
    p_active_listings = [l[0] for l in p_active_listings]

    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        len(p_active_listings), batch_id
    )
    status_update(source_count, cursor, connection)
    # chunks.extend([p_active_listings[i:i + chunk_size] for i in range(0, len(p_active_listings), chunk_size)])
    chunks = [
        p_active_listings[i : i + chunk_size]
        for i in range(0, len(p_active_listings), chunk_size)
    ]

    status_flag = True

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = 200  # Number of records to fetch in each request
    skip = 0  # Number of records to skip initially
    trestle_list_data = []
    source_count = len(p_active_listings)

    custom_inactive_request_filter = source_info.get("custom_inactive_request_filter")
    status_column = source_info.get("status_column", "StandardStatus")
    select_col = f"ListingKey,{status_column}"
    if source_id == 764:
        select_col = f"ListingId,{status_column}"
    elif source_id == 924:
        select_col = "ListingKey"

    for data in chunks:
        data = str(data).replace("[", "").replace("]", "")

        filter_value = "ListingId in ({0})".format(data)

        if custom_inactive_request_filter:
            filter_value = "ListingId in ({0}) {1}".format(
                data, custom_inactive_request_filter
            )

        params = {
            "$filter": filter_value,
            "$count": "true",
            "$top": top,
            "$skip": skip,
            "$select": select_col,
        }

        headers = {"Authorization": f"Bearer {password}"}

        try:
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            response_headers = response.headers

            trestle_list_data.extend(data["value"])

            # Checking Hit limit and dynamic wait time calculating if needed
            if response_headers["Application-RateLimit-Remaining"] <= "100":
                current_time = datetime.now(timezone.utc)
                burst_rate_limit_reset = response_headers["Application-RateLimit-Reset"]
                burst_rate_limit_reset_time = datetime.strptime(
                    burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                # Convert naive datetime to UTC-aware
                burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                    tzinfo=timezone.utc
                )

                wait_time = int(
                    (burst_rate_limit_reset_time - current_time).total_seconds()
                )
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": "Property",
                    "Message": f"Application-RateLimit-Exceed Waiting for {wait_time} seconds",
                }
                logger.warning(log_msg)
                return False

            elif response_headers["Burst-RateLimit-Remaining"] <= "15":
                current_time = datetime.now(timezone.utc)
                burst_rate_limit_reset = response_headers["Burst-RateLimit-Reset"]
                burst_rate_limit_reset_time = datetime.strptime(
                    burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                # Convert naive datetime to UTC-aware
                burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                    tzinfo=timezone.utc
                )

                wait_time = int(
                    (burst_rate_limit_reset_time - current_time).total_seconds()
                )
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": "Property",
                    "Message": f"Burst-RateLimit-Exceed Waiting for {wait_time} seconds",
                }
                logger.warning(log_msg)
                time.sleep(wait_time + 3)

        except requests.exceptions.RequestException as e:
            # Log exception
            status_flag = False
            log_msg = {
                "source_id": source_id,
                "source_type": source_type,
                "batch_id": batch_id,
                "download_status": status_flag,
                "error": str(e),
                "error_at_line": traceback.format_exc(),
            }
            logger.error(log_msg)

            return log_msg

    log_msg = {
        "status": status_flag,
        "source_id": source_id,
        "downloaded_count": len(trestle_list_data),
    }
    logger.info(log_msg)

    df = pd.DataFrame(trestle_list_data)

    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.drop(columns=["@odata.id", "FeedTypes"])
    if source_id == 764:
        df = df.drop(columns=["ListingKey"])
        key_column = "ListingId"
    else:
        key_column = "ListingKey"

    df = df.rename(columns={status_column: "status", key_column: "source_listing_id"})
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
    download_count = cursor.fetchone()[0]
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        download_count, batch_id
    )
    status_update(d_count, cursor, connection)
    status_flag = True

    response = {
        "source_id": source_id,
        "source_name": source_name,
        "source_type": source_type,
        "batch_id": batch_id,
        "download_status": status_flag,
        "total_count": download_count,
    }

    return response


def lambda_handler(event, context):
    # TODO implement

    logger.info({"message": "received", "event": event})
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    source_info = event["source_info"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    loginurl = event["auth"]["loginUrl"]
    password = event["auth"]["password"]

    try:

        rds_secret = os.environ.get("rdsDatabase")
        listing_secret = os.environ.get("listingDatabase")
        rds_secrets = fetch_secrets(rds_secret)
        listing_secrets = fetch_secrets(listing_secret)
        rds_connection = setup_db_connection(rds_secrets)
        listing_conn = setup_db_connection(listing_secrets)
        rds_cursor = rds_connection.cursor()
        listing_cursor = listing_conn.cursor()

        if listing_conn:
            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
            listing_cursor.execute(delete_query)
            listing_conn.commit()

            final_response = {}
            final_response = api_call_and_load_tables(
                source_type,
                source_info,
                source_id,
                source_name,
                batch_id,
                rds_cursor,
                listing_cursor,
                listing_conn,
                loginurl,
                password,
            )
            final_response["success"] = event["success"]
            final_response["run_host"] = run_host
            final_response["mls_board"] = mls_board
            final_response["inactive_threshold"] = event["inactive_threshold"]
            final_response["auth"] = event["auth"]
            logger.info({"message": "received", "event": final_response})

            final_response["source_info"] = source_info

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
        logger.error(log_msg)

        return final_response

    finally:
        if rds_connection:
            rds_connection.close()
        if rds_cursor:
            rds_cursor.close()
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
