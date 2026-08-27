import json
import boto3
import pandas as pd
import requests
import psycopg2
from datetime import datetime
from psycopg2 import extras
import os
import traceback
import logging
from rets import *

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-connectmls-rets-inactive-data-download-func")
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def db_conn(db_secret, sqlExecLimit):
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
            options=f"-c statement_timeout={sqlExecLimit}",
        )
        response_dict_success = {"status": "Success"}
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def download_listing_for_inactive(cursor, rds_cursor, connection, event, response):

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    inactive_threshold = event["inactive_threshold"]
    inactive_key_field = event["source_info"]["inactive_key_field"]
    status_column = event["source_info"]["status_column"]

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)
    chunk_size = 50
    query = f""" select source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id in ({source_id}) """
    logger.info(query)
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    logger.info(f"source_listing_id Count from Listing {len(inactive_listings)}")

    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        len(inactive_listings), batch_id
    )
    logger.info(source_count)
    status_update(source_count, cursor, connection)

    inactive_listings_chunks = [
        inactive_listings[i : i + chunk_size]
        for i in range(0, len(inactive_listings), chunk_size)
    ]

    status_flag = True
    property = pd.DataFrame()
    total_count = len(inactive_listings)
    query = f"select class_name from dev.class_metadata where source_id = {source_id} and download_flag ='t' and resource_name ='Property';"
    logger.info(query)
    rds_cursor.execute(query)
    results = rds_cursor.fetchall()
    results = [r[0] for r in results]
    for listing_chunk in inactive_listings_chunks:
        listing_count = len(listing_chunk)
        listing_chunk = ", ".join(
            listing_chunk
        )  # str(listing_chunk).replace('[', '').replace(']', '').replace("'", '"')
        query = f"({inactive_key_field}= {listing_chunk})"
        for r in results:
            query_params = {
                "SearchType": "Property",
                "Class": r,
                "Query": query,
                "Select": f"{inactive_key_field},{status_column}",
            }
            response["query_params"] = query_params
            # Data Downloading
            df, count = data_download(response)
            if count and count == 0:
                msg_log = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "query_params": query_params,
                    "message": f" 0 Records Downloaded, Query: {query_params['Class']}",
                }
                logger.info(msg_log)

            else:
                property = pd.concat([property, df], ignore_index=True)
    status_flag = True

    property["source_id"] = source_id
    property["batch_id"] = batch_id

    total_count = len(property)
    # property = property.rename(columns={status_column: 'status', inactive_key_field: 'source_listing_id'})
    # Rename columns to match database schema
    property = property.rename(
        columns={status_column: "status", inactive_key_field: "source_listing_id"}
    )
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    logger.info(d_count)
    status_update(d_count, cursor, connection)

    tuple_list = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols = ",".join(list(property.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(
        cols
    )
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    return status_flag, total_count


def lambda_handler(event, context):

    logger.info(event)
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    inactive_threshold = event["inactive_threshold"]
    auth = event["auth"]
    status_column = event["source_info"]["status_column"]
    inactive_key_field = event["source_info"]["inactive_key_field"]
    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    rds_secret = fetch_secrets(rds_secret)
    listing_conn = db_conn(listing_secrets, sqlExecLimit)
    rds_conn = db_conn(rds_secret, sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    rds_cursor = rds_conn.cursor()
    try:
        auth["source_id"] = source_id
        response = login(auth)
        response["Login"] = auth["loginUrl"]
        response["source_id"] = source_id
        status = False
        total_count = 0
        if response and response["Login"]:

            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(
                source_id
            )
            listing_cursor.execute(delete_query)
            listing_conn.commit()
            status, total_count = download_listing_for_inactive(
                listing_cursor, rds_cursor, listing_conn, event, response
            )
            msg = {
                "source_id": source_id,
                "source_name": source_name,
                "mls_board": mls_board,
                "source_type": source_type,
                "batch_id": batch_id,
                "download_status": status,
                "run_host": run_host,
                "inactive_threshold": inactive_threshold,
                "total_count": total_count,
                "success": event["success"],
            }
            msg.update(event)
            logger.info(msg)

        return msg

    except Exception as e:

        final_response = {
            "source_id": source_id,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
            "success": event["success"],
            "Error": str(e),
            "Error At Line": traceback.format_exc(),
        }
        logger.error(final_response)
        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
