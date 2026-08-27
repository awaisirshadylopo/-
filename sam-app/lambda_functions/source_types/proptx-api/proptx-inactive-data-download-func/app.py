import os
import logging
import json
import traceback
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import urllib.parse

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


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    source_id, batch_id, cursor, connection, loginurl, password
):

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    status_flag = True

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = 200  # Number of records to fetch in each request
    download_data_list = []
    chunk_size = 200
    query = f""" select source_listing_id from listing where source_status in ('ACTIVE', 'INACTIVE') and source_id in ({source_id}) """
    logger.info(query)
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    log_msg = {
        "source_id": source_id,
        "Count": cursor.rowcount,
        "Message": "source_listing_id Count from Listing",
    }
    logger.info(log_msg)

    inactive_listings_chunks = [
        inactive_listings[i : i + chunk_size]
        for i in range(0, len(inactive_listings), chunk_size)
    ]

    status_flag = True
    total_count = 0
    headers = {
        "Authorization": f"Bearer {password}",
        "Content-Type": "application/json",
    }

    total_count = len(inactive_listings)
    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    status_update(source_count, cursor, connection)

    for listing_chunk in inactive_listings_chunks:
        listing_chunk = str(listing_chunk).replace("[", "").replace("]", "")
        params = {
            "$filter": f"ContractStatus ne 'Unavailable' and ListingKey in ({listing_chunk})",
            # "$filter": f'ListingKey in ({listing_chunk})',
            "$top": top,
            "$select": "ListingKey,StandardStatus",
        }
        query_string = urllib.parse.urlencode(params, safe="(),'")
        query_string = query_string.replace("+", " ")
        full_url = f"{base_url}?{query_string}"
        response = None
        try:
            response = requests.get(
                url=full_url,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            data = json.loads(response.text)
            download_data_list.extend(data["value"])

        except requests.exceptions.RequestException as e:
            status_flag = False
            log_msg = {
                "source_id": source_id,
                "Message": "Error in fetching data from Source",
                "Response": str(response.text),
                "Error": e,
            }
            logger.info(log_msg)
            return {
                "source_id": source_id,
                "batch_id": batch_id,
                "download_status": status_flag,
            }

    df = pd.DataFrame(download_data_list)
    df["source_id"] = source_id
    df["batch_id"] = batch_id
    df = df.rename(
        columns={"StandardStatus": "status", "ListingKey": "source_listing_id"}
    )
    log_msg = {
        "source_id": source_id,
        "batch_id": batch_id,
        "Count_From": len(df),
        "Message": "Downloaded Data",
    }
    logger.info(log_msg)
    total_count = len(df)
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    status_update(d_count, cursor, connection)

    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))

    insert_query = """ 
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                 """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    status_flag = True

    return {
        "source_id": source_id,
        "batch_id": batch_id,
        "download_status": status_flag,
        "count": total_count,
    }


def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    loginurl = event["auth"]["loginUrl"]
    token = event["auth"]["password"]

    listing_secret = os.environ.get("listingDatabase")
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_conn = setup_db_connection(listing_secrets, sqlExecLimit)
    listing_cursor = listing_conn.cursor()

    try:

        delete_query = """
        DELETE FROM stage.direct_idx_id where source_id = {0}
         """.format(source_id)
        listing_cursor.execute(delete_query)
        listing_conn.commit()
        # return event
        final_response = api_call_and_load_tables(
            source_id, batch_id, listing_cursor, listing_conn, loginurl, token
        )

        final_response["auth"] = event["auth"]
        final_response["source_info"] = event["source_info"]
        final_response["source_type"] = source_type
        final_response["mls_board"] = mls_board
        final_response["run_host"] = run_host
        final_response["source_name"] = event["source_name"]
        final_response["success"] = event["success"]
        final_response["inactive_threshold"] = event["inactive_threshold"]

        logger.info(final_response)
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
