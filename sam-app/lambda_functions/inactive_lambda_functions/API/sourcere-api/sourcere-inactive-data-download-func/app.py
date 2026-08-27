import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import os
import traceback
import time  # Added missing import
from itertools import chain
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
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


def api_call_and_load_tables(
    source_type,
    mls_board,
    source_id,
    source_name,
    batch_id,
    run_host,
    rds_connection,
    rds_cursor,
    cursor,
    connection,
    api_limit,
    inactive_threshold,
    loginurl,
    token,
    event,
):
    """
    Description:
        This function is responsible to download all active status listings
        from source without any other filters. This happens with the top = 100 parameter.

    Returns:
        Return success status and downloaded count.

    Created:    2026-05-05                  Create By : Ammar Azkar
    """
    # -------------------------------------------------------------
    # 1. Get total source_t_counts(active and inactive) listings from source
    # -------------------------------------------------------------
    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    chunk_size = 1000

    source_count_query = "select count(*) from public.listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id = {};".format(
        source_id
    )
    cursor.execute(source_count_query)
    Total_source_count = cursor.fetchone()[0]
    # Total_source_count = cursor.fetchall().int
    logger.info(
        {
            "message": "Source Records Count",
            "source_id": source_id,
            "source_name": source_name,
            "source_type": source_type,
            "count": Total_source_count,
        }
    )
    source_count = """update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        Total_source_count, batch_id
    )
    status_update(source_count, cursor, connection)
    # p_active_listings = [l[0] for l in p_active_listings]

    # -------------------------------------------------------------
    # 2. Configuring paramsters for API call
    # -------------------------------------------------------------
    status_column = event["source_info"]["status_column"]
    active_status = event["source_info"]["active_status"]

    params = {
        "$filter": f"{status_column} in ({active_status}) and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease' and PropertyType ne 'Rental'",
        "$select": f"ListingKey,{status_column}",
        "$top": chunk_size,
        "$skip": 0,
        "$count": "true",
    }
    logger.info(
        {
            "message": "Calling API",
            "source_id": source_id,
            "source_type": source_type,
            "source_name": source_name,
            "params": params,
        }
    )

    next_url = url_endpoint = loginurl.replace("$metadata", "Property")
    headers = {"Authorization": f"Bearer {token}"}
    # Total_provided_count = 0

    # -------------------------------------------------------------
    # 3. Downloading data from API in pages (chunks)
    # -------------------------------------------------------------

    Property_Active_date = []
    while next_url:
        try:
            response = requests.get(
                url=next_url,
                params=params if next_url == url_endpoint else None,
                headers=headers,
            )
            response.raise_for_status()

            data = response.json()
            data["url_endpoint"] = next_url

            Property_Active_date.extend(data.get("value", []))

            logger.info(
                {
                    "step": "API_PAGE",
                    "API_Request": next_url,
                    "total_downloaded_so_far": len(Property_Active_date),
                }
            )

            # OData pagination
            next_url = data.get("@odata.nextLink")

            # params should be used only for the first call

        except requests.exceptions.RequestException:
            time.sleep(5)
            response = requests.get(
                url=next_url, params=params if params else None, headers=headers
            )
            response.raise_for_status()

            data = response.json()
            data["url_endpoint"] = next_url
            # openhouse.append(data)
            Property_Active_date.extend(data.get("value", []))
            next_url = data.get("@odata.nextLink")

    log_msg = {
        "message": "API Download Complete",
        "source_id": source_id,
        "source_name": source_name,
        "source_type": source_type,
        "source_id": source_id,
        "total_records": Total_source_count,
        "downloaded_count": len(Property_Active_date),
    }
    logger.info(log_msg)

    # -------------------------------------------------------------
    # 4. Update downloaded count in stage.etl_batchs table
    # -------------------------------------------------------------

    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        len(Property_Active_date), batch_id
    )
    status_update(d_count, cursor, connection)

    # -------------------------------------------------------------
    # 5. Preparing Dataframe and data to insert into stage.direct_idx_id table
    # -------------------------------------------------------------
    df = pd.DataFrame(Property_Active_date)

    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.drop(columns=["@odata.id", "@odata.etag"])
    df = df.rename(
        columns={status_column: "status", "ListingKey": "source_listing_id"}
    )

    required_columns = ["source_listing_id", "status", "source_id", "batch_id"]

    df = df[required_columns]
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(required_columns)

    # -------------------------------------------------------------
    # 6. Inserting downloaded data into stage.direct_idx_id table
    # -------------------------------------------------------------
    insert_query = """
                    INSERT INTO stage.direct_idx_id ({}) VALUES %s
                    """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    return {"download_status": True, "download_count": len(Property_Active_date)}


def lambda_handler(event, context):

    logger.info(event)

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    api_limit = event["source_info"].get("limit", 1000)
    inactive_threshold = event["inactive_threshold"]
    loginurl = event["auth"]["loginUrl"]
    token = event["auth"]["password"]

    rds_secret = os.environ.get("rdsDatabase")
    listing_secret = os.environ.get("listingDatabase")
    rds_secrets = fetch_secrets(rds_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    rds_connection = setup_db_connection(rds_secrets, sqlExecLimit)
    listing_conn = setup_db_connection(listing_secrets, sqlExecLimit)
    rds_cursor = rds_connection.cursor()
    listing_cursor = listing_conn.cursor()

    try:

        delete_query = """ 
        DELETE FROM stage.direct_idx_id where source_id = {0}
        """.format(source_id)
        listing_cursor.execute(delete_query)
        listing_conn.commit()

        final_response = api_call_and_load_tables(
            source_type,
            mls_board,
            source_id,
            source_name,
            batch_id,
            run_host,
            rds_connection,
            rds_cursor,
            listing_cursor,
            listing_conn,
            api_limit,
            inactive_threshold,
            loginurl,
            token,
            event,
        )

        final_response.update(
            {
                "source_id": source_id,
                "source_name": source_name,
                "mls_board": mls_board,
                "source_type": source_type,
                "batch_id": batch_id,
                "run_host": run_host,
                "inactive_threshold": inactive_threshold,
            }
        )

        final_response["success"] = event["success"]
        final_response["auth"] = event["auth"]
        final_response["source_info"] = event["source_info"]

        logger.info(final_response)

        return final_response

    except Exception as e:

        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "download_status": False,
        }
        event.update(log_msg)
        logger.error(event)
        return event

    finally:

        if rds_cursor:
            rds_cursor.close()
        if rds_connection:
            rds_connection.close()
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
