"""This Lambda function is responsible for downloading active listing."""

import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import time
import os
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_token(client_id, client_secret):
    url = "https://identity.crea.ca/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "DDFApi_Read",
        "client_id": str(client_id),
    }
    auth = (str(client_id), str(client_secret))

    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        logger.info({"message": "token generated", "status": "Success"})
        return token
    else:
        logger.error({"message": "token generation failed", "response": str(response)})
        return None


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def setup_db_connection(secret, sqlExecLimit):
    conn = psycopg2.connect(
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={sqlExecLimit}",
    )
    return conn


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


def get_total_count_from_property_endpoint(base_property_url, token):
    """
    Fetches the total number of active listings using the /Property endpoint.
    This endpoint supports $top=1, so we only fetch one record to minimise overhead.
    """
    params = {
        "$filter": "ModificationTimestamp ge 1990-01-01 and StandardStatus eq 'Active'",
        "$count": "true",
        "$top": 1,
        "$select": "ListingKey",
    }
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(base_property_url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("@odata.count", 0)


def fetch_all_replication_data(base_replication_url, token, preferred_page_size=100):
    """
    Downloads the complete master list from PropertyReplication.
    Uses @odata.nextLink for pagination (server-driven).
    Tries to set $top to preferred_page_size; if the server rejects it (400),
    automatically falls back to the default page size.
    """
    headers = {"Authorization": f"Bearer {token}"}
    all_records = []
    next_url = base_replication_url

    if preferred_page_size:
        separator = "&" if "?" in next_url else "?"
        next_url = f"{next_url}{separator}$top={preferred_page_size}"

    while next_url:
        try:
            resp = requests.get(next_url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as e:

            if resp.status_code == 400 and "$top" in next_url:
                logger.warning(
                    "$top rejected by server, falling back to default page size"
                )
                next_url = (
                    next_url.split("&$top=")[0]
                    if "&$top=" in next_url
                    else next_url.split("?$top=")[0]
                )
                continue
            else:
                raise

        data = resp.json()
        records = data.get("value", [])
        all_records.extend(records)
        logger.info(
            f"Fetched {len(records)} records (total collected: {len(all_records)})"
        )

        # Move to next page (if available)
        next_url = data.get("@odata.nextLink")

    return all_records


def api_call_and_load_tables(
    source_id, source_name, batch_id, cursor, connection, loginurl, token
):

    etl_status = f"""update stage.etl_batches
                     set load_missing_lst_status = 'in-progress'
                     where batch_id = {batch_id}"""
    status_update(etl_status, cursor, connection)

    delete_query = f"DELETE FROM stage.direct_idx_id WHERE source_id = {source_id}"
    status_update(delete_query, cursor, connection)

    base_property_url = loginurl.replace("$metadata", "Property")
    base_replication_url = loginurl.replace(
        "$metadata", "Property/PropertyReplication()"
    )

    total_count = get_total_count_from_property_endpoint(base_property_url, token)
    logger.info(
        {
            "source_id": source_id,
            "source_name": source_name,
            "total_active_listings": total_count,
        }
    )
    count_update = f"""update stage.etl_batches set source_t_counts = {total_count}
                       where batch_id = {batch_id}"""
    status_update(count_update, cursor, connection)

    if total_count == 0:

        status_update(
            f"update stage.etl_batches set load_missing_lst_status = 'completed' where batch_id = {batch_id}",
            cursor,
            connection,
        )
        return {
            "source_id": source_id,
            "batch_id": batch_id,
            "download_status": True,
            "count": 0,
            "downloaded_count": 0,
            "total_count": 0,
            "break_loop": True,
        }

    all_listings = fetch_all_replication_data(
        base_replication_url, token, preferred_page_size=100
    )
    logger.info(f"Downloaded {len(all_listings)} records in total")

    df = pd.DataFrame(all_listings)
    df["status"] = "Active"
    df["source_id"] = source_id
    df["batch_id"] = batch_id
    df = df.rename(columns={"ListingKey": "source_listing_id"})
    df = df[["source_listing_id", "status", "source_id", "batch_id"]]

    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = "source_listing_id, status, source_id, batch_id"
    insert_query = f"INSERT INTO stage.direct_idx_id ({cols}) VALUES %s"
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    downloaded_count = len(all_listings)
    status_update(
        f"update stage.etl_batches set downloaded_d_counts = {downloaded_count} where batch_id = {batch_id}",
        cursor,
        connection,
    )
    status_update(
        f"update stage.etl_batches set load_missing_lst_status = 'completed' where batch_id = {batch_id}",
        cursor,
        connection,
    )

    return {
        "source_id": source_id,
        "batch_id": batch_id,
        "download_status": True,
        "count": downloaded_count,
        "downloaded_count": downloaded_count,
        "total_count": total_count,
        "break_loop": True,
    }


def lambda_handler(event, context):
    logger.info({"message": "received", "event": event})

    listing_secret = os.environ.get("listingDatabase")
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis() - 1000
    listing_conn = setup_db_connection(listing_secrets, sqlExecLimit)
    listing_cursor = listing_conn.cursor()

    try:
        source_id = event["source_id"]
        source_name = event["source_name"]
        source_type = event["source_info"]["source_type"]
        mls_board = event["source_info"].get("mls_board")
        batch_id = event["batch_id"]
        loginurl = event["auth"]["loginUrl"]
        client_id = event["auth"]["user"]
        client_secret = event["auth"]["password"]

        token = create_token(client_id, client_secret)
        if not token:
            raise Exception("Failed to generate authentication token")

        final_response = api_call_and_load_tables(
            source_id,
            source_name,
            batch_id,
            listing_cursor,
            listing_conn,
            loginurl,
            token,
        )

        event.update(final_response)
        event["success"] = True
        event["break_loop"] = True
        event["source_type"] = source_type
        event["mls_board"] = mls_board
        event["run_host"] = event.get("run_host", "")
        return event

    except Exception as e:
        final_response = {
            "source_id": source_id if "source_id" in locals() else None,
            "source_name": source_name if "source_name" in locals() else None,
            "mls_board": mls_board if "mls_board" in locals() else None,
            "source_type": source_type if "source_type" in locals() else None,
            "batch_id": batch_id if "batch_id" in locals() else None,
            "download_status": False,
            "run_host": event.get("run_host", ""),
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "break_loop": True,
            "success": False,
        }
        logger.error({"message": "error occurred", "event": final_response})
        return final_response

    finally:
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
