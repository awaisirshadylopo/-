import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import os
import traceback
import time  # Added missing import
import logging
from urllib.parse import urlparse

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


# Create token
def create_token(loginUrl, username, password):
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    payload = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": username,
        "client_secret": password,
    }
    try:
        response = requests.post(loginUrl, headers=headers, data=payload)
        response.raise_for_status()
        token_data = response.json()
        token = token_data["access_token"]

        parsed = urlparse(loginUrl)
        # netloc = parsed.netloc.replace("apiidentity", "api")
        netloc = parsed.netloc.replace("apiidentity", "api").replace(
            "identityapi", "api"
        )
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("Unexpected loginUrl format")
        mls_code = parts[0]  # SACM in your case
        loginUrl = f"{parsed.scheme}://{netloc}/{mls_code}/RESO/OData"
        loginUrl = f"{loginUrl}/$metadata"

        return loginUrl, token
    except requests.exceptions.RequestException as e:
        raise Exception(
            f"Token generation failed: {response.status_code}, {response.text}"
        )


def build_or_filter(field, ids):
    return " or ".join([f"{field} eq '{i}'" for i in ids])


def api_call_and_load_tables(
    source_type,
    mls_board,
    status_column,
    source_id,
    source_name,
    batch_id,
    event,
    run_host,
    rds_connection,
    rds_cursor,
    cursor,
    connection,
    api_limit,
    inactive_threshold,
    inactive_key_field,
    loginurl,
    token,
):
    chunk_size = 20
    chunks = []
    p_active_query = "select source_listing_id from public.listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id = {};".format(
        source_id
    )
    cursor.execute(p_active_query)
    p_active_listings = cursor.fetchall()
    p_active_listings = [l[0] for l in p_active_listings]

    source_count = """ update stage.etl_batches set load_missing_lst_status = 'in-progress', batch_type= 'Inactive', source_t_counts = {0} where batch_id = {1};""".format(
        len(p_active_listings), batch_id
    )
    status_update(source_count, cursor, connection)

    chunks = [
        p_active_listings[i : i + chunk_size]
        for i in range(0, len(p_active_listings), chunk_size)
    ]

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = chunk_size
    all_list_data = []
    total_count = 0
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "$filter": "",
        "$count": "true",
        "$top": top,
        "$skip": 0,
        "$select": f"{inactive_key_field},{status_column}",
    }

    if source_id == 884 or source_id == 1001:
        # loginurl, token = create_token(auth_url, username, auth_token)
        loginurl = loginurl.replace("$metadata", "Property")
        base_url = loginurl

        params = {
            "Class": "ALL",
            "$count": "true",
            "$top": top,
            "$skip": 0,
            "$select": f"{inactive_key_field},{status_column}",
        }

        for data in chunks:
            filter_string = build_or_filter(inactive_key_field, data)
            params["$filter"] = filter_string

            try:
                response = requests.get(url=base_url, params=params, headers=headers)
                # If token expired → refresh and retry
                if response.status_code == 401:
                    # loginurl, token = create_token(auth_url, username, auth_token)
                    # headers = {"Authorization": f"Bearer {token}"}
                    response = requests.get(
                        url=base_url, params=params, headers=headers
                    )

                response.raise_for_status()
                data = json.loads(response.text)
                all_list_data.extend(data["value"])
                total_count += data["@odata.count"]

            except requests.exceptions.RequestException:
                # refresh token then retry
                time.sleep(10)
                # loginurl, token = create_token(auth_url, username, auth_token)
                # headers = {"Authorization": f"Bearer {token}"}

                response = requests.get(url=base_url, params=params, headers=headers)
                response.raise_for_status()

                data = json.loads(response.text)
                all_list_data.extend(data["value"])

    else:
        for data in chunks:
            data = str(data).replace("[", "").replace("]", "")
            params["$filter"] = "{} in ({})".format(inactive_key_field, data)

            try:
                response = requests.get(url=base_url, params=params, headers=headers)
                response.raise_for_status()
                data = json.loads(response.text)

                all_list_data.extend(data["value"])
                total_count += data["@odata.count"]
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                time.sleep(20)
                response = requests.get(
                    url=base_url, params=params, headers=headers
                )  # Fixed: was using loginurl
                response.raise_for_status()
                data = json.loads(response.text)
                all_list_data.extend(
                    data["value"]
                )  # Fixed: was using undefined property_list

    log_msg = {
        "source_name": source_name,
        "source_id": source_id,
        "downloaded_count": len(all_list_data),
    }
    logger.info(log_msg)

    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        len(all_list_data), batch_id
    )
    status_update(d_count, cursor, connection)

    df = pd.DataFrame(all_list_data)

    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.drop(columns=["@odata.id", "@odata.etag", "FeedTypes"], errors="ignore")
    df = df.rename(
        columns={status_column: "status", inactive_key_field: "source_listing_id"}
    )
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO stage.direct_idx_id ({}) VALUES %s
                    """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    return {
        **event,
        "download_status": True,
        "download_count": len(all_list_data),
        "status": True,
    }


def lambda_handler(event, context):

    logger.info(event)

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    api_limit = event["source_info"].get("limit", 200)
    inactive_threshold = event["inactive_threshold"]
    loginurl = event["auth"]["loginUrl"]
    token = event["auth"]["password"]
    status_column = event["source_info"]["status_column"]
    inactive_key_field = event["source_info"]["inactive_key_field"]

    if source_id in (1001, 884):
        token = event["auth"]["access_token"]

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
        query = f"""select batch_id from stage.etl_batches where source_id = {source_id} and load_inactive_lst_status = 'Completed' order by batch_id desc limit 1 """
        logger.info(query)
        listing_cursor.execute(query)
        batch_id = listing_cursor.fetchone()[0]
        event["batch_id"] = batch_id

        delete_query = """ 
        DELETE FROM stage.direct_idx_id where source_id = {0}
        """.format(source_id)
        listing_cursor.execute(delete_query)
        listing_conn.commit()

        event = api_call_and_load_tables(
            source_type,
            mls_board,
            status_column,
            source_id,
            source_name,
            batch_id,
            event,
            run_host,
            rds_connection,
            rds_cursor,
            listing_cursor,
            listing_conn,
            api_limit,
            inactive_threshold,
            inactive_key_field,
            loginurl,
            token,
        )

        event.update(
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

        logger.info(event)

        return event

    except Exception as e:

        final_response = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
            "success": event["success"],
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        logger.info(final_response)
        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
