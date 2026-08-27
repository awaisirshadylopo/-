import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import os
import traceback
import logging
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
import re
import time

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-matrix-rets-inactive-data-download-func")
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


def login(data, source_id):
    loginUrl = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    USER_AGENT = "Python/3.8 RETS Client/1.0"
    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    session.headers = {"rets-version": "RETS/1.7.2"}
    # Send login request
    response = session.get(loginUrl)

    response_text = response.text
    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
        logger.info("Login successful!")
        rets_data["session"] = session
        return rets_data
    except ParseError as e:
        msg = {"source_id": source_id, "message": f"Error in server login: {e}"}
        logger.error(msg)
        raise Exception(msg)


def data_download(data):
    """Data Download from RETS Server Function — Parses COMPACT-DECODED XML"""
    session = data["session"]
    search_url = data["Search"]
    query_params = data["query_params"]

    # Ensure RETS-required parameters
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
    except ET.ParseError as e:
        data["Error"] = f"Invalid XML returned from RETS: {e}"
        data["Error_Response"] = response_text[:500]
        return pd.DataFrame(), 0

    # Get RETS reply code and message
    reply_code = root.attrib.get("ReplyCode")
    reply_text = root.attrib.get("ReplyText")

    # Handle RETS error response
    if reply_code != "0":
        data["Error"] = f"RETS returned ReplyCode={reply_code}, Message={reply_text}"
        data["Error_Response"] = response_text[:500]
        return pd.DataFrame(), 0

    # Extract record count
    count_element = root.find(".//COUNT")
    data_count = int(count_element.get("Records")) if count_element is not None else 0

    # Extract columns
    columns_element = root.find(".//COLUMNS")
    if columns_element is None or not columns_element.text:
        data["Error"] = "Missing <COLUMNS> section in RETS response"
        data["Error_Response"] = response_text[:500]
        return pd.DataFrame(), data_count

    columns = [
        col.strip() for col in columns_element.text.strip().split("\t") if col.strip()
    ]

    # Extract data rows
    rows = []
    for row_elem in root.findall(".//DATA"):
        if row_elem.text:
            row_values = [
                val.strip()
                for val in row_elem.text.strip().split("\t")
                if val.strip() != ""
            ]
            if len(row_values) == len(columns):
                rows.append(row_values)

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)

    return df, data_count


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def download_listing_for_inactive(
    cursor, listing_cursor, listing_conn, event, response, key_column, status_column
):

    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    chunk_size = 50
    query = f""" select source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') AND source_id in ({source_id}) """
    listing_cursor.execute(query)
    inactive_listings = listing_cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    etl_status = f"""update stage.etl_batches set load_missing_lst_status = 'in-progress', batch_type= 'Inactive', source_t_counts = {len(inactive_listings)} where batch_id = {batch_id}"""
    status_update(etl_status, listing_cursor, listing_conn)

    inactive_listings_chunks = [
        inactive_listings[i : i + chunk_size]
        for i in range(0, len(inactive_listings), chunk_size)
    ]
    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "source_count": len(inactive_listings),
    }
    logger.info(log_msg)
    property = pd.DataFrame()

    class_query = f"select class_name from dev.class_metadata where source_id = {source_id} and download_flag ='t' and resource_name ='Property';"
    cursor.execute(class_query)
    results = cursor.fetchall()
    results = [r[0] for r in results]

    for listing_chunk in inactive_listings_chunks:
        listing_count = len(listing_chunk)
        listing_chunk = ",".join(list(filter(None, listing_chunk)))
        query = f"({key_column}={listing_chunk})"
        # if source_id == 215:
        #     Class = 'Resi'
        for class_name in results:
            query_params = {
                "SearchType": "Property",
                "Class": class_name,
                "Query": query,
                "Select": f"{key_column},{status_column}",
            }

            response["query_params"] = query_params
            # Data Downloading
            df, count = data_download(response)
            if count and count == 0:
                msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "batch_id": batch_id,
                    "listing_count": listing_count,
                    "query": query_params,
                    "msg": "No Records Found",
                }
                logger.info(msg)
            else:
                property = pd.concat([property, df], ignore_index=True)

    total_count = len(property)
    source_count = """ update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    status_update(source_count, listing_cursor, listing_conn)
    property["source_id"] = source_id
    property["batch_id"] = batch_id

    # property = property.rename(columns={'MlsStatus': 'status', 'ListingKey': 'source_listing_id'})
    property = property.rename(
        columns={f"{status_column}": "status", f"{key_column}": "source_listing_id"}
    )
    tuple_list = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols = ",".join(list(property.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(
        cols
    )
    extras.execute_values(listing_cursor, insert_query, tuple_list)
    listing_conn.commit()

    return True, total_count


def lambda_handler(event, context):

    logger.info(event)
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    key_column = event["source_info"]["key_column"]
    status_column = event["source_info"]["status_column"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    auth = event["auth"]

    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    rds_secrets = fetch_secrets(rds_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    listing_conn = db_conn(listing_secrets, sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    rds_conn = db_conn(rds_secrets, sqlExecLimit)
    rds_cursor = rds_conn.cursor()
    try:
        query = f""" select batch_id from stage.etl_batches where source_id = {source_id} and load_inactive_lst_status = 'Completed' ORDER BY BATCH_ID DESC limit 1 """
        logger.info(query)
        listing_cursor.execute(query)
        batch_id = listing_cursor.fetchone()[0]
        event["batch_id"] = batch_id
        response = login(auth, source_id)
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
                rds_cursor,
                listing_cursor,
                listing_conn,
                event,
                response,
                key_column,
                status_column,
            )
            event["download_status"] = status
            # event['total_count'] = total_count
            event["download_count"] = total_count
            event["source_type"] = source_type
            event["mls_board"] = mls_board

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
