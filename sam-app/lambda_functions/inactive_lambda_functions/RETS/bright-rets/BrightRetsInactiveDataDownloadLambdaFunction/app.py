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
logger = logging.getLogger("mls-bright-rets-inactive-data-download-func")
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


def login(data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    session.headers = {
        "User-Agent": "RETSMD/1.0",
        "RETS-Version": "RETS/1.7.2",
        "User-Agent-Password": "123456",
    }
    # Send login request
    response = session.get(login_url)

    # Check for successful login
    if response.status_code == 200:

        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
            rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
            logger.info("Login successful!")
            rets_data["session"] = session
            return rets_data
        except Exception as e:
            log_msg = {
                "response_text": response_text,
                "response_status_code": response.status_code,
                "Error": e,
            }
            raise ConnectionError(log_msg) from e

    else:
        log_msg = {
            "response_text": response.text,
            "response_status_code": response.status_code,
        }
        raise ConnectionError(log_msg)


def data_download(data):
    """Data Download from Rets Server Function"""
    session = data["session"]
    search_url = data["Search"]
    query_params = data["query_params"]
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        # Extract column names
        count_element = root.find(".//COUNT")

        data_count = int(count_element.get("Records"))  # type: ignore
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore
        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]  # type: ignore
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except (ET.ParseError, AttributeError) as e:
        logger.info(response_text)
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if "No records found" in reply_text:  # type: ignore
            return pd.DataFrame(), 0

        log_msg = {
            "query": query_params,
            "response_text": response_text,
            "response_status_code": response.status_code,
            "Error": e,
        }
        raise ValueError(log_msg) from e


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def download_listing_for_inactive(cursor, connection, event, response):

    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]

    chunk_size = 200
    query_sold = f""" select source_listing_id from listing_p_sold where modification_timestamp = '1990-01-01' and source_id in ({source_id})  """
    query = f""" select source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id in ({source_id})"""
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    etl_status = f"""update stage.etl_batches 
    set load_missing_lst_status = 'in-progress', 
    batch_type= 'Inactive',
    source_t_counts = {len(inactive_listings)}
    where batch_id = {batch_id}"""
    status_update(etl_status, cursor, connection)
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
    for listing_chunk in inactive_listings_chunks:
        listing_count = len(listing_chunk)
        listing_chunk = ",".join(list(filter(None, listing_chunk)))
        # listing_chunk = str(listing_chunk).replace('[', '').replace(']', '').replace("'", '"')
        query = f"(ListingKey={listing_chunk})"

        query_params = {
            "SearchType": "Property",
            "Class": "ALL",
            "Query": query,
            # 'Select':'ListingKey,PropertyType'
            "Select": "ListingKey,MlsStatus",
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
    status_update(source_count, cursor, connection)
    property["source_id"] = source_id
    property["batch_id"] = batch_id

    property = property.rename(
        columns={"MlsStatus": "status", "ListingKey": "source_listing_id"}
    )
    tuple_list = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols = ",".join(list(property.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(
        cols
    )
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    return True, total_count


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

    listing_secret = os.environ.get("listingDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    listing_conn = db_conn(listing_secrets, sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    try:

        response = login(auth)
        status = False
        total_count = 0

        delete_query = """ 
        DELETE FROM stage.direct_idx_id where source_id = {0}
        """.format(
            source_id
        )
        listing_cursor.execute(delete_query)
        listing_conn.commit()
        status, total_count = download_listing_for_inactive(
            listing_cursor, listing_conn, event, response
        )
        event["download_status"] = status
        event["total_count"] = total_count
        event["source_type"] = source_type
        event["mls_board"] = mls_board

        if status:
            logger.info(event)
        else:
            logger.error(event)

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
        logger.error(final_response)
        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
