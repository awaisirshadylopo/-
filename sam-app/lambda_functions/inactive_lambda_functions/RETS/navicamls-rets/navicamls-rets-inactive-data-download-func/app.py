import json
import boto3
import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
import psycopg2
from psycopg2 import extras
import os
import re
import traceback
import xml.etree.ElementTree as ET
from json import JSONDecodeError
import logging

logger = logging.getLogger("Navicamls-rets-inactive-data-download-func")
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
    loginUrl = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    session.headers = {"rets-version": "RETS/1.8"}
    # Send login request
    response = session.get(loginUrl)

    # Check for successful login
    if response.status_code == 200:

        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
            rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
            logger.info("Rets Login successful!")
            rets_data["session"] = session
            return rets_data
        except (ET.ParseError, AttributeError) as parse_error:
            try:
                response_json = json.loads(response_text)
                error_code = response_json["error"]["code"]
                error_message = response_json["error"]["message"]
                logger.error("Login failed with error code: %s", error_code)
                logger.error("Error message: %s", error_message)
            except JSONDecodeError as json_error:
                logger.error("Login failed with error: %s", response_text)
            return None
    else:
        logger.error(f"Login failed! Status code: {response.status_code}")
        logger.error(response.text)
        return None


def data_download(data):
    session = data["session"]
    source_id = data["source_id"]
    source_name = data["source_name"]
    query_params = data["query_params"]
    search_url = data["Login"]
    search_url = data["Search"]
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

    except Exception as e:
        try:
            root = ET.fromstring(response_text)
            reply_text = root.find("RETS-STATUS")
            reply_text = reply_text.attrib.get("ReplyText")
            if "No Records Found" in reply_text:  # type: ignore
                #   log_msg= {
                #     "surce_id": source_id,
                #     "source_name": source_name,
                #     "Resource Name": query_params['SearchType'],
                #     "Message": f"No Records Found for class name {query_params['Class']}",
                #     "query" : query_params['Query']
                #   }
                #   logger.info(log_msg)
                return pd.DataFrame(), 0
            log_msg = {
                "surce_id": source_id,
                "source_name": source_name,
                "Resource Name": query_params["SearchType"],
                "query": query_params["Query"],
                "source_response": reply_text,
            }
            logger.error(log_msg)
            return pd.DataFrame(), None
        except Exception as e:
            log_msg = {
                "surce_id": source_id,
                "source_name": source_name,
                "Resource Name": query_params["SearchType"],
                "query": query_params["Query"],
                "source_response": response_text,
            }
            logger.error(log_msg)
            return pd.DataFrame(), None


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def download_listing_for_inactive(cursor, rds_cursor, connection, event, response):

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    # status_column = event['source_info']['status_column']
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    inactive_threshold = event["inactive_threshold"]
    response["source_id"] = source_id
    response["source_name"] = source_name
    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)
    chunk_size = 100
    query = f""" select source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id in ({source_id}) """
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]

    inactive_listings_chunks = [
        inactive_listings[i : i + chunk_size]
        for i in range(0, len(inactive_listings), chunk_size)
    ]

    property = pd.DataFrame()
    total_count = len(inactive_listings)
    query = f"select class_name from dev.class_metadata where source_id = {source_id} and download_flag ='t' and resource_name ='Property';"
    rds_cursor.execute(query)
    results = rds_cursor.fetchall()
    results = [r[0] for r in results]
    for listing_chunk in inactive_listings_chunks:
        listing_count = len(listing_chunk)
        listing_chunk = ", ".join(
            listing_chunk
        )  # str(listing_chunk).replace('[', '').replace(']', '').replace("'", '"')
        query = f"(MST_MLS_NUMBER={listing_chunk})"
        for r in results:
            query_params = {
                "SearchType": "Property",
                "Class": r,
                "Query": query,
                "Select": "MST_MLS_NUMBER,Property_Status",
            }
            response["query_params"] = query_params
            # Data Downloading
            df, count = data_download(response)
            if count and count == 0:
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "query": query_params["Class"],
                    "Message": f"{len(df)} Records Downloaded",
                }
                logger.info(log_msg)
            elif count is None:
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "query": query_params["Class"],
                    "Message": "Error During Downloading",
                }
                logger.error(log_msg)
                return False, None
            else:
                property = pd.concat([property, df], ignore_index=True)
    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    status_update(source_count, cursor, connection)
    property["source_id"] = source_id
    property["batch_id"] = batch_id

    property = property.rename(
        columns={"Property_Status": "status", "MST_MLS_NUMBER": "source_listing_id"}
    )
    tuple_list = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols = ",".join(list(property.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        len(property), batch_id
    )
    status_update(d_count, cursor, connection)

    return True, len(property)


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
    rds_secret = os.environ.get("rdsDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    rds_secret = fetch_secrets(rds_secret)
    listing_conn = db_conn(listing_secrets, sqlExecLimit)
    rds_conn = db_conn(rds_secret, sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    rds_cursor = rds_conn.cursor()
    try:

        response = login(auth)
        if response is None:
            event.update({"download_status": False, "error": "Rets Login Failed"})
            logger.error(event)
            return event
        response["Login"] = auth["loginUrl"]
        status = False
        total_count = 0
        if response and response["Login"]:

            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
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
                "Download_count": total_count,
                "success": event["success"],
            }
            logger.info(msg)

            msg.update({"source_info": event["source_info"], "auth": auth})

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
            "Error_at": traceback.format_exc(),
        }
        logger.error(final_response)
        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
