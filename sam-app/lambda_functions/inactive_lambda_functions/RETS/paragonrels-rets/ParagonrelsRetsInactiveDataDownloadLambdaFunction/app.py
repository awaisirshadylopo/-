import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import os
import traceback
import logging
import re
import certifi
import ssl
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET

logger = logging.getLogger("paragonrels-rets-inactive-data-download-func")
logger.setLevel(logging.INFO)


class HostHeaderSSLAdapter(HTTPAdapter):
    def __init__(self, dest_host, **kwargs):
        self.dest_host = dest_host
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        kwargs["ssl_context"] = context
        kwargs["server_hostname"] = self.dest_host  # Force SNI to match
        self.poolmanager = PoolManager(*args, **kwargs)


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


def login_rets(loginUrl, username, password, USER_AGENT):
    session = requests.Session()

    parsed_url = urlparse(loginUrl)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    cert_hostname = "rets.paragonrels.com"

    adapter = HostHeaderSSLAdapter(dest_host=cert_hostname)
    session.mount("https://", adapter)

    session.auth = (username, password)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "RETS-Version": "RETS/1.7.2",
            "Accept": "*/*",
            "Host": parsed_url.netloc,
        }
    )

    try:
        response = session.get(loginUrl)

        response.raise_for_status()
        return response, session

    except requests.exceptions.SSLError as e:
        logger.error(f"SSL verification failed: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None


def login(data):
    loginUrl = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    source_id = data["source_id"]
    source_name = data["name"]
    USER_AGENT = "Python/3.8 RETS Client/1.0"

    # Create a session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    headers = {"rets-version": "RETS/1.8"}
    session.headers.update(headers)
    session.auth = auth

    response = None

    # Send login request
    if source_id in [798, 604]:
        response, session = login_rets(loginUrl, username, password, USER_AGENT)
    else:
        response = session.get(loginUrl)

    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        logger.info(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()
        logger.info(rets_response_text)
        # rets_data = dict(re.findall(r'(\\w+)=([^\  \n\\r]*)', rets_response_text))
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))

        logger.info("Login successful!")
        rets_data["source_id"] = source_id
        rets_data["name"] = source_name
        rets_data["session"] = session
        return rets_data

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.get("ReplyText")
        log_msg = {
            "Level": "Error",
            "Source": f"ID is: {source_id} and Name is: {source_name}",
            "Function": "login()",
            "Message": reply_text,
        }
        logging.error(log_msg)
        return None

    logger.error(f"Login failed! Status code: {response.status_code}")
    logger.error(response.text)
    return None


def data_download(data):
    session = data["session"]
    query_params = data["query_params"]
    search_url = data["Login"]
    if "paragonrels" in search_url:
        search_url = search_url.split("/rets")[0] + data["Search"]
    else:
        search_url = data["Search"]
    # query_params['rets-version']= 'rets/1.8'
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        # Extract column names
        count_element = root.find(".//COUNT")
        data_count = int(count_element.get("Records"))
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]
        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except Exception as e:
        try:
            root = ET.fromstring(response_text)
            reply_text = root.attrib.get("ReplyText")
            if "No Records Found" in reply_text:
                logger.warning(f"{reply_text} Warning {e}")
                return pd.DataFrame(), 0

            logger.error(f"{reply_text} Error {e}")
            return pd.DataFrame(), None
        except Exception as e:
            logger.error(f"{response_text} Error {e}")
            return pd.DataFrame(), None


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

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)
    chunk_size = 200
    query = f""" select source_listing_id from public.listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id in ({source_id}) """

    if source_id == 281:
        query = f""" select source_listing_id from public.listing_p_active where source_id in ({source_id}) """

    logger.info(query)
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    logger.info(f"source_listing_id Count from Listing {len(inactive_listings)}")

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
        query = f"(ListingID= {listing_chunk})"
        for r in results:
            query_params = {
                "SearchType": "Property",
                "Class": r,
                "Query": query,
                "Select": "L_ListingID,L_Status",
            }
            response["query_params"] = query_params
            # Data Downloading
            df, count = data_download(response)
            if count and count == 0:
                msg = f'source_id: {source_id} {len(df)} Records Downloaded, Query: {query_params["Class"]}'
                logger.info(msg)
            elif count is None:
                msg = f"source_id: {source_id}  {len(df)} Downloaded Error, Query: {query_params}"
                logger.error(msg)
                status_flag = False
                return status_flag, None
            else:
                property = pd.concat([property, df], ignore_index=True)
    status_flag = True
    source_count = """ update stage.etl_batches set source_t_counts = {0}, inactive_threshold = {2} where batch_id = {1};""".format(
        total_count, batch_id, inactive_threshold
    )
    logger.info(source_count)
    status_update(source_count, cursor, connection)
    property["source_id"] = source_id
    property["batch_id"] = batch_id

    total_count = len(property)
    property = property.rename(
        columns={"L_Status": "status", "L_ListingID": "source_listing_id"}
    )
    tuple_list = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols = ",".join(list(property.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        total_count, batch_id
    )
    logger.info(d_count)
    status_update(d_count, cursor, connection)

    return status_flag, total_count


def lambda_handler(event, context):

    logger.info(event)
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    source_id = event["source_id"]
    inactive_threshold = event["inactive_threshold"]
    auth = event["auth"]
    auth["source_id"] = source_id
    auth["name"] = source_name

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
                "total_count": total_count,
                "success": event["success"],
            }
            logger.info(msg)

            msg.update({"auth": auth, "source_info": event["source_info"]})

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
