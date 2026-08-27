"""Lambda function for updating missing sold date and sold price in listings."""

import os
import traceback
import logging
import json
import re
import xml.etree.ElementTree as ET
import boto3
import pandas as pd
import psycopg2
import requests
from requests.auth import HTTPBasicAuth


logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    """Fetches secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def db_conn(db_secret, sql_execlimit):
    """
    Establishes a connection to the PostgreSQL database.
    """
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
            options=f"-c statement_timeout={sql_execlimit}",
        )
        logger.info("Connection established successfully")
        return connection
    except ConnectionError as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


def login(data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    # Create a session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    session.auth = auth
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
    query_params["Limit"] = 1000

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
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if "No records found" in reply_text:  # type: ignore
            return pd.DataFrame(), 0

        log_msg = {
            "response_text": response_text,
            "response_status_code": response.status_code,
            "Query": query_params,
            "Error": e,
        }
        raise ValueError(log_msg) from e


def status_update(query, cursor, connection):
    """Executes a status update query on the database."""
    cursor.execute(query)
    connection.commit()


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


# Function to make API call and load tables
def download_and_update_sold_date(cursor, rds_cursor, connection, event, response):
    """Downloads listings for inactive properties and updates the database."""
    source_id = event["source_id"]
    source_name = event["source_name"]
    sold_column = event["source_info"]["sold_column"]
    batch_id = event["batch_id"]
    originating_system_name = event["source_info"]["originating_system_name"]
    limit = event["source_info"].get("limit", 1000)

    chunk_size = 200
    query = f""" select source_listing_id from listing_p_sold
    where source_id = {source_id} and
    (sold_date is null or sold_price is null or sold_date = '1990-01-01') 
    order by modification_timestamp desc limit {limit};"""
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    if len(inactive_listings) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No Missing Sold Date and Sold Price Listings",
        }
        logger.info(log_msg)
        return

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "listing_count": len(inactive_listings),
        "message": "Missing Sold Date and Sold Price Listings",
    }
    logger.info(log_msg)

    inactive_listings_chunks = [
        inactive_listings[i : i + chunk_size]
        for i in range(0, len(inactive_listings), chunk_size)
    ]

    property_df = pd.DataFrame()

    query = f""" select class_name from dev.class_metadata
     where source_id = {source_id}
     and download_flag ='t' 
     and resource_name ='Property';
    """
    rds_cursor.execute(query)
    results = rds_cursor.fetchall()
    classes = [r[0] for r in results]

    for listing_chunk in inactive_listings_chunks:
        listing_chunk = ",".join(
            listing_chunk
        )  # str(listing_chunk).replace('[', '').replace(']', '').replace("'", '"')
        query = f"(OriginatingSystemName={originating_system_name}),(ListingKey={listing_chunk})"
        for class_name in classes:
            query_params = {
                "SearchType": "Property",
                "Class": class_name,
                "Query": query,
                "Select": f"ListingKey,{sold_column},ClosePrice",
            }
            response["query_params"] = query_params
            # Data Downloading
            df, count = data_download(response)
            if count == 0:
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "message": f"No Records Found for {class_name} with Query: {query_params}",
                }
                logger.info(log_msg)
            else:
                property_df = pd.concat([property_df, df], ignore_index=True)

    if len(property_df) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No Records Downloaded from Source",
        }
        logger.info(log_msg)
        return

    property_df["source_id"] = source_id
    property_df = property_df.apply(lambda col: col.map(clean_value))
    property_df = property_df.rename(
        columns={
            "ClosePrice": "sold_price",
            "ListingKey": "source_listing_id",
            sold_column: "sold_date",
        }
    )

    query = """
        update listing_p_sold set sold_date = %s , sold_price = %s, batch_id = %s where source_listing_id = %s and source_id  =  %s
        """

    # data_to_update = [
    #     tuple(row)
    #     for row in property_df[
    #         ["sold_date", "sold_price", "source_listing_id", "source_id"]
    #     ].values
    # ]
    data_to_update = [
        tuple(row)
        for row in property_df[
            ["sold_date", "sold_price", "source_listing_id", "source_id"]
        ]
        .assign(batch_id=batch_id)[
            ["sold_date", "sold_price", "batch_id", "source_listing_id", "source_id"]
        ]
        .values
    ]
    cursor.executemany(query, data_to_update)
    connection.commit()

    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "Update_count": len(property_df),
        "Status": True,
    }
    logger.info(log_data)


def lambda_handler(event, context):
    """Main Lambda function handler."""
    logger.info(event)
    auth = event["auth"]

    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    sql_execlimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    rds_secret = fetch_secrets(rds_secret)
    listing_conn = db_conn(listing_secrets, sql_execlimit)
    rds_conn = db_conn(rds_secret, sql_execlimit)
    listing_cursor = listing_conn.cursor()  # type: ignore
    rds_cursor = rds_conn.cursor()  # type: ignore
    try:
        event["download_status"] = False

        response = login(auth)
        # response["Login"] = auth["loginUrl"]

        download_and_update_sold_date(
            listing_cursor, rds_cursor, listing_conn, event, response
        )

        event["download_status"] = True

        return event

    except Exception as e:

        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(log_msg)
        logger.error(event)
        return event

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
