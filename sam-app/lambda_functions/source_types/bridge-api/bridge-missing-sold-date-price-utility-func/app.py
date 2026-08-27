"""Bridge missing Sold Date Utillity Download Lambda"""

import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone, timedelta
import os
import time
import traceback
import itertools
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("Bridge-missing-sold-date-utility")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


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


def clean_value(value):
    if pd.isna(value) or str(value).strip().lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


# force fully update sold_date in target
def sold_date_update(listing_cursor, source_id, source_name, auth, ls_connection):

    loginurl = auth["loginUrl"]
    password = auth["password"]
    loginurl = loginurl.replace("$metadata", "Property")

    query = f"""
        select mls_number from listing_p_sold l  where source_id = {source_id} and (sold_date is null or sold_price is null) order by modification_timestamp limit 10000
    """
    listing_cursor.execute(query)

    result = listing_cursor.fetchall()
    mls_numbers = []
    list_downloaded_data = []
    chunk_size = 200

    if len(result) > 0:
        mls_numbers = [t[0] for t in result]
    else:
        log_data = {
            "source_id": source_id,
            "Message": "No Listing Found With Missing Sold_Date or Sold_Price.",
        }
        logger.info(log_data)
        return True

    chunks = [
        mls_numbers[i : i + chunk_size] for i in range(0, len(mls_numbers), chunk_size)
    ]

    for data in chunks:
        data = str(data).replace("[", "(").replace("]", ")")
        params = {
            "$filter": f"ListingId in {data}",
            "$top": 200,
            "$select": "ListingId,CloseDate,ClosePrice",
        }

        headers = {"Authorization": f"Bearer {password}"}
        response = None
        try:
            response = requests.get(url=loginurl, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            response_headers = response.headers
            list_downloaded_data.extend(data["value"])

            # Checking Hit limit and dynamic wait time calculating if needed
            if response_headers["Application-RateLimit-Remaining"] <= "100":
                current_time = datetime.now(timezone.utc)
                burst_rate_limit_reset = response_headers["Application-RateLimit-Reset"]
                burst_rate_limit_reset_time = datetime.strptime(
                    burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                # Convert naive datetime to UTC-aware
                burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                    tzinfo=timezone.utc
                )

                wait_time = int(
                    (burst_rate_limit_reset_time - current_time).total_seconds()
                )
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": "Property",
                    "Message": f"Application-RateLimit-Exceed Waiting for {wait_time} seconds",
                }
                logger.warning(log_msg)
                return False

            elif response_headers["Burst-RateLimit-Remaining"] <= "10":
                current_time = datetime.now(timezone.utc)
                burst_rate_limit_reset = response_headers["Burst-RateLimit-Reset"]
                burst_rate_limit_reset_time = datetime.strptime(
                    burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                # Convert naive datetime to UTC-aware
                burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                    tzinfo=timezone.utc
                )

                wait_time = int(
                    (burst_rate_limit_reset_time - current_time).total_seconds()
                )
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": "Property",
                    "Message": f"Burst-RateLimit-Exceed Waiting for {wait_time} seconds",
                }
                logger.warning(log_msg)
                time.sleep(wait_time + 3)

        except requests.exceptions.RequestException as e:

            log_data = {
                "source_id": source_id,
                "Server_Response": response.text,  # type: ignore
                "Error_AT ": traceback.format_exc(),
                "Error": str(e),
            }
            logger.error(log_data)

            return False

    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "download_count": len(list_downloaded_data),
    }
    logger.info(log_data)
    if len(list_downloaded_data) == 0:
        log_data = {
            "source_id": source_id,
            "source_name": source_name,
            "download_count": len(list_downloaded_data),
            "message": "No count found form source",
        }
        return True

    df = pd.DataFrame(list_downloaded_data)
    df = df.drop(columns=["@odata.id", "FeedTypes"], errors="ignore")

    df.insert(0, "source_id", source_id)
    if "ClosePrice" not in df.columns:
        df.insert(0, "ClosePrice", None)
        log_data = {
            "source_id": source_id,
            "ListingId": df["ListingId"].values.tolist(),
            "Message": "No Column ClosePrice Found From Source Side.",
        }
        logger.info(log_data)
    if "CloseDate" not in df.columns:
        df.insert(0, "CloseDate", None)
        log_data = {
            "source_id": source_id,
            "ListingId": df["ListingId"].values.tolist(),
            "Message": "No Column CloseDate Found From Source Side.",
        }
        logger.info(log_data)

    df.fillna(pd.NaT)  # type: ignore
    df.fillna("")
    df = df.apply(lambda col: col.map(clean_value))
    df = df.dropna(subset=["CloseDate", "ClosePrice"], how="all")

    query = """
        update listing_p_sold set sold_date = %s , sold_price = %s where mls_number = %s and source_id  =  %s
        """

    data_to_update = [
        tuple(row)
        for row in df[["CloseDate", "ClosePrice", "ListingId", "source_id"]].values
    ]

    listing_cursor.executemany(query, data_to_update)
    ls_connection.commit()

    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "Update_count": len(df),
        "Status": True,
    }
    logger.info(log_data)
    return True


def lambda_handler(event, context):

    secret_name = os.environ.get("rdsDatabase")
    # sqlExecLimit= context.get_remaining_time_in_millis()
    sqlExecLimit = 900000
    secrets = fetch_secrets(secret_name)
    connection = setup_db_connection(secrets, sqlExecLimit)
    cursor = connection.cursor()

    ls_secret_name = os.environ.get("listingDatabase")
    ls_secrets = fetch_secrets(ls_secret_name)
    ls_connection = setup_db_connection(ls_secrets, sqlExecLimit)
    ls_cursor = ls_connection.cursor()

    source_id = event["source_id"]
    auth = event["auth"]
    source_name = event["source_name"]
    try:

        status = sold_date_update(
            ls_cursor, source_id, source_name, auth, ls_connection
        )
        event["status"] = status
        return event
    except Exception as e:

        event["status"] = False

        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }  # "Payload": final_response}
        event.update(log_msg)
        logger.error(event)

        return event

    finally:

        if cursor:
            cursor.close()
        if connection:
            connection.close()
        if ls_cursor:
            ls_cursor.close()
        if ls_connection:
            ls_connection.close()
