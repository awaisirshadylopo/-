"""Trestle-API missing Sold Date Utillity Download Lambda"""

import json
import boto3
import pandas as pd
import requests
import psycopg2
import os
import traceback
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("trestle-api-missing-sold-date-utility")
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


def create_token(client_id, client_secret):
    # OAuth token endpoint URL
    url = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
    }

    auth = (str(client_id), str(client_secret))
    response = requests.post(url, headers=headers, data=data, auth=auth)

    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        # Log token generation failure
        logs = {"Token Generation": "Failed", "Status Code": response.status_code}
        logger.error(logs)
        # log_data = LogData(event=logs)
        # log_message(LogMessage('ERROR', 'received', log_data))


def clean_value(value):
    if pd.isna(value) or str(value).strip().lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


# force fully update sold_date in target
def sold_date_update(
    ls_cursor, batch_id, source_id, auth, ls_connection, originating_system_name
):

    loginurl = "https://api-prod.corelogic.com/trestle/odata/Property"
    # password = auth['password']

    client_id = auth["user"]
    client_secret = auth["password"]
    password = create_token(client_id, client_secret)
    # loginurl = loginurl.replace("$metadata", "Property")
    # if source_id in (793, 792):

    #     query = f"""
    #         select mls_number from listing_p_sold l
    #         where source_id = {source_id}
    #         and (sold_price is not null)
    #         order by modification_timestamp desc limit 10000
    #     """
    #     ls_cursor.execute(query)
    # else:
    query = f"""
        select mls_number from listing_p_sold l 
        where source_id = {source_id} 
        and (sold_date is null or sold_price is null or sold_date::date = '1990-01-01') 
        order by modification_timestamp desc limit 3000
    """
    ls_cursor.execute(query)

    result = ls_cursor.fetchall()
    mls_numbers = []
    list_downloaded_data = []
    chunk_size = 50

    if len(result) > 0:
        mls_numbers = [t[0] for t in result]
        log_data = {
            "source_id": source_id,
            "ListingId_count": len(mls_numbers),
            "Message": "Number of Listings Found With Missing Sold_Date or Sold_Price.",
        }
        logger.info(log_data)
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

    for item in chunks:
        item = (
            str(item).replace("[", "").replace("]", "")
        )  # .replace("'","").replace(" ","")
        params = {
            "$filter": f"OriginatingSystemName eq {originating_system_name} and ListingId in ({item})",
            "$select": "ListingId,CloseDate,ClosePrice",
            "$top": 200,
        }

        headers = {"Authorization": f"Bearer {password}"}
        response = None
        try:
            response = requests.get(url=loginurl, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            list_downloaded_data.extend(data["value"])

        except requests.exceptions.RequestException as e:

            log_data = {
                "source_id": source_id,
                "Server_Response": response.text,  # type: ignore
                "Error_AT ": traceback.format_exc(),
                "Error": str(e),
            }
            raise Exception(log_data)
            # logger.error(log_data)

            return False

    log_data = {"source_id": source_id, "download_count": len(list_downloaded_data)}
    logger.info(log_data)

    if len(list_downloaded_data) == 0:
        log_data = {
            "source_id": source_id,
            "Message": "No Listing Found From Source Side.",
        }
        logger.info(log_data)
        return True
    df = pd.DataFrame(list_downloaded_data)

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

    df.insert(0, "source_id", source_id)
    df = df.fillna(pd.NaT)
    df = df.fillna("")
    df = df.apply(lambda col: col.map(clean_value))

    query = """
        update listing set sold_date = %s , sold_price = %s, batch_id = %s  where mls_number = %s and source_id  =  %s
        """

    # data_to_update = [tuple(row) for row in df[['CloseDate', 'ClosePrice', 'ListingId', 'source_id']].values]
    data_to_update = [
        tuple(row)
        for row in df[["CloseDate", "ClosePrice", "ListingId", "source_id"]]
        .assign(batch_id=batch_id)[
            ["CloseDate", "ClosePrice", "batch_id", "ListingId", "source_id"]
        ]
        .values
    ]

    ls_cursor.executemany(query, data_to_update)
    ls_connection.commit()

    log_data = {
        "source_id": source_id,
        "Update_count": len(df),
        "Status": True,
    }
    logger.info(log_data)
    return True


def lambda_handler(event, context):

    secret_name = os.environ.get("rdsDatabase")
    batch_id = event["batch_id"]
    sqlExecLimit = context.get_remaining_time_in_millis()
    secrets = fetch_secrets(secret_name)
    connection = setup_db_connection(secrets, sqlExecLimit)
    cursor = connection.cursor()

    ls_secret_name = os.environ.get("listingDatabase")
    ls_secrets = fetch_secrets(ls_secret_name)
    ls_connection = setup_db_connection(ls_secrets, sqlExecLimit)
    ls_cursor = ls_connection.cursor()

    source_id = event["source_id"]
    auth = event["auth"]
    originating_system_name = event["source_info"]["originating_system_name"]

    try:

        # ls_cursor, source_id, auth, ls_connection, originating_system_name
        # status = sold_date_update(ls_cursor, batch_id,source_id, auth, ls_connection, originating_system_name)
        status = sold_date_update(
            ls_cursor, batch_id, source_id, auth, ls_connection, originating_system_name
        )
        event["status"] = status

        # return event

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

    return event
