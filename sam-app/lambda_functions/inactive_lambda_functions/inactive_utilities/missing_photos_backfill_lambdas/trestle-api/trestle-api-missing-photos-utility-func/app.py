"""Trestle-API Download Backdated/Missing Media"""

import json
import boto3
import pandas as pd
import requests
import psycopg2
import os
from datetime import datetime
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


# Download and update backdated Media
def Download_Missing_Media(
    ls_cursor, batch_id, source_id, auth, ls_connection, originating_system_name
):

    loginurl = "https://api-prod.corelogic.com/trestle/odata/Property"
    # password = auth['password']

    client_id = auth["user"]
    client_secret = auth["password"]
    password = create_token(client_id, client_secret)

    # loginurl = loginurl.replace("$metadata", "Property")

    query = f"""
            Delete from stage.direct_idx_photo where source_id = {source_id};
            SELECT
                l.mls_number
            FROM listing_p_active l
            WHERE l.source_id = {source_id}
            AND l.photo_count > 0
            AND  NOT EXISTS (
                SELECT 1
                FROM listing_photo lp
                WHERE lp.listing_id = l.id
            )
            ;
    """
    # query = f"""
    #     SELECT l.mls_number
    #         FROM listing l
    #         WHERE l.source_id = {source_id}
    #         AND l.photo_count > 0
    #         and mls_number in ('1347881')
    # """
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
            "Message": "Number of Listings Found With Missing Photos.",
        }
        logger.info(log_data)
    else:
        log_data = {
            "source_id": source_id,
            "Message": "No Listing Found With Missing Photos.",
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
            "$select": "ListingId,Media",
            # "$top": 200,
            "$expand": "Media($select=ResourceRecordKey,MediaURL,Order,Permission)",
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
    # logger.info(f"Dataframe created with {df} rows")
    media_list = []
    for listing in list_downloaded_data:
        media_list.extend(listing.get("Media", []))
    logger.info(f"Media list created with {len(media_list)} items")
    df_media = pd.DataFrame(media_list)
    # logger.info(f"Media dataframe created with {df_media} rows"  )
    # df = df.merge(df_media[['ResourceRecordKey', 'MediaURL', 'Order', 'Permission']], left_on='ListingId', right_on='ResourceRecordKey', how='inner')

    df_media = df_media.rename(
        columns={
            "ResourceRecordKey": "source_listing_id",
            "Order": "photo_order",
            "Permission": "permission",
            "MediaURL": "media_url",
        }
    )

    df_media["source_id"] = source_id
    df_media["batch_id"] = batch_id
    df_media["y_creation_date"] = datetime.now()

    insert_query = """
    INSERT INTO stage.direct_idx_photo (
        source_listing_id,
        photo_order,
        permission,
        media_url,
        source_id,
        batch_id,
        y_creation_date
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    data = df_media[
        [
            "source_listing_id",
            "photo_order",
            "permission",
            "media_url",
            "source_id",
            "batch_id",
            "y_creation_date",
        ]
    ].values.tolist()

    ls_cursor.executemany(insert_query, data)
    ls_connection.commit()

    Ingest_Media_Query = f"""
            INSERT INTO listing_photo(listing_id,batch_id,media_modification_timestamp,media_url,source_creation_date,source_last_update_date,y_creation_date,y_last_update_date)
            select  t.id as listing_id,t.batch_id,
                    t.media_modification_timestamp::timestamp ,s.media_url,
                    s.y_creation_date AS source_creation_date,
                    s.y_creation_date as source_last_update_date,
                    now()  AS y_creation_date,
                    now() AS y_last_update_date
            from stage.direct_idx_photo s 
            join listing_p_active t
                on s.source_listing_id=t.source_listing_id::text
                and s.source_id = {source_id} and  t.source_id = {source_id}
            left join listing_photo lp
                on t.id= lp.listing_id
            where s.source_id = {source_id} and t.source_id = {source_id} 
            and lp.listing_id is null 
            order by s.id,s.photo_order::int
        """
    ls_cursor.execute(Ingest_Media_Query)
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
        Ingest_Media_Query = f"""
            DELETE from stage.direct_idx_photo where source_id = {source_id};
        """
        ls_cursor.execute(Ingest_Media_Query)
        ls_connection.commit()
        status = Download_Missing_Media(
            ls_cursor, batch_id, source_id, auth, ls_connection, originating_system_name
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

    return event
