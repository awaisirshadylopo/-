"""R-API Download Backdated/Missing Media"""

import os
import logging
import json
import traceback
from datetime import datetime
import boto3
import pandas as pd
import requests
import psycopg2
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    """
    Fetch secrets from AWS Secrets Manager
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def setup_db_connection(secret, sql_exec_limit):
    """
    Setup database connection with statement timeout
    """
    return psycopg2.connect(
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={sql_exec_limit}",
    )


def clean_value(val):
    """
    Clean values by stripping whitespace and handling NaN
    """
    if pd.isna(val):
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def create_token(auth_data):
    """
    Generate authentication token for RealComp API
    Supports both 'user/password' and 'client_id/client_secret'
    """
    client_id = auth_data.get("client_id") or auth_data.get("user")
    client_secret = auth_data.get("client_secret") or auth_data.get("password")
    audience = auth_data.get("audience", "rcapi.realcomp.com")
    login_url = auth_data.get("loginUrl", "https://auth.realcomp.com/Token")

    if not client_id or not client_secret:
        raise ValueError(
            "Missing client_id/user or client_secret/password in auth_data"
        )

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "rcapi.realcomp.com",
        "grant_type": "client_credentials",
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url=login_url, headers=headers, data=json.dumps(payload), timeout=30
        )
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(
            {
                "message": "Failed to get token from RealComp Auth API",
                "error": str(e),
                "payload": {"client_id": client_id, "audience": audience},
            }
        )
        raise


def download_and_update_missing_photos(cursor, connection, event):
    """
    Main function to download missing photos and update database
    """
    source_id = event["source_id"]
    source_name = event.get("source_name")
    batch_id = event.get("batch_id", 0)

    auth_data = event["auth"]
    login_url = auth_data.get("metadataUrl", "").replace("$metadata", "Property")
    logger.info(f"Login URL: {login_url}")

    delete_query = f"""
        DELETE FROM stage.direct_idx_photo WHERE source_id = {source_id};
    """
    cursor.execute(delete_query)
    connection.commit()
    logger.info(
        f"Deleted existing records from stage.direct_idx_photo for source_id: {source_id}"
    )

    query = f"""
        SELECT
            l.mls_number
        FROM listing_p_active l
        WHERE l.source_id = {source_id}
        AND l.photo_count > 0
        AND NOT EXISTS (
            SELECT 1
            FROM listing_photo lp
            WHERE lp.listing_id = l.id
        )
    """
    cursor.execute(query)
    result = cursor.fetchall()

    if len(result) == 0:
        log_data = {
            "source_id": source_id,
            "source_name": source_name,
            "Message": "No Listing Found With Missing Photos.",
        }
        logger.info(log_data)
        return True

    mls_numbers = [str(t[0]) for t in result]
    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "ListingId_count": len(mls_numbers),
        "Message": "Number of Listings Found With Missing Photos.",
    }
    logger.info(log_data)

    try:
        token = create_token(auth_data)
        headers = {"User-Agent": "Ylopo", "Authorization": f"Bearer {token}"}
        logger.info("Token generated successfully")
    except Exception as e:
        logger.error(
            {
                "source_id": source_id,
                "source_name": source_name,
                "message": "Failed to generate token",
                "error": str(e),
            }
        )
        raise

    list_downloaded_data = []
    chunk_size = 50
    chunks = [
        mls_numbers[i : i + chunk_size] for i in range(0, len(mls_numbers), chunk_size)
    ]

    for chunk_index, chunk in enumerate(chunks):
        # Wrap each ListingId in quotes for the API call
        listing_ids = ",".join([f"'{mls}'" for mls in chunk])
        api_url = f"{login_url}?$filter=ListingId in ({listing_ids})&$select=ListingId,Media&$expand=Media($select=ResourceRecordKeyNumeric,MediaURL,Order)"

        # logger.info(f"Processing chunk {chunk_index + 1}/{len(chunks)} with {len(chunk)} listings")

        try:
            response_api = requests.get(api_url, headers=headers, timeout=30)
            response_api.raise_for_status()
            data = response_api.json()
            chunk_data = data.get("value", [])
            list_downloaded_data.extend(chunk_data)
            # logger.info(f"Chunk {chunk_index + 1}: Downloaded {len(chunk_data)} records")

        except requests.exceptions.RequestException as e:
            logger.error(
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "message": "Error fetching data, retrying after token refresh",
                    "error": str(e),
                    "chunk": chunk_index + 1,
                }
            )
            time.sleep(10)

            try:
                token = create_token(auth_data)
                headers["Authorization"] = f"Bearer {token}"
                response_api = requests.get(api_url, headers=headers, timeout=30)
                response_api.raise_for_status()
                data = response_api.json()
                chunk_data = data.get("value", [])
                list_downloaded_data.extend(chunk_data)
                logger.info(
                    f"Chunk {chunk_index + 1}: Retry successful, downloaded {len(chunk_data)} records"
                )
            except Exception as retry_error:
                logger.error(
                    {
                        "source_id": source_id,
                        "source_name": source_name,
                        "message": "Retry with new token also failed",
                        "error": str(retry_error),
                        "chunk": chunk_index + 1,
                    }
                )
                raise

    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "download_count": len(list_downloaded_data),
        "message": "Data download completed",
    }
    logger.info(log_data)

    if len(list_downloaded_data) == 0:
        log_data = {
            "source_id": source_id,
            "source_name": source_name,
            "Message": "No Listing Found From Source Side.",
        }
        logger.info(log_data)
        return True

    media_list = []
    for listing in list_downloaded_data:
        # Get the ListingId from the parent object
        listing_id = listing.get("ListingId")
        for media in listing.get("Media", []):
            media["ListingId"] = listing_id
            media_list.append(media)

    logger.info(
        {
            "source_id": source_id,
            "source_name": source_name,
            "media_count": len(media_list),
            "message": "Media list created",
        }
    )

    if len(media_list) == 0:
        log_data = {
            "source_id": source_id,
            "source_name": source_name,
            "Message": "No media found for listings.",
        }
        logger.info(log_data)
        return True

    df_media = pd.DataFrame(media_list)
    df_media = df_media.rename(
        columns={
            "ResourceRecordKeyNumeric": "source_listing_id",
            "Order": "photo_order",
            "MediaURL": "media_url",
            "ListingId": "mls_number",
        }
    )

    df_media["photo_order"] = (
        pd.to_numeric(df_media["photo_order"], errors="coerce").fillna(0).astype(int)
    )

    df_media = df_media.sort_values(["source_listing_id", "photo_order"])

    df_media["source_id"] = source_id
    df_media["batch_id"] = batch_id
    df_media["y_creation_date"] = datetime.now()

    insert_query = """
        INSERT INTO stage.direct_idx_photo (
            source_listing_id,
            photo_order,
            media_url,
            source_id,
            batch_id,
            y_creation_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    df_media = df_media.dropna(subset=["source_listing_id", "media_url"])

    data = df_media[
        [
            "source_listing_id",
            "photo_order",
            "media_url",
            "source_id",
            "batch_id",
            "y_creation_date",
        ]
    ].values.tolist()

    cursor.executemany(insert_query, data)
    connection.commit()

    logger.info(
        {
            "source_id": source_id,
            "source_name": source_name,
            "stage_insert_count": len(data),
            "message": "Data inserted into stage.direct_idx_photo",
        }
    )

    ingest_media_query = f"""
        INSERT INTO listing_photo(
            listing_id,
            batch_id,
            media_modification_timestamp,
            media_url,
            source_creation_date,
            source_last_update_date,
            y_creation_date,
            y_last_update_date
        )
        SELECT 
            t.id as listing_id,
            s.batch_id,
            t.media_modification_timestamp as media_modification_timestamp,
            s.media_url,
            s.y_creation_date AS source_creation_date,
            s.y_creation_date as source_last_update_date,
            now() AS y_creation_date,
            now() AS y_last_update_date
        FROM stage.direct_idx_photo s 
        JOIN listing_p_active t
            ON s.source_listing_id = t.source_listing_id::text
            AND s.source_id = {source_id} 
            AND t.source_id = {source_id}
        LEFT JOIN listing_photo lp
            ON t.id = lp.listing_id
        WHERE s.source_id = {source_id} 
            AND t.source_id = {source_id} 
            AND lp.listing_id IS NULL 
        ORDER BY t.id, s.photo_order::int ASC
    """

    cursor.execute(ingest_media_query)
    connection.commit()

    count_query = f"""
        SELECT COUNT(*) 
        FROM listing_photo lp
        JOIN listing_p_active t ON lp.listing_id = t.id
        WHERE t.source_id = {source_id}
        AND lp.batch_id = {batch_id}
    """
    cursor.execute(count_query)
    inserted_count = cursor.fetchone()[0]

    log_data = {
        "source_id": source_id,
        "source_name": source_name,
        "stage_insert_count": len(data),
        "listing_photo_insert_count": inserted_count,
        "Status": True,
        "message": "Missing photos backfilled successfully",
    }
    logger.info(log_data)
    return True


def lambda_handler(event, context):
    """
    AWS Lambda handler function
    """
    listing_conn = None
    listing_cursor = None
    event["success"] = False

    try:
        logger.info({"event": event})

        # Get database connection details from secrets
        listing_secret_name = os.environ.get("listingDatabase")
        if not listing_secret_name:
            raise ValueError("Environment variable 'listingDatabase' is not set")

        sql_exec_limit = context.get_remaining_time_in_millis() if context else 300000
        listing_secrets = fetch_secrets(listing_secret_name)

        # Setup database connection
        listing_conn = setup_db_connection(listing_secrets, sql_exec_limit)
        listing_cursor = listing_conn.cursor()

        download_and_update_missing_photos(
            cursor=listing_cursor, connection=listing_conn, event=event
        )

        event["success"] = True
        logger.info(
            {
                "source_id": event.get("source_id"),
                "message": "Lambda executed successfully",
            }
        )
        return event

    except Exception as e:
        logger.error(
            {
                "source_id": event.get("source_id"),
                "Error": str(e),
                "Traceback": traceback.format_exc(),
            }
        )
        event["success"] = False
        event["error"] = str(e)
        return event

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
