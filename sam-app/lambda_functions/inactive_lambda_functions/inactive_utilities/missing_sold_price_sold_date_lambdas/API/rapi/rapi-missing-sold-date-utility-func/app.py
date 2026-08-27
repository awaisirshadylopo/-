import os
import logging
import json
import traceback
import boto3
import pandas as pd
import requests
import psycopg2
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def setup_db_connection(secret, sql_exec_limit):
    return psycopg2.connect(
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={sql_exec_limit}",
    )


def clean_value(val):
    if pd.isna(val):
        return None
    if isinstance(val, str):
        return val.strip()
    return val


# Generate token dynamically
# Supports 'user/password' or 'client_id/client_secret'


def create_token(auth_data):
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


# https://realcompapi.auth0.com/oauth/token


def download_and_update_sold_date(cursor, connection, event):
    source_id = event["source_id"]
    source_name = event.get("source_name")
    # limit = event["source_info"].get("limit", 1000)

    auth_data = event["auth"]
    loginUrl = event["auth"].get("metadataUrl").replace("$metadata", "Property")
    logger.info(f"loggin url :{loginUrl}")

    # Step 1: Get missing sold listings
    query = f"""
        SELECT source_listing_id
        FROM listing_p_sold
        WHERE source_id = {source_id}
          AND (sold_date IS NULL OR sold_price IS NULL)
        ORDER BY modification_timestamp DESC;
    """
    cursor.execute(query)
    listings = [str(r[0]) for r in cursor.fetchall()]

    if not listings:
        logger.info(
            {
                "source_id": source_id,
                "source_name": source_name,
                "message": "No Missing Sold Date and Sold Price Listings",
            }
        )
        event["Missing_Sold_Status"] = "No Missing Sold Date and Sold Price Listings"
        return event

    logger.info(
        {
            "source_id": source_id,
            "source_name": source_name,
            "listing_with_missing_Sold_date_count": len(listings),
            "message": "Found missing sold listings",
        }
    )

    try:
        token = create_token(auth_data)
        headers = {"User-Agent": "Ylopo", "Authorization": f"Bearer {token}"}

        downloaded_data = []
        chunk_size = 20
        for i in range(0, len(listings), chunk_size):
            chunk = listings[i : i + chunk_size]
            listing_keys = ",".join(chunk)
            api_url = f"{loginUrl}?$filter=ListingKeyNumeric in ({listing_keys}) and StandardStatus eq 'Closed'&$select=CloseDate,ClosePrice,StandardStatus,ListingKeyNumeric"

            try:
                response_api = requests.get(api_url, headers=headers, timeout=30)
                response_api.raise_for_status()
                data = response_api.json()
                downloaded_data.extend(data.get("value", []))

            except requests.exceptions.RequestException as e:

                logger.error(
                    {
                        "source_id": source_id,
                        "message": "Error fetching data, retrying after token refresh",
                        "error": str(e),
                    }
                )
                time.sleep(10)
                # Retry once with new token
                token = create_token(auth_data)
                headers["Authorization"] = f"Bearer {token}"
                response_api = requests.get(api_url, headers=headers, timeout=30)
                response_api.raise_for_status()
                data = response_api.json()
                downloaded_data.extend(data.get("value", []))

        if not downloaded_data:
            logger.info(
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "message": "No Records Downloaded from Source against provided listings.",
                }
            )
            return

        df = pd.DataFrame(downloaded_data)
        logger.info(
            {
                "source_id": source_id,
                "message": "Downloaded DataFrame info",
                "columns": list(df.columns),
                "Sold_Data_Downloaded": len(df),
                "data": df.head(2).to_dict(orient="records"),
            }
        )
        # logger.info({
        #     "source_id": source_id,
        #     "message": "Downloaded DataFrame sample",
        #     "data": df.head(2).to_dict(orient="records")
        # })
        df["source_id"] = source_id
        df = df.rename(
            columns={
                "ListingKeyNumeric": "source_listing_id",
                "CloseDate": "sold_date",
                "ClosePrice": "sold_price",
            }
        )
        df = df.apply(
            lambda col: (
                col.map(clean_value) if col.name in ["sold_date", "sold_price"] else col
            )
        )

        update_query = """
            UPDATE listing_p_sold
            SET sold_date = %s,
                sold_price = %s
            WHERE source_id = %s
            AND source_listing_id = %s;
        """
        data_to_update = [
            (
                row["sold_date"],
                row["sold_price"],
                row["source_id"],
                str(row["source_listing_id"]),
            )
            for _, row in df.iterrows()
        ]
        cursor.executemany(update_query, data_to_update)
        connection.commit()

        logger.info(
            {
                "source_id": source_id,
                "source_name": source_name,
                "update_count": len(df),
                "message": "Sold data backfilled successfully",
            }
        )
    except Exception as e:
        logger.error(
            {
                "source_id": source_id,
                "message": "Error updating sold data",
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
        )
        raise


def lambda_handler(event, context):
    listing_conn = None
    listing_cursor = None
    event["success"] = False

    try:
        logger.info({"event": event})

        listing_secret_name = os.environ.get("listingDatabase")
        sql_exec_limit = context.get_remaining_time_in_millis()
        listing_secrets = fetch_secrets(listing_secret_name)

        listing_conn = setup_db_connection(listing_secrets, sql_exec_limit)
        listing_cursor = listing_conn.cursor()

        download_and_update_sold_date(
            cursor=listing_cursor, connection=listing_conn, event=event
        )

        event["success"] = True
        logger.info(
            {"source_id": event["source_id"], "message": "Lambda executed successfully"}
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
