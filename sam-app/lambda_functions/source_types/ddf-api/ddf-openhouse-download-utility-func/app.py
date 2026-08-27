"""
DDF OpenHouse API Validation and Data Download
"""

import requests
import pandas as pd
import boto3
import psycopg2
import logging
import traceback
import os
import json
from datetime import datetime, timedelta, timezone
import time
import re
from botocore.exceptions import ClientError

# -------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------
# Secret Manager Helper
# -------------------------------------------------------------------
class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name="us-west-2"):
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])
        except ClientError as e:
            logger.error(f"Secret fetch failed: {str(e)}")
            raise


# -------------------------------------------------------------------
# DB Connection
# -------------------------------------------------------------------
def db_conn(secret, timeout_ms):
    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={int(timeout_ms)}",
    )


# -------------------------------------------------------------------
# Ensure Date Cast
# -------------------------------------------------------------------
def ensure_date_cast(expr: str) -> str:
    if not expr:
        return expr

    expr = expr.strip()
    match = re.search(r"\s+as\s+(\w+)\s*$", expr, flags=re.IGNORECASE)

    if match:
        alias = match.group(1)
        base_expr = expr[: match.start()].strip()
    else:
        alias = None
        base_expr = expr

    if not base_expr.lower().endswith("::date"):
        base_expr = f"({base_expr})::date"

    return f"{base_expr} AS {alias}" if alias else base_expr


def create_token(client_id, client_secret):
    token_url = "https://identity.crea.ca/connect/token"

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    payload = {"grant_type": "client_credentials", "scope": "DDFApi_Read"}

    response = requests.post(
        token_url, headers=headers, data=payload, auth=(client_id, client_secret)
    )

    response.raise_for_status()

    access_token = response.json()["access_token"]

    logger.info("Token generated successfully")

    return access_token


def fetch_openhouse_mappings(rds_cursor, source_id):

    EXPECTED_TARGETS = {
        "date": "openhousedate::timestamp::date AS openhousedate",
        "start_time": "openhousestarttime::timestamp::time AS openhousestarttime",
        "end_time": "openhouseendtime::timestamp::time AS openhouseendtime",
    }

    query = """
        SELECT replace(target_column,'"',''),
               business_transformation
        FROM etl.mappings
        WHERE resource_name ~* 'openhouse'
        AND source_id = %(source_id)s
    """

    rds_cursor.execute(query, {"source_id": source_id})
    rows = rds_cursor.fetchall()

    mappings = {}
    for row in rows:
        col, expr = row
        if expr:
            mappings[col] = expr

    for key in EXPECTED_TARGETS:
        if key not in mappings:
            logger.warning(f"Using default mapping for {key}")
            mappings[key] = EXPECTED_TARGETS[key]

    mappings["date"] = ensure_date_cast(mappings["date"])

    return [
        mappings["date"],
        mappings["start_time"],
        mappings["end_time"],
    ]


def process_openhouse_data(
    openhouse_data,
    source_id,
    batch_id,
    current_time,
    ls_cursor,
    rds_cursor,
    homelisting_conn,
):

    df = pd.DataFrame(openhouse_data)

    if df.empty:
        return 0

    df = df.rename(
        columns={
            "OpenHouseStartTime": "openhousestarttime",
            "OpenHouseEndTime": "openhouseendtime",
            "OpenHouseDate": "openhousedate",
        }
    )

    required_cols = [
        "ListingKey",
        "openhousedate",
        "openhousestarttime",
        "openhouseendtime",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["source_id"] = source_id
    df["batch_id"] = batch_id
    df["y_creation_date"] = current_time
    df["y_update_date"] = current_time

    df = df[
        [
            "source_id",
            "batch_id",
            "ListingKey",
            "openhousedate",
            "openhousestarttime",
            "openhouseendtime",
            "y_creation_date",
            "y_update_date",
        ]
    ]

    temp_table = "temp_openhouse_sync"

    ls_cursor.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            source_id INT,
            batch_id BIGINT,
            ListingKey TEXT,
            openhousedate DATE,
            openhousestarttime TEXT,
            openhouseendtime TEXT,
            y_creation_date TIMESTAMP,
            y_update_date TIMESTAMP
        ) ON COMMIT PRESERVE ROWS;
        """)

    homelisting_conn.commit()

    insert_query = f"""
        INSERT INTO {temp_table}
        VALUES (%(source_id)s,%(batch_id)s,%(ListingKey)s,
                %(openhousedate)s,%(openhousestarttime)s,
                %(openhouseendtime)s,%(y_creation_date)s,%(y_update_date)s)
    """

    ls_cursor.executemany(insert_query, df.to_dict("records"))
    homelisting_conn.commit()

    ls_cursor.execute(
        "DELETE FROM stage.direct_idx_openhouse_sync WHERE source_id=%s",
        (source_id,),
    )
    homelisting_conn.commit()

    # Dynamic mapping
    mapping_expr = fetch_openhouse_mappings(rds_cursor, source_id)

    select_sql = ", ".join(
        ["source_id", "batch_id", "ListingKey"]
        + mapping_expr
        + ["y_creation_date", "y_update_date"]
    )

    insert_final = f"""
        INSERT INTO stage.direct_idx_openhouse_sync
        (source_id,batch_id,ListingKey,
         openhousedate,openhousestarttime,openhouseendtime,
         y_creation_date,y_update_date)
        SELECT {select_sql}
        FROM {temp_table}
        WHERE source_id=%s AND batch_id=%s
    """

    ls_cursor.execute(insert_final, (source_id, batch_id))
    homelisting_conn.commit()

    return ls_cursor.rowcount


def download_all_openhouse(source_data, cursor_rds, ls_cursor, homelisting_conn):

    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    auth = source_data["auth"]

    source_type = source_data["source_info"].get("source_type")
    client_id = auth["user"]
    client_secret = auth["password"]
    access_token = create_token(client_id, client_secret)

    headers = {"Authorization": f"Bearer {access_token}"}

    loginurl = auth["loginUrl"].replace("$metadata", "")

    url = loginurl + "OpenHouse"

    last_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    params = {
        "$filter": f"OpenHouseDate ge {last_day}",
        "$select": "ListingKey,OpenHouseDate,OpenHouseStartTime,OpenHouseEndTime",
    }

    openhouse_data = []
    next_url = url

    while next_url:
        response = requests.get(
            next_url, params=params if next_url == url else None, headers=headers
        )
        response.raise_for_status()
        data = response.json()

        openhouse_data.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
        params = None

    logger.info(f"Downloaded {len(openhouse_data)} records")

    if openhouse_data:
        current_time = datetime.now(timezone.utc)

        rows = process_openhouse_data(
            openhouse_data,
            source_id,
            batch_id,
            current_time,
            ls_cursor,
            cursor_rds,
            homelisting_conn,
        )

        source_data["Openhouse_records_downloaded"] = len(openhouse_data)
        source_data["Openhouse_records_inserted"] = rows

    return source_data


# -------------------------------------------------------------------
# Lambda Handler
# -------------------------------------------------------------------
def lambda_handler(event, context):

    serverless_db = None
    listing_db = None
    cursor_rds = None
    cursor_listing = None

    try:
        timeout = context.get_remaining_time_in_millis()

        rds_secret_name = os.environ["rdsDatabase"]
        listing_secret_name = os.environ["listingDatabase"]

        rds_secret = SecretManagerHelper.get_secret(rds_secret_name)
        listing_secret = SecretManagerHelper.get_secret(listing_secret_name)

        serverless_db = db_conn(rds_secret, timeout)
        listing_db = db_conn(listing_secret, timeout)

        cursor_rds = serverless_db.cursor()
        cursor_listing = listing_db.cursor()

        response = download_all_openhouse(
            event,
            cursor_rds,
            cursor_listing,
            listing_db,
        )

        event["success"] = True
        return response

    except Exception as e:
        logger.error(traceback.format_exc())
        event["success"] = False
        event["error"] = str(e)
        return event

    finally:
        if cursor_rds:
            cursor_rds.close()
        if cursor_listing:
            cursor_listing.close()
        if serverless_db:
            serverless_db.close()
        if listing_db:
            listing_db.close()
