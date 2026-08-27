# --- Silvar InActive Data Download Function ---#
import json
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
import pandas as pd
import re
import traceback
import os
import boto3
import psycopg2
from psycopg2 import extras
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# RETS LOGIN
# ============================================================
"""Establishes a RETS session using Digest Authentication.
Parses the RETS login response and returns session metadata
along with the authenticated requests session object. """


def login(auth):
    session = requests.Session()
    session.auth = HTTPDigestAuth(auth["user"], auth["password"])
    session.headers.update(auth["headers"])
    response = session.get(auth["loginUrl"])
    if response.status_code != 200:
        logger.error(f"Login failed! Status code: {response.status_code}")
        return None
    try:
        root = ET.fromstring(response.text)
        rets_text = root.find("RETS-RESPONSE").text.strip()
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_text))
        rets_data["session"] = session
        logger.info("Login successful!")
        return rets_data
    except Exception as e:
        logger.error(f"Login parsing failed: {e}")
        return None


# ============================================================
# Fetches database credentials from AWS Secrets Manager.
# ============================================================
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def db_conn(secret, sql_limit):
    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={sql_limit}",
    )


def clean_value(val):
    if pd.isna(val) or str(val).lower() in ["none", "nan", "na", ""]:
        return None
    return val


# ---------------- DATA DOWNLOAD ---------------- #
def data_download(response):
    session = response["session"]
    params = response["query_params"]
    search_url = response["Search"]
    params["QueryType"] = "DMQL2"
    params["Format"] = "COMPACT-DECODED"
    params["Count"] = "1"

    resp = session.get(search_url, params=params)
    try:
        root = ET.fromstring(resp.text)
        count = int(root.find(".//COUNT").get("Records"))
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]
        rows = [d.text.split("\t")[1:-1] for d in root.findall("./DATA")]
        # return pd.DataFrame(rows, columns=columns), count
        return rows, count
    except Exception:
        return pd.DataFrame(), 0


# ---------------- MAIN LOGIC ---------------- #
def download_data_for_inactive(
    cursor, rds_cursor, conn, event, response, limit, offset
):
    """
    Downloads Property status data for ACTIVE/INACTIVE listings and inserts
    results into staging table in bulk.

    Workflow:
    1. Fetch source_listing_ids from primary DB based on pagination (limit/offset)
    2. Fetch class metadata (download-enabled classes) from RDS
    3. Chunk listing IDs for efficient API calls
    4. Call external data_download API per class per chunk
    5. Normalize response rows
    6. Bulk insert into staging table
    """

    # -------------------------------------------------------------
    # step 1. Fetch input parameters
    # -------------------------------------------------------------
    source_id = event["source_id"]
    batch_id = event["batch_id"]
    chunk_size = 100

    # -------------------------------------------------------------
    # step 2. Fetch listings from database
    # -------------------------------------------------------------
    query = f""" select source_listing_id from public.listing where source_status IN ('ACTIVE' , 'INACTIVE') and source_id in ({source_id}) order by source_listing_id desc limit {limit} offset {offset} """

    cursor.execute(query)
    listings = [r[0] for r in cursor.fetchall()]
    if not listings:
        return 0

    chunks = [listings[i : i + chunk_size] for i in range(0, len(listings), chunk_size)]

    property_rows = []
    columns = None

    # -------------------------------------------------------------
    # step 3. Fetch download-enabled class metadata
    # -------------------------------------------------------------
    rds_cursor.execute(f"""
        select class_name
        from dev.class_metadata
        where source_id = {source_id} and download_flag='t' and resource_name='Property';
    """)
    classes = [r[0] for r in rds_cursor.fetchall()]

    # -------------------------------------------------------------
    # step 4. Call Silvar API and process results
    # -------------------------------------------------------------
    for chunk in chunks:
        ids = ", ".join(chunk)
        dmql = f"(PropertyID = {ids})"
        for cls in classes:
            response["query_params"] = {
                "SearchType": "Property",
                "Class": cls,
                "Query": dmql,
                "Select": "PropertyID,ListingStatus",
            }
            rows, count = data_download(response)
            if count > 0:
                # property_df = pd.concat([property_df, df], ignore_index=True)
                property_rows.extend(rows)
                if columns is None:
                    columns = ["PropertyID", "ListingStatus"]

    # -------------------------------------------------------------
    # step 5. Transform API response into DB insert format
    # -------------------------------------------------------------
    if not property_rows:
        return 0
    final_rows = []
    for row in property_rows:

        if not row or len(row) < 2:
            continue

        final_rows.append(
            (row[0], row[1], source_id, batch_id)  # PropertyID  # ListingStatus
        )

    db_columns = ["source_listing_id", "status", "source_id", "batch_id"]

    cols = ",".join(db_columns)
    # -------------------------------------------------------------
    # step 6. Bulk insert into staging table
    # -------------------------------------------------------------
    insert_query = f"""
         INSERT INTO stage.direct_idx_id (
            source_listing_id,
            status,
            source_id,
            batch_id
        )
        VALUES %s
    """

    extras.execute_values(cursor, insert_query, final_rows, page_size=5000)
    conn.commit()
    # -------------------------------------------------------------
    # step 6. Bulk insert into staging table
    # -------------------------------------------------------------
    logger.info({"Updated": len(property_rows), "Offset": offset})
    logger.info({"Total_Downloaded_Records": len(property_rows)})
    return len(property_rows)


# ---------------- LAMBDA HANDLER ---------------- #
def lambda_handler(event, context):
    """ "
    Description:
            This lambda function processes ACTIVE/INACTIVE listings for a given source by
            fetching listing IDs, calling the Bridge API in chunks, and inserting property
            status data into a staging table. It supports pagination, batch processing,
            and robust data transformation for downstream pipelines.

    Sample Event:
            {
            "source_id": 306,
            },
            "source_info": {
                "limit": 1000,
                "Commercial Multi-Residential (5+ units)": "CMF"
            }, #other parameters
            "run_host": "Serverless-Inactive-RETS-Silvar",
            "batch_creation_date": "2026-05-19 09:12:06.922791",
            "in-active-status": true
            }
    Created by: Ammmar Azkar
    Date: 22th May, 2026
    """
    # -------------------------------------------------------------
    # step 1. Fetch input parameters and environment variables
    # -------------------------------------------------------------

    source_id = event["source_id"]
    run_host = event["run_host"]
    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    sql_limit = context.get_remaining_time_in_millis()
    offset = event.get("offset", 0)
    source_info = event.get("source_info", {})
    source_type = event.get("source_info",{}).get("source_type")
    mls_board = event.get("source_info",{}).get("mls_board")
    logger.info(f"Lambda invoked with source_id={source_id}, offset={offset}")

    # -------------------------------------------------------------
    # step 2. Handle pagination exit condition (offset control)
    # -------------------------------------------------------------
    if offset != 0:
        auth = event.get("auth")
        source_count = event["source_count"]
        if offset >= source_count:
            logger.info("All records processed. Breaking loop.")
            return {
                "source_id": source_id,
                "offset": offset,
                "source_count": source_count,
                "updated_count": 0,
                "break_loop": True,
                "success": True,
                "batch_id":event["batch_id"],
                "auth": event["auth"],
                "run_host": run_host,
                "download_status": True,
                "source_type": source_type,
                "mls_board": mls_board,
                "inactive_threshold" : event['inactive_threshold'],
                "source_name": event["source_name"],
            }

    limit = 50000
    listing_conn = None
    rds_conn = None

    try:
        # -------------------------------------------------------------
        # step 3. Establish database connections
        # -------------------------------------------------------------
        listing_conn = db_conn(fetch_secrets(listing_secret), sql_limit)
        rds_conn = db_conn(fetch_secrets(rds_secret), sql_limit)

        listing_cursor = listing_conn.cursor()
        rds_cursor = rds_conn.cursor()

        # -------------------------------------------------------------
        # step 4. Initial run setup (cleanup + metadata fetch)
        # -------------------------------------------------------------
        if offset == 0:
            delete_query = """ 
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
            listing_cursor.execute(delete_query)
            listing_conn.commit()

            listing_cursor.execute(
                "SELECT auth FROM source WHERE id = %s", (source_id,)
            )

            auth_row = listing_cursor.fetchone()
            # logger.info(f"Credentials: {auth_row}")
            if not auth_row:
                raise Exception(f"No auth found for source_id={source_id}")

            auth = auth_row[0]

            if isinstance(auth, str):
                auth = json.loads(auth)

            listing_cursor.execute(
                """
                SELECT COUNT(1)
                FROM listing
                WHERE source_id = %s
                AND source_status IN ('ACTIVE' , 'INACTIVE')
            """,
                (source_id,),
            )

            source_count = listing_cursor.fetchone()[0]

            logger.info(
                f"Total listings to processed for source_id={source_id}: Total Records: {source_count}"
            )
            logger.info(f"Current Offset: {offset}")

        response = login(auth)
        if not response:
            raise Exception("Login failed")

        response["Search"] = auth["loginUrl"].replace("Login.ashx", "Search.ashx")
        response["source_id"] = source_id
        # -------------------------------------------------------------
        # step 5. Download inactive/active listing data
        # -------------------------------------------------------------
        updated = download_data_for_inactive(
            listing_cursor, rds_cursor, listing_conn, event, response, limit, offset
        )

        listing_conn.commit()
        rds_conn.commit()

        offset += limit

        logger.info(f"downloaded Records in this iteration: {updated}")

        return {
            "source_id": source_id,
            "offset": offset,
            "source_count": source_count,
            "source_type": source_type,
            "mls_board": mls_board,
            "source_info": source_info,
            "Downloaded_count": updated,
            "break_loop": offset >= source_count,
            "success": True if offset >= source_count else False,
            "auth": auth,
            "source_count": source_count,
            "run_host": run_host,
            "download_status": True,
            "batch_id":event["batch_id"],
            "inactive_threshold" : event['inactive_threshold'],
            "source_name": event["source_name"],
        }

    # -------------------------------------------------------------
    # step 6. Error handling and rollback
    # -------------------------------------------------------------

    except Exception as e:

        logger.error({"Error": str(e), "Trace": traceback.format_exc(), "event": event})

        if listing_conn:
            listing_conn.rollback()
        if rds_conn:
            rds_conn.rollback()

        return {"success": False, "error": str(e), "event": event}

    finally:
        if listing_conn:
            listing_conn.close()
        if rds_conn:
            rds_conn.close()
