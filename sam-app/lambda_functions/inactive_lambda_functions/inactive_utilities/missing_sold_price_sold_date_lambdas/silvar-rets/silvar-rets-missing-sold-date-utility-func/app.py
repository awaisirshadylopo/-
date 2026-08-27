# ---- Description: This lambda function is designed to update missing sold dates and sold prices in the listing_p_sold table for Silvar RETS data source. It connects to the RETS server, retrieves the necessary data, and updates the database accordingly. The function handles pagination to ensure that all records are processed, and includes error handling to manage potential issues during execution. Created by: Muhammad Fasihuddin, Date: 2026-02-23 ---- #

import json
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
import pandas as pd
import re
import traceback
import psycopg2
import os
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------------- LOGIN ---------------- #
def login(auth):
    session = requests.Session()
    session.auth = HTTPDigestAuth(auth['user'], auth['password'])
    session.headers.update(auth['headers'])
    response = session.get(auth['loginUrl'])
    if response.status_code != 200:
        logger.error(f"Login failed! Status code: {response.status_code}")
        return None
    try:
        root = ET.fromstring(response.text)
        rets_text = root.find('RETS-RESPONSE').text.strip()
        rets_data = dict(re.findall(r'(\w+)=([^\n\r]*)', rets_text))
        rets_data['session'] = session
        logger.info("Login successful!")
        return rets_data
    except Exception as e:
        logger.error(f"Login parsing failed: {e}")
        return None

# ---------------- DB CONNECTION ---------------- #
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def db_conn(secret, sql_limit):
    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        options=f"-c statement_timeout={sql_limit}"
    )

def clean_value(val):
    if pd.isna(val) or str(val).lower() in ["none","nan","na",""]:
        return None
    return val

# ---------------- DATA DOWNLOAD ---------------- #
def data_download(response):
    session = response['session']
    params = response['query_params']
    search_url = response['Search']
    params['QueryType'] = 'DMQL2'
    params['Format'] = 'COMPACT-DECODED'
    params['Count'] = '1'
    
    resp = session.get(search_url, params=params)
    try:
        root = ET.fromstring(resp.text)
        count = int(root.find('.//COUNT').get('Records'))
        columns = root.find('./COLUMNS').text.split('\t')[1:-1]
        rows = [d.text.split('\t')[1:-1] for d in root.findall('./DATA')]
        return pd.DataFrame(rows, columns=columns), count
    except Exception:
        return pd.DataFrame(), 0

# ---------------- MAIN LOGIC ---------------- #
def download_update_sold(cursor, rds_cursor, conn, event, response, limit, offset):
    source_id = event["source_id"]
    chunk_size = 50
    
    query = f"""
        select source_listing_id
        from listing_p_sold
        where source_id = {source_id}
        and (sold_date is null or sold_price is null)  
        order by modification_timestamp desc
        limit {limit} offset {offset};
    """
    #limit {limit} offset {offset};
    #and mls_number~*'ML82017614'
    cursor.execute(query)
    listings = [r[0] for r in cursor.fetchall()]
    if not listings:
        return 0

    
    chunks = [listings[i:i+chunk_size] for i in range(0, len(listings), chunk_size)]

    property_df = pd.DataFrame()
    rds_cursor.execute(f"""
        select class_name
        from dev.class_metadata
        where source_id = {source_id} and download_flag='t' and resource_name='Property';
    """)
    classes = [r[0] for r in rds_cursor.fetchall()]

    for chunk in chunks:
        ids = ", ".join(chunk)
        dmql = f"(PropertyID = {ids})"
        for cls in classes:
            response['query_params'] = {
                "SearchType": "Property",
                "Class": cls,
                "Query": dmql,
                "Select": "PropertyID,COEDate,SalePrice"
            }
            df, count = data_download(response)
            if count > 0:
                property_df = pd.concat([property_df, df], ignore_index=True)

    if property_df.empty:
        return 0

    property_df["source_id"] = source_id
    property_df = property_df.applymap(clean_value)
    property_df = property_df.rename(columns={
        "PropertyID": "source_listing_id",
        "COEDate": "sold_date",
        "SalePrice": "sold_price"
    })

    update_query = """
        update listing_p_sold set sold_date=%s, sold_price=%s
        where source_listing_id=%s and source_id=%s
    """
    update_data = [tuple(row) for row in property_df[["sold_date","sold_price","source_listing_id","source_id"]].values]
    cursor.executemany(update_query, update_data)
    conn.commit()
    logger.info({"Updated": len(property_df), "Offset": offset})
    logger.info({"Sample_Property_DF": property_df.head(10).to_dict(orient="records"),"Total_Downloaded_Records": len(property_df)})
    return len(property_df)

# ---------------- LAMBDA HANDLER ---------------- #
def lambda_handler(event, context):

    source_id = event["source_id"]
    run_host = event['run_host']
    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    sql_limit = context.get_remaining_time_in_millis()
    offset = event.get("offset", 0)
    logger.info(f"Lambda invoked with source_id={source_id}, offset={offset}")
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
                "auth":event["auth"],
                "run_host": run_host
             }

    limit = 1000
    listing_conn = None
    rds_conn = None

    try:

        listing_conn = db_conn(fetch_secrets(listing_secret), sql_limit)
        rds_conn = db_conn(fetch_secrets(rds_secret), sql_limit)

        listing_cursor = listing_conn.cursor()
        rds_cursor = rds_conn.cursor()

        if offset == 0:
            listing_cursor.execute(
                "SELECT auth FROM source WHERE id = %s",
                (source_id,)
            )

            auth_row = listing_cursor.fetchone()
            # logger.info(f"Credentials: {auth_row}")
            if not auth_row:
                raise Exception(f"No auth found for source_id={source_id}")

            auth = auth_row[0]

            if isinstance(auth, str):
                auth = json.loads(auth)

            listing_cursor.execute("""
                SELECT COUNT(1)
                FROM listing_p_sold
                WHERE source_id = %s
                AND (sold_date IS NULL OR sold_price IS NULL)
            """, (source_id,))

            source_count = listing_cursor.fetchone()[0]

            logger.info(f"Total Missing Sold Records: {source_count}")
            logger.info(f"Current Offset: {offset}")

        response = login(auth)
        if not response:
            raise Exception("Login failed")

        response["Search"] = auth["loginUrl"].replace("Login.ashx", "Search.ashx")
        response["source_id"] = source_id

        updated = download_update_sold(
            listing_cursor,
            rds_cursor,
            listing_conn,
            event,
            response,
            limit,
            offset
        )

        listing_conn.commit()
        rds_conn.commit()

        offset += limit

        logger.info(f"Updated Records: {updated}")

        return {
            "source_id": source_id,
            "offset": offset,
            "source_count": source_count,
            "updated_count": updated,
            "break_loop": offset >= source_count,
            "success": True if offset >= source_count else False,
            "auth": auth,
            "source_count": source_count,
            "run_host": run_host,
        }

    except Exception as e:
        logger.error({
            "Error": str(e),
            "Trace": traceback.format_exc(),
            "event": event
        })

        if listing_conn:
            listing_conn.rollback()
        if rds_conn:
            rds_conn.rollback()

        return {
            "success": False,
            "error": str(e),
            "event": event
        }

    finally:
        if listing_conn:
            listing_conn.close()
        if rds_conn:
            rds_conn.close()
