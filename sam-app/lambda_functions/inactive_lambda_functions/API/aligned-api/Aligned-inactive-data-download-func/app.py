import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import os
import time
import traceback
import logging

logger = logging.getLogger("spark-inactive-data-download-func")
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])



def setup_db_connection(secret):
    return psycopg2.connect(
        dbname=secret['dbname'],
        user=secret['username'],
        password=secret['password'],
        host=secret['host'],
        port=secret['port']
    )


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()



def api_call_and_load_tables(
    source_type,
    source_id,
    source_name,
    batch_id,
    cursor,
    connection,
    loginurl,
    password,
    has_mlsid=False,
    mls_ids=None
):

    status_update(
        f"""
        UPDATE stage.etl_batches
        SET load_missing_lst_status = 'in-progress'
        WHERE batch_id = {batch_id}
        """,
        cursor,
        connection
    )

    base_url = loginurl.replace("$metadata", "Property")
    top = 100
    chunk_size = 100
    
    cursor.execute(
        f"SELECT source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id = {source_id}"
        )

    source_listing_ids = [i[0] for i in cursor.fetchall()]
    total_count = len(source_listing_ids)

    listing_chunks = [
        source_listing_ids[i:i + chunk_size]
        for i in range(0, total_count, chunk_size)
    ]

    status_update(
        f"""
        UPDATE stage.etl_batches
        SET source_t_counts = {total_count}
        WHERE batch_id = {batch_id}
        """,
        cursor,
        connection
    )

    download_list_data = []

    for listings in listing_chunks:
        listings = str(tuple(listings))

        value = f"ListingKey in {listings}"

        #Dynamic MLS ID filter
        if has_mlsid and mls_ids:
            value += f" and MlsId in {mls_ids}"

        extra_filter_value = " and PropertyType ne 'Rental'" if source_id != 1025 else ""

        value += (
            " and PropertyType ne 'Residential Lease'"
            " and PropertyType ne 'Commercial Lease'"
            f"{extra_filter_value}"
        )

        params = {
            "$filter": value,
            "$top": top,
            "$select": "ListingKey,StandardStatus"
        }

        headers = {"Authorization": f"Bearer {password}"}

        try:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            download_list_data.extend(data.get("value", []))
            time.sleep(1)

        except Exception as e:
            logger.error({
                "source_id": source_id,
                "source_name": source_name,
                "error": str(e),
                "response": getattr(response, "text", None)
            })
																		 
            return False, 0

    #--- Insert Data --
    if not download_list_data:
        return True, 0

    df = pd.DataFrame(download_list_data)
    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.rename(columns={
        "ListingKey": "source_listing_id",
        "StandardStatus": "status"
    })

    if "@odata.id" in df.columns:
        df = df.drop(columns=["@odata.id"])
        df = df.drop(columns=["ListingKeyNumeric"])

    records = [tuple(row) for row in df.itertuples(index=False, name=None)]
    columns = ",".join(df.columns)

    insert_query = f"""
        INSERT INTO stage.direct_idx_id ({columns})
        VALUES %s
    """

    extras.execute_values(cursor, insert_query, records)
    connection.commit()

    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT source_listing_id)
        FROM stage.direct_idx_id
        WHERE source_id = {source_id}
        """
    )
    download_count = cursor.fetchone()[0]

    status_update(
        f"""
        UPDATE stage.etl_batches
        SET downloaded_d_counts = {download_count}
        WHERE batch_id = {batch_id}
        """,
        cursor,
        connection
    )

    return True, download_count



def lambda_handler(event, context):

    logger.info({"message": "EVENT_RECEIVED", "event": event})

    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    run_host = event["run_host"]
    inactive_threshold = event["inactive_threshold"]

    source_info = event.get("source_info", {})
    source_type = source_info.get("source_type")
    mls_board = source_info.get("mls_board")
    has_mlsid = source_info.get("has_mlsid", False)
    mls_ids = source_info.get("mlsid")

    loginurl = event["auth"]["loginUrl"]
    password = event["auth"]["password"]

    try:
        listing_secret = os.environ["listingDatabase"]
        secrets = fetch_secrets(listing_secret)
        conn = setup_db_connection(secrets)
        cursor = conn.cursor()

        cursor.execute(
            f"DELETE FROM stage.direct_idx_id WHERE source_id = {source_id}"
        )
        conn.commit()

        status, download_count = api_call_and_load_tables(
            source_type,
            source_id,
            source_name,
            batch_id,
            cursor,
            conn,
            loginurl,
            password,
            has_mlsid,
            mls_ids
        )

        response = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": status,
            "download_count": download_count,
            "inactive_threshold": inactive_threshold,
            "run_host": run_host,
            "success": event.get("success"),
            "auth": event.get("auth"),
            "source_info": source_info
        }

        logger.info({"message": "PROCESS_COMPLETED", "event": response})
        return response

    except Exception as e:
        logger.error({
					
            "message": "LAMBDA_FAILED",
						   
            "error": str(e),
            "trace": traceback.format_exc()
        })
		  
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "download_status": False,
        }
        event.update(log_msg)
        logger.error(event)
        return event

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
