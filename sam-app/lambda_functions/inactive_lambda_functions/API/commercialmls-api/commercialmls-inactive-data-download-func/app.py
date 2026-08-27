"""Commercialmls API InActive Data Download"""

# Importing required libraries
import requests
import pandas as pd
import boto3
import psycopg2
from psycopg2 import extras
import logging
import traceback
import os
import json
import time
from datetime import datetime

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("Commercialmls-InActive-Lambda")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


# make connection with PostgreSQL
def setup_db_connection(db_secret):
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
        )
        response_dict_success = {"status": "Success"}
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


# main func
def lambda_handler(event, context):

    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    login_url = event["auth"]["loginUrl"]
    password = event["auth"]["password"]

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "mls_board": "",
        "source_type": event["source_info"]["source_type"],
        "run_host": event["run_host"],
        "source_info": event["source_info"],
        "inactive_threshold": event["inactive_threshold"],
        "download_status": False,
        "success": False,
    }

    try:

        # db_connections
        listingDatabase = os.environ.get("listingDatabase")
        homelisting_secrets = fetch_secrets(listingDatabase)
        homelisting_connection = setup_db_connection(homelisting_secrets)
        homelisting_cursor = homelisting_connection.cursor()

        deletion_query = (
            f"DELETE FROM stage.direct_idx_id WHERE source_id = {source_id}"
        )
        homelisting_cursor.execute(deletion_query)
        homelisting_connection.commit()

        active_counts_query = f"SELECT COUNT(*) FROM public.listing_p_active WHERE source_id = {source_id};"
        homelisting_cursor.execute(active_counts_query)
        listings_active_count = homelisting_cursor.fetchone()[0]

        update_batch_status_query = f"""
            UPDATE stage.etl_batches 
            SET load_missing_lst_status = 'in-progress',
                source_t_counts = {listings_active_count}
            WHERE source_id = {source_id} AND batch_id = {batch_id}; 
        """
        homelisting_cursor.execute(update_batch_status_query)
        homelisting_connection.commit()

        """ data downloading """
        url_endpoints = ["SaleLeaseListingsByDate", "BusinessOpportunitiesByDate"]
        download_count = 0
        current_datetime = datetime.now()
        current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        for url_endpoint in url_endpoints:

            if url_endpoint == "BusinessOpportunitiesByDate":
                logger.info(
                    f"Commercialmls API Inactive Data Download | {source_name} ({source_id}) | Waiting for 10 minutes before next request"
                )
                time.sleep(10 * 60)  # wait for 10 minutes before next request.

            selection_fields = "ListingID,AvailabilityStatus"
            response_format = "JSON"
            # 1 request in 10 minutes; date filter do not work
            request_url = f"{login_url}/{url_endpoint}?fields={selection_fields}&token={password}&format={response_format}"

            response = requests.get(url=request_url)

            if response.status_code != 200:
                raise Exception(f"{response.status_code}: {response.text}")

            data = response.json()  # list of dicts
            data_df = pd.json_normalize(data)

            rename_map = {
                "ListingID": "source_listing_id",
                "AvailabilityStatus": "status",
            }
            data_df = data_df.rename(columns=rename_map)

            data_df.insert(0, "created_at", current_datetime)
            data_df.insert(0, "batch_id", int(batch_id))
            data_df.insert(0, "source_id", int(source_id))

            cols = ",".join(list(data_df.columns))
            data_values = [tuple(row) for row in data_df.values]
            del data_df  # releasing memory

            download_count = download_count + len(data_values)

            insert_query = "INSERT INTO stage.direct_idx_id({}) VALUES %s".format(cols)
            extras.execute_values(homelisting_cursor, insert_query, data_values)
            homelisting_connection.commit()

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "endpoint": url_endpoint,
                "insertion_count": len(data_values),
            }
            logger.info(log_msg)

        # update download count in stage.etl_batches
        update_download_counts_query = f"""
            UPDATE stage.etl_batches 
            SET downloaded_d_counts = {download_count}
            WHERE source_id = {source_id} AND batch_id = {batch_id}; 
        """
        homelisting_cursor.execute(update_download_counts_query)
        homelisting_connection.commit()
        final_response["download_status"] = True
        return final_response

    except Exception as e:

        log_msg = {
            "download_status": False,
            "Error": str(e),
            "Error At Line": traceback.format_exc(),
        }
        final_response.update(log_msg)
        logger.error(final_response)
        return final_response

    finally:
        if homelisting_cursor:
            homelisting_cursor.close()
        if homelisting_connection:
            homelisting_connection.close()
