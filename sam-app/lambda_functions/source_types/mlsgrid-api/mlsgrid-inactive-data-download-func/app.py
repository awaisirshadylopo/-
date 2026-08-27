import json
import boto3
import pandas as pd
import requests
import psycopg2
import random
import time
from datetime import datetime
from psycopg2 import extras
import os
import traceback
from itertools import chain
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
def setup_db_connection(secret):
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    return conn


def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# FUNCTION TO CREATE OAUTH TOKEN USING CLIENT CREDENTIALS
def create_token(source_id, source_name, client_id, client_secret):
    # OAUTH TOKEN ENDPOINT URL
    url = "https://reso.dovetaildata.com/odata/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
        "client_secret": str(client_secret),
    }
    auth = (str(client_id), str(client_secret))
    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        # LOG TOKEN GENERATION FAILURE
        logs = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "Token Generation Failed",
            "Status Code": response.status_code,
        }
        logger.error(logs)


# Function to make API call and load tables
def api_call_and_load_tables(
    source_type,
    source_id,
    source_name,
    batch_id,
    run_host,
    cursor,
    connection,
    api_limit,
    inactive_threshold,
    loginurl,
    password,
    originating_system_name,
    active_status,
):

    # -------------------------------------------------------------
    # 1. Set the initial last modification date and prepare SQL query to update ETL status
    # -------------------------------------------------------------
    last_modification_date = "1990-01-01T00:00:00Z"
    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    status_flag = True
    # -------------------------------------------------------------
    # 2. Set the login URL and parameters for API requests
    # -------------------------------------------------------------

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = 500  # Number of records to fetch in each request
    skip = 0  # Number of records to skip initially
    data_list = []
    total_count = 0
    status_column = None

    # -------------------------------------------------------------
    # 3. Set the status column based on the source_id
    # -------------------------------------------------------------
    if source_id in (829, 706):
        status_column = "MlsStatus"
    else:
        status_column = "StandardStatus"
    # -------------------------------------------------------------
    # 4. Begin looping to fetch data in chunks from the source
    # -------------------------------------------------------------
    while True:
        if source_id == 871:

            # -------------------------------------------------------------
            # 5. Special case for source_id == 871, adjust top and filter parameters
            # -------------------------------------------------------------
            top = 1000
            value = "OriginatingSystemName eq '{}' and MlgCanView eq true".format(
                originating_system_name
            )
            params = {
                "$filter": value,
                "$count": "true",
                "$top": top,
                "$skip": skip,
                "$select": f"ListingKey,{status_column},ModificationTimestamp",
            }

            headers = {"Authorization": f"Bearer {password}"}
        elif source_id == 1014:
            # -------------------------------------------------------------
            # 5.1. Special case for source_id == 1014, adjust filter parameters like remove MlgCanView variable from API because it fails the API hit, similarly for StandardStatus to IN operator.
            # -------------------------------------------------------------
            # value = "OriginatingSystemName eq '{}' and ModificationTimestamp ge {} and {}".format(originating_system_name,last_modification_date,active_status)
            value = "ModificationTimestamp ge {} and {}".format(
                last_modification_date, active_status
            )
            params = {
                "$filter": value,
                "$count": "true",
                "$top": top,
                "$skip": skip,
                "$select": f"ListingKey,{status_column},ModificationTimestamp",
                "$orderby": "ModificationTimestamp asc",
            }

            headers = {"Authorization": f"Bearer {password}"}

        else:
            # -------------------------------------------------------------
            # 6. Default case for other source_ids, filtering by modification date and status
            # -------------------------------------------------------------
            value = "OriginatingSystemName eq '{}' and ModificationTimestamp ge {} and MlgCanView eq true and StandardStatus eq Enums.StandardStatus'Active' or StandardStatus eq Enums.StandardStatus'Coming Soon' or StandardStatus eq Enums.StandardStatus'Active Under Contract' or StandardStatus eq Enums.StandardStatus'Pending'".format(
                originating_system_name, last_modification_date
            )
            # value = "OriginatingSystemName eq '{}' and ModificationTimestamp ge {} and MlgCanView eq true and (StandardStatus eq Enums.StandardStatus'Active' or StandardStatus eq Enums.StandardStatus'Coming Soon' or StandardStatus eq Enums.StandardStatus'Active Under Contract' or StandardStatus eq Enums.StandardStatus'Pending') and (PropertyType eq 'Residential' or PropertyType eq 'Business Opportunity' or PropertyType eq 'Land' or PropertyType eq 'Residential Income' or PropertyType eq 'Commercial Sale')".format(originating_system_name, last_modification_date)
            params = {
                "$filter": value,
                "$count": "true",
                "$top": top,
                "$skip": skip,
                "$select": f"ListingKey,{status_column},ModificationTimestamp",
                "$orderby": "ModificationTimestamp asc",
            }

            headers = {"Authorization": f"Bearer {password}"}

        try:
            # -------------------------------------------------------------
            # 7. Introduce random delay to avoid throttling or rate-limiting issues
            # -------------------------------------------------------------
            random_number = random.randint(1, 3)
            random_number
            time.sleep(random_number)

            # -------------------------------------------------------------
            # 8. Send GET request to API and handle response
            # -------------------------------------------------------------
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()  # Check if the request was successful
            data = json.loads(response.text)
            data_list.append(data["value"])  # Append fetched data to data_list
            total_count = data["@odata.count"]  # Get total number of records

            # -------------------------------------------------------------
            # 9. Check if all records are fetched, otherwise continue fetching
            # -------------------------------------------------------------
            if skip + top >= total_count:
                break
            elif skip + top >= 1000:
                last_modification_date = data["value"][-1]["ModificationTimestamp"]
                skip = 0
                continue

            # # Increment the skip parameter to fetch the next batch
            skip += top

        except requests.exceptions.RequestException as e:
            # --Handle exceptions that might occur during the request
            # raise Exception( f"Error: {e},  Error At Line: {traceback.format_exc()}" )
            raise Exception(
                {
                    "source_id": source_id,
                    "batch_id": batch_id,
                    "source_type": source_type,
                    "Error": str(e),
                    "Error At Line": traceback.extract_tb(e.__traceback__)[-1].lineno,
                    "HIttingURL": base_url,
                    "Hitting_Param": params,
                    "Header": headers,
                }
            )

    # -------------------------------------------------------------
    # 13. Update ETL status with the downloaded count
    # -------------------------------------------------------------
    flat_list = list(chain(*data_list))
    download_count = len(flat_list)
    source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
        download_count, batch_id
    )
    status_update(source_count, cursor, connection)

    df = pd.DataFrame(flat_list)
    df["source_id"] = source_id
    df["batch_id"] = batch_id

    df = df.drop(columns=["@odata.id", "ModificationTimestamp"], errors="ignore")
    df = df.rename(
        columns={f"{status_column}": "status", "ListingKey": "source_listing_id"}
    )
    # -------------------------------------------------------------
    # 15. Prepare data and insert into the database
    # -------------------------------------------------------------
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))
    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(cols)
    # --Insert the processed data into the database
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    download_count = """ select count(distinct source_listing_id) from stage.direct_idx_id where source_id = {};""".format(
        source_id
    )
    cursor.execute(download_count)
    download_count = cursor.fetchone()
    download_count = download_count[0]

    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        download_count, batch_id
    )
    # status_update(d_count,cursor,connection)
    status_flag = True

    return {
        "source_id": source_id,
        "source_type": source_type,
        "batch_id": batch_id,
        "download_status": status_flag,
        "run_host": run_host,
        "inactive_threshold": inactive_threshold,
        "Total_Downloaded_Count": download_count,
    }


def lambda_handler(event, context):

    # -------------------------------------------------------------
    # 1. Extract input parameters from the event object
    # -------------------------------------------------------------
    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"]["mls_board"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    originating_system_name = event["source_info"]["originating_system_name"]
    api_limit = 200
    inactive_threshold = event["inactive_threshold"]
    loginurl = event["auth"]["loginUrl"]
    password = event["auth"]["password"]
    client_id = event["auth"]["user"]
    active_status = event["source_info"]["active_status"]

    token_generation = event.get("source_info", {}).get("token_generation", False)
    # GENERATES TOKEN
    if token_generation:
        token = create_token(source_id, source_name, client_id, password)
        password = token

    try:
        # -------------------------------------------------------------
        # 2. Fetch database connection details and set up the connection
        # -------------------------------------------------------------
        homelisting_secret = os.environ.get("listingDatabase")
        homelisting_secrets = fetch_secrets(homelisting_secret)
        homelisting_connection = setup_db_connection(homelisting_secrets)
        homelisting_cursor = homelisting_connection.cursor()

        if homelisting_connection:
            # -------------------------------------------------------------
            # 3. Delete existing records for the current source_id from the stage table
            # -------------------------------------------------------------
            delete_query = """
            DELETE FROM stage.direct_idx_id where source_id = {0}
            """.format(source_id)
            homelisting_cursor.execute(delete_query)
            homelisting_connection.commit()
            # -------------------------------------------------------------
            # 4. Call the function to fetch and process the data for the source
            # -------------------------------------------------------------

            final_response = api_call_and_load_tables(
                source_type,
                source_id,
                source_name,
                batch_id,
                run_host,
                homelisting_cursor,
                homelisting_connection,
                api_limit,
                inactive_threshold,
                loginurl,
                password,
                originating_system_name,
                active_status,
            )
            # -------------------------------------------------------------
            # 5. Set the success flag in the response and return it
            # -------------------------------------------------------------
            final_response["success"] = True
            final_response["mls_board"] = mls_board
            final_response["auth"] = event["auth"]
            final_response["source_info"] = event["source_info"]

            return final_response

    except Exception as e:
        # -------------------------------------------------------------
        # 6. Handle exceptions, log error, and prepare failure response
        # -------------------------------------------------------------
        final_response = {
            "source_id": source_id,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
            "Error": e,
            "mls_board": mls_board,
            "Error At line": traceback.format_exc(),
        }
        final_response["success"] = False
        log_msg = {
            "Error": e,
            "Error At line": traceback.format_exc(),
            "Payload": final_response,
        }
        logger.error(log_msg)

        return final_response

    finally:
        # -------------------------------------------------------------
        # 7. Close the database connection and cursor in the finally block
        # -------------------------------------------------------------
        if homelisting_cursor:
            homelisting_cursor.close()
        if homelisting_connection:
            homelisting_connection.close()
