"""Unified RETS InActive Data Download"""

# Importing required libraries
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
import pandas as pd
import psycopg2
from psycopg2 import extras
import logging
import traceback
import os
import boto3
import json
from datetime import datetime

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("Unified-Rets-InActive-Lambda")
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


def login(data):
    login_url = data["loginUrl"]
    username = data["user"]
    password = data["password"]

    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    headers = data.get("headers", {})
    session.auth = auth
    response = None

    try:
        # Send login request
        response = session.get(login_url, headers=headers)

        if response.status_code == 200:

            login_data = {
                "session": session,
                "Login": login_url,
                "Search": login_url.replace("login", "search"),
                "GetObject": login_url.replace("login", "getObject"),
            }

            return login_data
        else:
            raise Exception(
                {
                    "response_status_code": response.status_code,
                    "response_text": response.text,
                }
            )

    except Exception as e:
        log_msg = {
            "response_status_code": response.status_code,
            "response_text": response.text,
            "Error": e,
            "Error At Line": traceback.format_exc(),
        }
        raise Exception(log_msg)


def get_class_count(request_data, query_type):
    session = request_data["session"]
    query_params = request_data["query_params"]
    search_url = request_data["Search"]
    query_params["QueryType"] = query_type
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "2"

    count_value = None
    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        count_element = root.find(".//COUNT")
        count_value = int(count_element.get("Records"))
        return count_value

    except Exception as e:
        log_msg = {
            "response_text": response_text,
            "query_params": query_params,
            "response_status_code": response.status_code,
            "Error": e,
        }
        raise Exception(log_msg)


def data_download(request_data, query_type):
    response = None
    session = request_data["session"]
    query_params = request_data["query_params"]
    search_url = request_data["Search"]

    query_params["QueryType"] = query_type
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    if "no records found" in response_text.lower():
        return pd.DataFrame()

    try:
        root = ET.fromstring(response_text)
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore

        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]  # type: ignore
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)

        return df_temp

    except:
        raise Exception(
            {
                "response_status_code": response.status_code,
                "response_text": response_text,
            }
        )


# main func
def lambda_handler(event, context):

    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    auth = event["auth"]
    source_info = event["source_info"]

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "mls_board": "",
        "source_type": event["source_info"]["source_type"],
        "run_host": event["run_host"],
        "inactive_threshold": event["inactive_threshold"],
        "download_status": False,
        "success": False,
    }

    try:

        """db_connections"""
        rds_cursor, homelisting_cursor = (None, None)
        listing_database = os.environ.get("listingDatabase")
        rds_database = os.environ.get("rdsDatabase")
        db_secret_rds = fetch_secrets(rds_database)
        db_secret_listing = fetch_secrets(listing_database)
        rds_connection = setup_db_connection(db_secret_rds)
        homelisting_connection = setup_db_connection(db_secret_listing)
        rds_cursor = rds_connection.cursor()  # type: ignore
        homelisting_cursor = homelisting_connection.cursor()  # type: ignore

        """ updating DB statuses """
        deletion_query = (
            f"DELETE FROM stage.direct_idx_id WHERE source_id = {source_id}"
        )
        homelisting_cursor.execute(deletion_query)
        homelisting_connection.commit()

        active_listings_count = f"SELECT count(source_listing_id) FROM public.listing_p_active WHERE source_id = {source_id};"
        homelisting_cursor.execute(active_listings_count)
        active_listings_count = homelisting_cursor.fetchone()[0]

        update_batch_status_query = f"""
            UPDATE stage.etl_batches 
            SET load_missing_lst_status = 'in-progress',
                source_t_counts = {active_listings_count}
            WHERE source_id = {source_id} AND batch_id = {batch_id}; 
        """
        homelisting_cursor.execute(update_batch_status_query)
        homelisting_connection.commit()

        """ data downloading """
        property_active_classes_query = """ 
            SELECT resource_name, class_name 
            FROM dev.class_metadata 
            WHERE source_id = {0} 
                AND active_flag = 't' AND download_flag = 't'
                AND lower(resource_name) = 'property'
        """.format(source_id)
        rds_cursor.execute(property_active_classes_query)
        download_classes = rds_cursor.fetchall()

        download_count = 0
        current_datetime = datetime.now()
        current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        listing_modification_timestamp_column = source_info[
            "listing_modification_timestamp_column"
        ]
        status_column = source_info["status_column"]
        sold_status = source_info["sold_status"]
        listing_key_column = source_info["listing_key_column"]
        query_type = source_info["query_type"]

        request_data = login(auth)

        query = f"({listing_modification_timestamp_column}=1990-01-01T00:00:00+)"
        if source_id == 533:
            query = query + f",~({status_column}={sold_status})"

        for resource_name, class_name in download_classes:

            """requesting source"""
            offset = 0
            request_data["query_params"] = {
                "SearchType": resource_name,
                "Class": class_name,
                "Query": query,
                "Select": f"{listing_key_column},{status_column}",
                "Offset": offset,
            }

            class_total_count = get_class_count(request_data, query_type)

            while offset < class_total_count:

                data_df = data_download(request_data, query_type)

                class_download_count = len(data_df)
                offset += class_download_count
                request_data["query_params"]["Offset"] = offset

                if class_download_count == 0:
                    continue
                else:
                    """insertion into DB"""
                    rename_map = {
                        listing_key_column: "source_listing_id",
                        status_column: "status",
                    }
                    data_df = data_df.rename(columns=rename_map)

                    data_df.insert(0, "created_at", current_datetime)
                    data_df.insert(0, "batch_id", int(batch_id))
                    data_df.insert(0, "source_id", int(source_id))

                    cols = ",".join(list(data_df.columns))
                    data_values = [tuple(row) for row in data_df.values]

                    insert_query = (
                        "INSERT INTO stage.direct_idx_id({}) VALUES %s".format(cols)
                    )
                    extras.execute_values(homelisting_cursor, insert_query, data_values)
                    homelisting_connection.commit()

                    download_count = (
                        download_count + class_download_count
                    )  # updating download count with every class

                    log_msg = {
                        "source_id": source_id,
                        "source_name": source_name,
                        "class_name": class_name,
                        "insertion_count": class_download_count,
                    }
                    logger.info(log_msg)

        """ finalizing data downloading """
        update_download_counts_query = f"""
            UPDATE stage.etl_batches 
            SET downloaded_d_counts = {download_count}
            WHERE source_id = {source_id} AND batch_id = {batch_id}; 
        """
        homelisting_cursor.execute(update_download_counts_query)
        homelisting_connection.commit()
        final_response["success"] = True
        final_response["download_status"] = True

        final_response["auth"] = event["auth"]
        final_response["source_info"] = event["source_info"]
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
            homelisting_connection.close()
        if rds_cursor:
            rds_cursor.close()
            rds_connection.close()
