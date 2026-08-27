"""MLS Router API Inactive Data Download"""

import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
import os
import traceback
import logging
from itertools import chain

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# runtime token generation
def token_generation(data, source_id, cursor, connection):
    expires_in = data["expires_in"]
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    # Print token info
    log_msg = {
        "source_id": source_id,
        "expires_in": expires_in,
        "current_time": current_time,
        "token_preview": (
            data.get("access_token", "No token")[:50] + "..."
            if data.get("access_token")
            else "No token"
        ),
    }
    logger.info(f"TOKEN_CHECK: {json.dumps(log_msg)}")

    if expires_in > current_time:
        log_msg = {
            "source_id": source_id,
            "expires_in": expires_in,
            "current_time": current_time,
            "Message": "Token is still valid",
        }
        logger.info(log_msg)
        logger.info(f"USING_EXISTING_TOKEN: {data['access_token'][:100]}...")
        return data["access_token"]

    else:
        logger.info(f"GENERATING_NEW_TOKEN for source_id: {source_id}")
        response = create_token(data)
        expires_in = response["expires_in"]  # type: ignore
        token = response["access_token"]  # type: ignore

        logger.info(f"NEW_TOKEN_GENERATED: {token[:100]}...")
        logger.info(f"TOKEN_EXPIRES_IN_SECONDS: {expires_in}")

        # Get current datetime
        now = datetime.now()
        # Add expires_in seconds
        future_date = now + timedelta(seconds=expires_in)
        future_date = future_date.strftime("%Y-%m-%d %H:%M:%S")

        query = """ UPDATE source 
           SET auth = auth || %s::jsonb
           WHERE id = %s """

        # Create the JSON object as a Python dictionary
        json_update = {"expires_in": future_date, "access_token": token}

        # Execute with parameters
        cursor.execute(query, (json.dumps(json_update), source_id))
        connection.commit()

        return token


def create_token(data):
    client_id = data["client_id"]
    loginUrl = "https://api.realtyfeed.com/v1/auth/token"
    client_secret = data["client_secret"]

    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    token_payload = {
        "grant_type": "client_credentials",
        "client_id": str(client_id),
        "client_secret": str(client_secret),
    }

    # Log token request metadata only (never log secrets or secret-derived payloads)
    safe_log_msg = {
        "url": loginUrl,
        "grant_type": "client_credentials",
        "message": "Requesting new token",
    }
    logger.info(f"TOKEN_REQUEST: {json.dumps(safe_log_msg)}")

    response = requests.post(url=loginUrl, headers=headers, data=token_payload, timeout=30)

    logger.info(f"TOKEN_RESPONSE_STATUS: {response.status_code}")

    if response.status_code == 200:
        response = json.loads(response.content)
        logger.info(
            f"TOKEN_RECEIVED_SUCCESSFULLY - Expires in: {response.get('expires_in')} seconds"
        )
        return response
    else:
        ret = {"statusCode": response.status_code, "body": "Token Generation Failed"}
        logger.error(f"TOKEN_GENERATION_FAILED: {json.dumps(ret)}")
        logger.info(ret)


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
def setup_db_connection(db_secret, sqlExecLimit):
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
            options=f"-c statement_timeout={sqlExecLimit}",
        )
        response_dict_success = {"status": "Success"}
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


# execute query in homelistings
def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


# Function to make API call and load tables
def api_call_and_load_tables(
    source_id,
    mls_board,
    batch_id,
    cursor,
    connection,
    loginurl,
    password,
    active_status_values,
    status_column,
):

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(
        batch_id
    )
    status_update(etl_status, cursor, connection)

    status_flag = True

    loginurl = loginurl.replace("$metadata", "Property")
    base_url = loginurl
    top = 200  # Number of records to fetch in each request
    skip = 0
    download_data_list = []

    # Build the filter condition for API
    # Format status values for OData filter (comma-separated in quotes)
    status_values_formatted = ",".join([f"'{value}'" for value in active_status_values])

    # Create the filter with status column and PropertyType exclusions
    filter_condition = f"{status_column} in ({status_values_formatted}) and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"

    log_msg = {
        "source_id": source_id,
        "mls_board": mls_board,
        "filter_condition": filter_condition,
        "message": "Fetching data directly from source using filter",
    }
    logger.info(log_msg)

    headers = {"Authorization": f"Bearer {password}"}

    # Log API request headers without sensitive token content
    logger.info("API_REQUEST_HEADERS: Authorization header set")

    # First, get total count to track progress
    count_params = {
        "$count": "true",
        "$top": 0,
        "$filter": filter_condition,
        "$select": "ListingKey",
    }

    # Log the count API request
    logger.info(f"COUNT_API_REQUEST: URL={base_url}, PARAMS={json.dumps(count_params)}")

    try:
        count_response = requests.get(
            url=base_url, params=count_params, headers=headers
        )

        # Log API response status
        logger.info(f"COUNT_API_RESPONSE_STATUS: {count_response.status_code}")

        count_response.raise_for_status()
        count_data = json.loads(count_response.text)
        total_count = count_data.get("@odata.count", 0)

        logger.info(f"COUNT_API_RESPONSE_DATA: Total count = {total_count}")

        source_count = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(
            total_count, batch_id
        )
        status_update(source_count, cursor, connection)

        log_msg = {
            "source_id": source_id,
            "mls_board": mls_board,
            "total_count": total_count,
            "message": f"Total {total_count} records to fetch from source",
        }
        logger.info(log_msg)

    except requests.exceptions.RequestException as e:
        status_flag = False
        log_msg = {
            "source_id": source_id,
            "mls_board": mls_board,
            "Message": "Error fetching total count from source",
            "Error": str(e),
            "Response_Status": (
                count_response.status_code
                if "count_response" in locals()
                else "No response"
            ),
            "Response_Text": (
                count_response.text[:500]
                if "count_response" in locals()
                else "No response"
            ),
        }
        logger.error(log_msg)
        return {
            "source_id": source_id,
            "batch_id": batch_id,
            "download_status": status_flag,
        }

    # Fetch data in chunks using $skip and $top with the filter
    params = {
        "$count": "false",
        "$top": top,
        "$skip": skip,
        "$filter": filter_condition,
        "$select": "ListingKey,StandardStatus",
    }

    request_number = 1

    while skip < total_count:
        params["$skip"] = skip

        # Log each API request
        logger.info(
            f"DATA_API_REQUEST #{request_number}: URL={base_url}, PARAMS={json.dumps(params)}"
        )

        try:
            response = requests.get(url=base_url, params=params, headers=headers)

            # Log response status
            logger.info(
                f"DATA_API_RESPONSE #{request_number}: Status={response.status_code}"
            )

            response.raise_for_status()
            data = json.loads(response.text)

            # Log response summary
            records_in_response = len(data.get("value", []))
            logger.info(
                f"DATA_API_RESPONSE #{request_number}: Records received={records_in_response}"
            )

            if records_in_response > 0:
                # Log first record as sample (without sensitive data)
                sample_record = data["value"][0] if data["value"] else {}
                logger.info(
                    f"DATA_API_RESPONSE #{request_number}: Sample record = {json.dumps(sample_record)}"
                )

            if "value" in data and len(data["value"]) > 0:
                download_data_list.extend(data["value"])
                log_msg = {
                    "source_id": source_id,
                    "mls_board": mls_board,
                    "request_number": request_number,
                    "skip": skip,
                    "fetched": len(download_data_list),
                    "total": total_count,
                    "percentage": (
                        f"{(len(download_data_list)/total_count)*100:.2f}%"
                        if total_count > 0
                        else "N/A"
                    ),
                    "message": "Progress update",
                }
                logger.info(log_msg)

            skip += top
            request_number += 1

        except requests.exceptions.RequestException as e:
            status_flag = False
            log_msg = {
                "source_id": source_id,
                "mls_board": mls_board,
                "request_number": request_number,
                "Message": "Error in fetching data from source",
                "Error": str(e),
                "Response_Status": (
                    response.status_code if "response" in locals() else "No response"
                ),
                "Response_Text": (
                    response.text[:500] if "response" in locals() else "No response"
                ),
            }
            logger.error(log_msg)

            return {
                "source_id": source_id,
                "batch_id": batch_id,
                "download_status": status_flag,
            }

    logger.info(f"TOTAL_API_REQUESTS_MADE: {request_number - 1}")
    logger.info(f"TOTAL_RECORDS_FETCHED: {len(download_data_list)}")

    # Create DataFrame from downloaded data
    df = pd.DataFrame(download_data_list)

    if df.empty:
        log_msg = {
            "source_id": source_id,
            "mls_board": mls_board,
            "message": "No data fetched from source",
        }
        logger.info(log_msg)

        d_count = """update stage.etl_batches set downloaded_d_counts = 0 where batch_id = {0};""".format(
            batch_id
        )
        status_update(d_count, cursor, connection)

        return {
            "source_id": source_id,
            "batch_id": batch_id,
            "download_status": status_flag,
            "count": 0,
        }

    df["source_id"] = source_id
    df["batch_id"] = batch_id

    # Log DataFrame info
    logger.info(f"DATAFRAME_CREATED: Shape={df.shape}, Columns={list(df.columns)}")

    # Drop unnecessary columns if they exist
    columns_to_drop = [
        "@odata.id",
        "ActiveOpenHouse",
        "OriginatingSystemName",
        "InternetAddressDisplayYN",
        "Badge",
    ]
    df = df.drop(
        columns=[col for col in columns_to_drop if col in df.columns], errors="ignore"
    )
    df = df.rename(
        columns={"StandardStatus": "status", "ListingKey": "source_listing_id"}
    )

    logger.info(
        f"DATAFRAME_AFTER_PROCESSING: Shape={df.shape}, Columns={list(df.columns)}"
    )

    downloaded_count = len(df)
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(
        downloaded_count, batch_id
    )
    status_update(d_count, cursor, connection)

    # Delete existing records for this source_id before inserting new ones
    delete_query = """ 
    DELETE FROM stage.direct_idx_id where source_id = {0}
    """.format(
        source_id
    )

    logger.info(f"DELETING_OLD_RECORDS: {delete_query}")

    cursor.execute(delete_query)
    rows_deleted = cursor.rowcount
    connection.commit()

    log_msg = {
        "source_id": source_id,
        "mls_board": mls_board,
        "message": "Deleting from stage.direct_idx_id",
        "query": delete_query,
        "rows_deleted": rows_deleted,
    }
    logger.info(log_msg)

    # Insert new records
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ",".join(list(df.columns))

    insert_query = """
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                """.format(
        cols
    )

    logger.info(
        f"INSERTING_RECORDS: {len(tuple_list)} records into stage.direct_idx_id"
    )

    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    logger.info(f"INSERT_COMPLETE: Successfully inserted {len(tuple_list)} records")

    return {
        "source_id": source_id,
        "batch_id": batch_id,
        "download_status": status_flag,
        "count": downloaded_count,
    }


def lambda_handler(event, context):
    log_msg = {"message": "MLS Router API Inactive Downloading", "info": event}
    logger.info(f"LAMBDA_START: {json.dumps(log_msg)}")

    run_host = event["run_host"]
    source_id = event["source_id"]
    source_type = event["source_info"]["source_type"]
    mls_board = event["source_info"].get("mls_board")
    batch_id = event["batch_id"]
    last_batch_status = event["last_batch_status"]
    loginurl = event["auth"]["loginUrl"]
    client_id = event["auth"]["client_id"]
    client_secret = event["auth"]["client_secret"]
    data = event["auth"]

    # Log auth info (without sensitive data)
    logger.info(f"AUTH_INFO: loginUrl={loginurl}, client_id={client_id[:10]}...")

    # Get active status values and status column from event or use defaults
    active_status_values = event.get(
        "active_status_values",
        ["Active", "Pending", "Coming Soon", "Active Under Contract"],
    )
    status_column = event.get("status_column", "StandardStatus")

    logger.info(
        f"CONFIGURATION: active_status_values={active_status_values}, status_column={status_column}"
    )

    try:
        listing_secret = os.environ.get("listingDatabase")
        logger.info("FETCHING_DATABASE_SECRETS")

        listing_secrets = fetch_secrets(listing_secret)
        sqlExecLimit = (
            context.get_remaining_time_in_millis()
            if hasattr(context, "get_remaining_time_in_millis")
            else 30000
        )

        logger.info(f"SQL_EXECUTION_LIMIT: {sqlExecLimit} ms")

        listing_conn = setup_db_connection(listing_secrets, sqlExecLimit)
        listing_cursor = listing_conn.cursor()

        # Generate token
        logger.info(f"GENERATING_TOKEN for source_id: {source_id}")
        token = token_generation(data, source_id, listing_cursor, listing_conn)

        logger.info(f"TOKEN_OBTAINED: {token[:100]}... (length={len(token)})")

        final_response = api_call_and_load_tables(
            source_id,
            mls_board,
            batch_id,
            listing_cursor,
            listing_conn,
            loginurl,
            token,
            active_status_values,
            status_column,
        )

        final_response["source_type"] = source_type
        final_response["mls_board"] = mls_board
        final_response["run_host"] = run_host
        final_response["source_name"] = event["source_name"]
        final_response["success"] = event["success"]
        final_response["inactive_threshold"] = event["inactive_threshold"]
        final_response['source_info'] = event['source_info']

        log_msg = {
            "message": "Inactive data downloaded successfully",
            "source_id": source_id,
            "mls_board": mls_board,
            "downloaded_count": final_response.get("count", 0),
        }
        logger.info(f"LAMBDA_SUCCESS: {json.dumps(log_msg)}")
        final_response["event"] = event
        final_response["success"] = True
        return final_response

    except Exception as e:

        final_response = {
            "source_id": source_id,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "download_status": False,
            "run_host": run_host,
            "source_info":event['source_info'],
        }
        final_response["success"] = event["success"]

        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "Payload": final_response,
        }
        logger.error(f"LAMBDA_ERROR: {json.dumps(log_msg)}")

        return final_response

    finally:
        if "listing_cursor" in locals() and listing_cursor:
            listing_cursor.close()
            logger.info("DATABASE_CURSOR_CLOSED")
        if "listing_conn" in locals() and listing_conn:
            listing_conn.close()
            logger.info("DATABASE_CONNECTION_CLOSED")
