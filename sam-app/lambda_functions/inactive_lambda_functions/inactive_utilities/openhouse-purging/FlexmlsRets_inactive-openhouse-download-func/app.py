"""Lambda function for downloading and processing OpenHouse data from RETS server."""

import os
import traceback
import logging
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import boto3
import pandas as pd
import psycopg2
import requests
from requests.auth import HTTPDigestAuth
from xml.etree.ElementTree import ParseError

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    """Fetches secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def db_conn(db_secret, sql_execlimit):
    """
    Establishes a connection to the PostgreSQL database.
    """
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
            options=f"-c statement_timeout={sql_execlimit}",
        )
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        logger.error(log_msg)
        return None


def login(auth_data):
    """Login to RETS server"""
    loginUrl = auth_data["loginUrl"]
    password = auth_data["password"]
    username = auth_data["user"]

    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    session.headers = {
        "rets-version": "RETS/1.7.2",
        "User-Agent": "Python/3.8 RETS Client/1.0",
    }

    # Send login request
    response = session.get(loginUrl)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
        logger.info("Login successful!")
        rets_data["session"] = session
        return rets_data
    except ParseError as e:
        msg = {"message": f"Error in server login: {e}"}
        logger.error(msg)
        raise Exception(msg)


def data_download(rets_data, class_name, query_params):
    """Download data from RETS server"""
    session = rets_data["session"]
    search_url = rets_data.get("Search") or rets_data.get("search_url")
    source_id = query_params.get("source_id", "unknown")

    if not search_url:
        raise Exception("No Search URL found in RETS response")

    # Construct full search URL if needed
    if not search_url.startswith("http"):
        search_url = "http://retsgw.flexmls.com" + search_url

    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)

        # Check for errors
        reply_code = root.attrib.get("ReplyCode")
        if reply_code and reply_code != "0":
            reply_text = root.attrib.get("ReplyText", "Unknown error")
            if "No Records Found" in reply_text:
                return pd.DataFrame(), 0
            raise Exception(f"RETS Error {reply_code}: {reply_text}")

        # Extract column names
        count_element = root.find(".//COUNT")
        if count_element is None:
            return pd.DataFrame(), 0

        data_count = int(count_element.get("Records", 0))

        # Extract columns
        columns_element = root.find("./COLUMNS")
        if columns_element is None or columns_element.text is None:
            return pd.DataFrame(), data_count

        columns = columns_element.text.split("\t")[1:-1]

        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            if data_element.text:
                row = data_element.text.split("\t")[1:-1]
                data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except ParseError as e:
        logger.error(f"XML Parse Error for class {class_name}: {e}")
        return pd.DataFrame(), 0
    except Exception as e:
        logger.error(f"Error in data_download for class {class_name}: {e}")
        raise


def clean_value(value):
    """Clean null or empty values."""
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value


def fetch_openhouse_mappings(rds_cursor, mapping_table, source_id, batch_id):
    """Fetch dynamic mappings for OpenHouse columns from database."""

    # These match the field names from the RETS connector
    EXPECTED_TARGETS = {
        "unique_listing_id": "unique_listing_id",
        "event_start": "event_start",
        "event_end": "event_end",
    }

    fetch_mapping_query = f"""
        SELECT replace(target_column, '"', '') AS target_column,
        business_transformation
        FROM etl.mappings
        WHERE resource_name ~* 'openhouse'
          AND replace(target_column, '"', '') IN ('unique_listing_id', 'event_start', 'event_end')
          AND source_id = %(source_id)s
        ORDER BY
            CASE replace(target_column, '"', '') 
                WHEN 'unique_listing_id' THEN 1
                WHEN 'event_start' THEN 2
                WHEN 'event_end' THEN 3
            END;
    """

    rds_cursor.execute(fetch_mapping_query, {"source_id": source_id})
    rows = rds_cursor.fetchall()

    found_mappings = {
        row[0]: row[1] for row in rows if row[1] is not None and row[1] != "null"
    }
    missing_targets = set(EXPECTED_TARGETS) - set(found_mappings)

    # Use defaults for missing mappings
    for target in missing_targets:
        logger.warning(
            f"Using default column name for missing openhouse column '{target}' "
            f"for source_id={source_id}"
        )
        found_mappings[target] = EXPECTED_TARGETS[target]

    mapping_expressions = [
        found_mappings["unique_listing_id"],
        found_mappings["event_start"],
        found_mappings["event_end"],
    ]

    logger.info(
        {
            "source_id": source_id,
            "batch_id": batch_id,
            "etl_table": mapping_table,
            "mapping_expressions": mapping_expressions,
        }
    )

    return mapping_expressions


def process_openhouse_data(
    openhouse_data,
    source_id,
    batch_id,
    current_time,
    ls_cursor,
    rds_cursor,
    homelisting_conn,
    temp_table="temp_openhouse_sync",
    target_table="stage.direct_idx_openhouse_sync",
):
    """
    Process and insert openhouse data into target table.
    """
    try:
        # -------------------------------------------------------------
        # 1. Load raw data into DataFrame
        # -------------------------------------------------------------

        df_openhouse_upload = pd.DataFrame(openhouse_data)

        if len(df_openhouse_upload) == 0:
            logger.info("No OpenHouse data to process")
            return 0

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "TRANSFORM_START",
                "records_received": len(df_openhouse_upload),
                "columns": list(df_openhouse_upload.columns),
            }
        )

        if "unique_listing_id" in df_openhouse_upload.columns:
            df_openhouse_upload["ListingKey"] = df_openhouse_upload["unique_listing_id"]

        def parse_event_start(event_start):
            if pd.isna(event_start) or event_start is None:
                return None, None
            try:
                dt = pd.to_datetime(event_start)
                return dt.date(), dt.time()
            except Exception as e:
                logger.debug(f"Error parsing event_start '{event_start}': {e}")
                return None, None

        if "event_start" in df_openhouse_upload.columns:
            df_openhouse_upload[["openhousedate", "openhousestarttime"]] = (
                df_openhouse_upload["event_start"].apply(
                    lambda x: pd.Series(parse_event_start(x))
                )
            )

        def parse_event_end(event_end):
            if pd.isna(event_end) or event_end is None:
                return None
            try:
                dt = pd.to_datetime(event_end)
                return dt.time()
            except Exception as e:
                logger.debug(f"Error parsing event_end '{event_end}': {e}")
                return None

        if "event_end" in df_openhouse_upload.columns:
            df_openhouse_upload["openhouseendtime"] = df_openhouse_upload[
                "event_end"
            ].apply(parse_event_end)

        # Clean the data
        df_openhouse_upload = df_openhouse_upload.apply(
            lambda col: col.map(clean_value)
        )

        # -------------------------------------------------------------
        # 3. Ensure required columns exist and add metadata columns
        # -------------------------------------------------------------

        required_cols = [
            "ListingKey",
            "openhousedate",
            "openhousestarttime",
            "openhouseendtime",
        ]
        for col in required_cols:
            if col not in df_openhouse_upload.columns:
                df_openhouse_upload[col] = None

        # Add metadata columns
        df_openhouse_upload["source_id"] = source_id
        df_openhouse_upload["batch_id"] = batch_id
        df_openhouse_upload["y_creation_date"] = current_time
        df_openhouse_upload["y_update_date"] = current_time

        # -------------------------------------------------------------
        # 4. Keep only columns needed for temp table
        # -------------------------------------------------------------

        columns_to_keep = [
            "source_id",
            "batch_id",
            "ListingKey",
            "openhousedate",
            "openhousestarttime",
            "openhouseendtime",
            "y_creation_date",
            "y_update_date",
        ]

        df_openhouse_upload = df_openhouse_upload[columns_to_keep]

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "TRANSFORM_COMPLETE",
                "records_ready_for_insert": len(df_openhouse_upload),
            }
        )

        # -------------------------------------------------------------
        # 5. Create temporary table
        # -------------------------------------------------------------

        create_temp_table_sql = f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            source_id INT,
            batch_id BIGINT,
            ListingKey TEXT,
            openhousedate DATE,
            openhousestarttime TIME,
            openhouseendtime TIME, 
            y_creation_date TIMESTAMP, 
            y_update_date TIMESTAMP
        ) ON COMMIT PRESERVE ROWS;
        """
        ls_cursor.execute(create_temp_table_sql)
        homelisting_conn.commit()

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "TEMP_TABLE_CHECK",
                "message": f"Temp table `{temp_table}` created successfully.",
            }
        )

        # -------------------------------------------------------------
        # 6. Insert data into temporary table
        # -------------------------------------------------------------
        records = df_openhouse_upload.to_dict("records")

        insert_temp_query = f"""
        INSERT INTO {temp_table}
        (source_id, batch_id, ListingKey, openhousedate, openhousestarttime, 
         openhouseendtime, y_creation_date, y_update_date)
        VALUES (%(source_id)s, %(batch_id)s, %(ListingKey)s, %(openhousedate)s, 
                %(openhousestarttime)s, %(openhouseendtime)s, %(y_creation_date)s, 
                %(y_update_date)s)
        """

        try:
            ls_cursor.executemany(insert_temp_query, records)
            homelisting_conn.commit()
            logger.info(
                {
                    "step": "TEMP_INSERT_COMPLETE",
                    "records_inserted": len(records),
                    "table": temp_table,
                }
            )
        except Exception as e:
            homelisting_conn.rollback()
            logger.error(
                {
                    "source_id": source_id,
                    "batch_id": batch_id,
                    "step": "TEMP_INSERT_FAILED",
                    "error": str(e),
                    "table": temp_table,
                }
            )
            raise

        # -------------------------------------------------------------
        # 7. Delete previous records in target table
        # -------------------------------------------------------------
        delete_previous_query = f"""
            DELETE FROM {target_table}
            WHERE source_id = %(source_id)s
        """
        ls_cursor.execute(delete_previous_query, {"source_id": source_id})
        homelisting_conn.commit()

        logger.info(
            {
                "source_id": source_id,
                "step": "DELETE_COMPLETE",
                "message": f"Deleted previous records for source_id {source_id} from {target_table}",
            }
        )

        # -------------------------------------------------------------
        # 8. Insert transformed data into target table
        # -------------------------------------------------------------
        insert_dynamic_query = f"""
            INSERT INTO {target_table}
            (source_id, batch_id, ListingKey, openhousedate, 
             openhousestarttime, openhouseendtime, y_creation_date, y_update_date)
            SELECT 
                source_id, 
                batch_id, 
                ListingKey,
                openhousedate,
                openhousestarttime,
                openhouseendtime,
                y_creation_date, 
                y_update_date
            FROM {temp_table}
            WHERE source_id = {source_id}
                AND batch_id = {batch_id}
        """

        ls_cursor.execute(insert_dynamic_query)
        homelisting_conn.commit()
        rows_inserted = ls_cursor.rowcount

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "DB_INSERT_COMPLETE",
                "records_inserted": rows_inserted,
                "table": target_table,
            }
        )

        return rows_inserted

    except Exception as e:
        homelisting_conn.rollback()
        logger.error(
            {
                "step": "PROCESS_OPENHOUSE_FAILED",
                "error": str(e),
                "traceback": traceback.format_exc(),
                "source_id": source_id,
                "batch_id": batch_id,
            }
        )
        raise


def download_openhouse_data(
    ls_cursor, rds_cursor, ls_conn, event, rets_response, rds_conn
):
    """Downloads OpenHouse data for active listings and processes them."""
    source_id = event["source_id"]
    source_name = event["source_name"]
    batch_id = event["batch_id"]
    current_time = datetime.now()

    # Fetch all active listing IDs from listing_p_active
    query = f"""
        SELECT l.source_listing_id 
        FROM listing_p_active l
        JOIN listing_openhouse o ON l.id = o.listing_id
        WHERE l.source_id = {source_id}
        ORDER BY l.modification_timestamp DESC
    """

    ls_cursor.execute(query)
    active_listings = ls_cursor.fetchall()
    active_listings = [item[0] for item in active_listings]

    if len(active_listings) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No Active Listings Found",
        }
        logger.info(log_msg)
        return

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "listing_count": len(active_listings),
        "message": "Processing OpenHouse data for active listings",
    }
    logger.info(log_msg)

    # Get OpenHouse classes from class_metadata
    query = f"""
        SELECT class_name 
        FROM dev.class_metadata
        WHERE source_id = {source_id}
        AND download_flag = 't' 
        AND resource_name = 'OpenHouse'
    """
    rds_cursor.execute(query)
    results = rds_cursor.fetchall()
    classes = [r[0] for r in results]

    if len(classes) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No OpenHouse classes found for this source",
        }
        logger.info(log_msg)
        return

    select_fields = "LIST1,EVENT100,EVENT200"

    current_datetime = datetime.now()
    one_day_ago = current_datetime - timedelta(days=1)
    one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%dT%H:%M:%S")

    query_params = {
        "SearchType": "OpenHouse",
        "Query": f"(EVENT100={one_day_ago_formatted}+)",
        "Select": select_fields,
    }

    all_openhouse_data = []

    for class_name in classes:
        query_params["Class"] = class_name
        query_params["source_id"] = source_id

        df, count = data_download(rets_response, class_name, query_params)

        if count == 0:
            logger.debug(
                {
                    "source_id": source_id,
                    "class_name": class_name,
                    "message": "No OpenHouse Records Found",
                }
            )
        else:
            # Rename columns to match expected field names
            df = df.rename(
                columns={
                    "LIST1": "unique_listing_id",
                    "EVENT100": "event_start",
                    "EVENT200": "event_end",
                }
            )

            # Convert to records and add to list
            all_openhouse_data.extend(df.to_dict("records"))

            logger.info(
                {
                    "source_id": source_id,
                    "class_name": class_name,
                    "records_downloaded": len(df),
                    "total_records_so_far": len(all_openhouse_data),
                }
            )

    if len(all_openhouse_data) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No OpenHouse Records Downloaded from Source",
        }
        logger.info(log_msg)
        return

    try:
        rows_processed = process_openhouse_data(
            openhouse_data=all_openhouse_data,
            source_id=source_id,
            batch_id=batch_id,
            current_time=current_time,
            ls_cursor=ls_cursor,
            rds_cursor=rds_cursor,
            homelisting_conn=ls_conn,
            temp_table="temp_openhouse_sync",
            target_table="stage.direct_idx_openhouse_sync",
        )

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "openhouse_records_downloaded": len(all_openhouse_data),
            "openhouse_records_processed": rows_processed,
            "message": "OpenHouse data processed successfully",
        }
        logger.info(log_msg)

    except Exception as e:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "message": "Failed to process OpenHouse data",
        }
        logger.error(log_msg)
        raise


def lambda_handler(event, context):
    """Main Lambda function handler."""
    logger.info("Starting OpenHouse data sync lambda")
    logger.info(f"Event: {json.dumps(event, default=str)}")

    # Validate event has required fields
    required_fields = ["auth", "source_id", "source_name", "batch_id"]
    for field in required_fields:
        if field not in event:
            error_msg = f"Missing required field in event: {field}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    auth = event["auth"]

    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")

    if not listing_secret or not rds_secret:
        error_msg = (
            "Missing required environment variables: listingDatabase or rdsDatabase"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    sql_execlimit = context.get_remaining_time_in_millis() - 5000

    try:
        listing_secrets = fetch_secrets(listing_secret)
        rds_secrets = fetch_secrets(rds_secret)
    except Exception as e:
        logger.error(f"Failed to fetch secrets: {str(e)}")
        raise

    listing_conn = db_conn(listing_secrets, sql_execlimit)
    rds_conn = db_conn(rds_secrets, sql_execlimit)

    if not listing_conn or not rds_conn:
        error_msg = "Failed to establish database connections"
        logger.error(error_msg)
        raise ConnectionError(error_msg)

    listing_cursor = listing_conn.cursor()
    rds_cursor = rds_conn.cursor()

    try:
        event["download_status"] = False

        # Login to RETS server
        logger.info("Logging into RETS server...")
        rets_response = login(auth)
        logger.info("RETS login successful")

        # Download and process OpenHouse data for active listings
        download_openhouse_data(
            ls_cursor=listing_cursor,
            rds_cursor=rds_cursor,
            ls_conn=listing_conn,
            event=event,
            rets_response=rets_response,
            rds_conn=rds_conn,
        )

        event["download_status"] = True
        event["message"] = "OpenHouse data sync completed successfully"
        logger.info("Lambda execution completed successfully")

        return event

    except Exception as e:
        error_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(error_msg)
        event["download_status"] = False
        event["message"] = f"Lambda execution failed: {str(e)}"
        logger.error(event)
        return event

    finally:
        # Close all database connections
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
        if rds_cursor:
            rds_cursor.close()
        if rds_conn:
            rds_conn.close()
        logger.info("Database connections closed")
