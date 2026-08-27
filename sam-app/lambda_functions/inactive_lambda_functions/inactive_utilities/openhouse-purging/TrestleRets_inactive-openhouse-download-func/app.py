"""Lambda function for downloading and processing OpenHouse data for Rets Sources."""

import os
import traceback
import logging
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import boto3
import pandas as pd
import psycopg2
import requests
from requests.auth import HTTPBasicAuth


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
    except ConnectionError as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)
        return None


def login(data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    # Create a session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    session.auth = auth
    # Send login request
    response = session.get(login_url)

    # Check for successful login
    if response.status_code == 200:

        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
            rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
            logger.info("Login successful!")
            rets_data["session"] = session
            return rets_data
        except Exception as e:
            log_msg = {
                "response_text": response_text,
                "response_status_code": response.status_code,
                "Error": e,
            }
            raise ConnectionError(log_msg) from e

    else:
        log_msg = {
            "response_text": response.text,
            "response_status_code": response.status_code,
        }
        raise ConnectionError(log_msg)


def data_download(data):
    """Data Download from Rets Server Function"""
    session = data["session"]
    search_url = data["Search"]
    query_params = data["query_params"]
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"
    query_params["Limit"] = 500

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        # Extract column names
        count_element = root.find(".//COUNT")

        data_count = int(count_element.get("Records"))  # type: ignore
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore
        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]  # type: ignore
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except (ET.ParseError, AttributeError) as e:
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if reply_text and "No records found" in reply_text:
            return pd.DataFrame(), 0

        log_msg = {
            "response_text": response_text,
            "response_status_code": response.status_code,
            "Query": query_params,
            "Error": e,
        }
        raise ValueError(log_msg) from e


def clean_value(value):
    """Clean null or empty values."""
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


def fetch_openhouse_mappings(rds_cursor, mapping_table, source_id, batch_id):
    """Fetch dynamic mappings for OpenHouse columns from database."""

    EXPECTED_TARGETS = {
        "date": "openhousedate::timestamp::date AS openhousedate",
        "start_time": "OpenHouseStartTime::timestamp::time AS start_time",
        "end_time": "OpenHouseEndTime::timestamp::time AS end_time",
    }

    fetch_mapping_query = f"""
        SELECT replace(target_column, '"', '') AS target_column,
        CASE WHEN business_transformation ~* 'case' THEN NULL ELSE business_transformation END AS business_transformation
        FROM etl.mappings
        WHERE resource_name ~* 'openhouse'
          AND replace(target_column, '"', '')  IN ('date', 'start_time', 'end_time')
          AND source_id = %(source_id)s
        ORDER BY
            CASE replace(target_column, '"', '') 
                WHEN 'date' THEN 1
                WHEN 'start_time' THEN 2
                WHEN 'end_time' THEN 3
            END;
    """

    # 1 Try given source_id
    rds_cursor.execute(fetch_mapping_query, {"source_id": source_id})
    rows = rds_cursor.fetchall()

    found_mappings = {
        row[0]: row[1] for row in rows if row[1] is not None and row[1] != "null"
    }
    missing_targets = set(EXPECTED_TARGETS) - set(found_mappings)

    # 2 Fallback to source_id = 838
    # if missing_targets:
    #     logger.warning(
    #         f"Missing openhouse mappings {missing_targets} for source_id={source_id}, "

    #     )

    #     rds_cursor.execute(fetch_mapping_query, {"source_id": 838})
    #     fallback_rows = rds_cursor.fetchall()

    #     for target, transformation in fallback_rows:
    #         found_mappings.setdefault(target, transformation)

    #     missing_targets = set(EXPECTED_TARGETS) - set(found_mappings)

    # 3 Final fallback to defaults
    for target in missing_targets:
        logger.warning(f"Using default mapping for missing openhouse column '{target}'")
        found_mappings[target] = EXPECTED_TARGETS[target]

    mapping_expressions = [
        found_mappings["date"],
        found_mappings["start_time"],
        found_mappings["end_time"],
    ]

    logger.info(
        {
            "source_id": source_id,
            "batch_id": batch_id,
            "etl table": mapping_table,
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
    rdsconnection,
    homelisting_conn,
    temp_table="temp_openhouse_sync",
    target_table="stage.direct_idx_openhouse_sync",
    mapping_table="etl.mappings",
):
    """
    Process and insert openhouse data into target table with dynamic mappings from DB.
    """
    try:
        # -------------------------------------------------------------
        # 1. Load raw data into DataFrame and rename columns
        # -------------------------------------------------------------

        df_openhouse_upload = pd.DataFrame(openhouse_data)

        if len(df_openhouse_upload) == 0:
            logger.info("No OpenHouse data to process")
            return 0

        # Rename columns to match target table
        df_openhouse_upload = df_openhouse_upload.rename(
            columns={
                "OpenHouseStartTime": "openhousestarttime",
                "OpenHouseEndTime": "openhouseendtime",
                "OpenHouseDate": "openhousedate",
            }
        )

        # Clean the data
        df_openhouse_upload = df_openhouse_upload.apply(
            lambda col: col.map(clean_value)
        )

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "TRANSFORM_START",
                "records_received": len(df_openhouse_upload),
            }
        )

        # -------------------------------------------------------------
        # 2. Ensure required columns exist and add metadata columns
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
        # 3. Reorder columns for temp table
        # -------------------------------------------------------------

        df_openhouse_upload = df_openhouse_upload[
            ["source_id", "batch_id", "ListingKey"]
            + [
                col
                for col in df_openhouse_upload.columns
                if col
                not in [
                    "source_id",
                    "batch_id",
                    "ListingKey",
                    "y_creation_date",
                    "y_update_date",
                ]
            ]
            + ["y_creation_date", "y_update_date"]
        ]

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "TRANSFORM_COMPLETE",
                "records_ready_for_insert": len(df_openhouse_upload),
            }
        )

        # -------------------------------------------------------------
        # 4. Create temporary table with ON COMMIT PRESERVE ROWS
        # -------------------------------------------------------------

        create_temp_table_sql = f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            source_id INT,
            batch_id BIGINT,
            ListingKey TEXT,
            openhousedate TEXT,
            openhousestarttime TEXT,
            openhouseendtime TEXT, 
            y_creation_date timestamp, 
            y_update_date timestamp
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
        # 5. Insert data into temporary table
        # -------------------------------------------------------------
        records = df_openhouse_upload.to_dict("records")

        insert_temp_query = f"""
        INSERT INTO {temp_table}
        (source_id, batch_id, ListingKey, openhousedate, openhousestarttime, openhouseendtime, y_creation_date, y_update_date)
        VALUES (%(source_id)s, %(batch_id)s, %(ListingKey)s, %(openhousedate)s, %(openhousestarttime)s, %(openhouseendtime)s, %(y_creation_date)s, %(y_update_date)s)
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
        # 6. Fetch dynamic mappings from mapping table
        # -------------------------------------------------------------

        mapping_expressions = fetch_openhouse_mappings(
            rds_cursor, mapping_table, source_id, batch_id
        )

        # -------------------------------------------------------------
        # 7. Delete previous records in target table for the same source_id
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

        # --------------------------------------------------------------------
        # 8. Insert transformed data into target table using dynamic mappings
        # --------------------------------------------------------------------
        select_columns = (
            ["source_id", "batch_id", "ListingKey"]
            + mapping_expressions
            + ["y_creation_date", "y_update_date"]
        )
        select_sql = ", ".join(select_columns)

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "DYNAMIC_SELECT_SQL",
                "select_insert_sql": select_sql,
            }
        )

        insert_dynamic_query = f"""
            INSERT INTO {target_table}
            (source_id, batch_id, ListingKey, openhousedate, openhousestarttime, openhouseendtime, y_creation_date, y_update_date)
            SELECT {select_sql}
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
    originating_system_name = event["source_info"]["originating_system_name"]
    limit = event["source_info"].get(
        "limit", 500
    )  # Default high limit for active listings
    current_time = datetime.now()

    chunk_size = 10

    # Fetch all active listing IDs from listing_p_active
    query = f""" SELECT DISTINCT l.source_listing_id
    FROM listing_p_active l
    JOIN listing_openhouse o
    ON l.id = o.listing_id
    WHERE l.source_id = {source_id}
    AND o.date > CURRENT_DATE - INTERVAL '1 day';"""

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

    # Split listings into chunks
    active_listings_chunks = [
        active_listings[i : i + chunk_size]
        for i in range(0, len(active_listings), chunk_size)
    ]

    # Store all OpenHouse data
    all_openhouse_data = []

    # Get OpenHouse classes from class_metadata
    query = f""" SELECT class_name FROM dev.class_metadata
    WHERE source_id = {source_id}
    AND download_flag = 't' 
    AND resource_name = 'OpenHouse';
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

    # Download OpenHouse data for each chunk and class
    for chunk_index, listing_chunk in enumerate(active_listings_chunks):
        listing_chunk_str = ",".join(listing_chunk)
        query_filter = f"(OriginatingSystemName={originating_system_name}),(ListingKey={listing_chunk_str})"

        for class_name in classes:
            query_params = {
                "SearchType": "OpenHouse",
                "Class": class_name,
                "Query": query_filter,
                "Select": "ListingKey,OpenHouseDate,OpenHouseStartTime,OpenHouseEndTime",
            }
            rets_response["query_params"] = query_params

            # Data Downloading
            df, count = data_download(rets_response)
            if count == 0:
                logger.debug(
                    {
                        "source_id": source_id,
                        "class_name": class_name,
                        "chunk": chunk_index + 1,
                        "message": f"No OpenHouse Records Found",
                    }
                )
            else:
                all_openhouse_data.extend(df.to_dict("records"))

    if len(all_openhouse_data) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No OpenHouse Records Downloaded from Source",
        }
        logger.info(log_msg)
        return

    # Process OpenHouse data through transformation pipeline
    try:
        rows_processed = process_openhouse_data(
            openhouse_data=all_openhouse_data,
            source_id=source_id,
            batch_id=batch_id,
            current_time=current_time,
            ls_cursor=ls_cursor,
            rds_cursor=rds_cursor,
            rdsconnection=rds_conn,
            homelisting_conn=ls_conn,
            temp_table="temp_openhouse_sync",
            target_table="stage.direct_idx_openhouse_sync",
            mapping_table="etl.mappings",
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

    auth = event["auth"]

    # Get secrets from environment variables
    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")

    if not listing_secret or not rds_secret:
        error_msg = (
            "Missing required environment variables: listingDatabase or rdsDatabase"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Get remaining time for statement timeout
    sql_execlimit = (
        context.get_remaining_time_in_millis() - 5000
    )  # Subtract 5 seconds for safety

    # Fetch secrets
    try:
        listing_secrets = fetch_secrets(listing_secret)
        rds_secrets = fetch_secrets(rds_secret)
    except Exception as e:
        logger.error(f"Failed to fetch secrets: {str(e)}")
        raise

    # Establish database connections
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
