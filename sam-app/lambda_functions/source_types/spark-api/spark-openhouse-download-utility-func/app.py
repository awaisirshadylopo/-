"""Spark API Validation and Data Download"""

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
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import time

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# fetch secrets
class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response_db = client.get_secret_value(SecretId=secret_name)
            # Decrypts secret using the associated KMS key.
            secret_db = get_secret_value_response_db["SecretString"]
            secret_dict_db = json.loads(secret_db)
            return secret_dict_db
        except ClientError as e:
            raise e


# setup DB connection
def db_conn(db_secret, sqlExecLimit):
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
            options=f"-c statement_timeout={int(sqlExecLimit)}",
        )

        return connection

    except Exception as e:
        log_msg = {
            "Message": "Connection not established",
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        logger.error(log_msg)


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

        # Rename columns to match target table
        df_openhouse_upload = df_openhouse_upload.rename(
            columns={
                "OpenHouseStartTime": "openhousestarttime",
                "OpenHouseEndTime": "openhouseendtime",
                "OpenHouseDate": "openhousedate",
            }
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
        # 3. Reorder columns for temp table (put required columns first)
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
                "message": f"Temp table `{temp_table}` checked/created successfully.",
            }
        )

        # -------------------------------------------------------------
        # 5. Insert data into temporary table
        # -------------------------------------------------------------
        # Convert DataFrame to dict records
        records = df_openhouse_upload.to_dict("records")

        # Parameterized insert query
        insert_temp_query = f"""
        INSERT INTO {temp_table}
        (source_id, batch_id, ListingKey, openhousedate, openhousestarttime, openhouseendtime, y_creation_date, y_update_date)
        VALUES (%(source_id)s, %(batch_id)s, %(ListingKey)s, %(openhousedate)s, %(openhousestarttime)s, %(openhouseendtime)s, %(y_creation_date)s, %(y_update_date)s)
        """
        # Execute the insert
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

        fetch_mapping_query = f"""
            SELECT business_transformation
            FROM {mapping_table}
            WHERE resource_name ~* 'openhouse' 
                AND target_column in ('date', 'start_time', 'end_time')
                AND source_id = %(source_id)s
            ORDER BY
                CASE target_column
                    WHEN 'date' THEN 1
                    WHEN 'start_time' THEN 2
                    WHEN 'end_time' THEN 3
                END;
        """
        rds_cursor.execute(fetch_mapping_query, {"source_id": source_id})
        mapping_rows = rds_cursor.fetchall()
        if not mapping_rows:
            raise ValueError(f"No mapping found for source_id={source_id}")

        mapping_expressions = [row[0] for row in mapping_rows]
        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "mapping_expressions": mapping_expressions,
            }
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

        # --------------------------------------------------------------------
        # 8. Insert transformed data into target table using dynamic mappings
        # --------------------------------------------------------------------
        # making sql select statement dynamically
        select_columns = (
            ["source_id", "batch_id", "ListingKey"]
            + mapping_expressions
            + ["y_creation_date", "y_update_date"]
        )
        select_sql = ", ".join(select_columns)

        # Log the dynamic SELECT SQL
        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "DYNAMIC_SELECT_SQL",
                "Source_id": source_id,
                "select_insert_sql": select_sql,
            }
        )
        # rds_cursor.execute(f"SELECT {select_sql} FROM {temp_table} LIMIT 5")
        # print("temp table data",rds_cursor.fetchall())
        # rds_cursor.execute(f"SELECT * FROM {temp_table} LIMIT 5")
        # print("temp  data",rds_cursor.fetchall())

        insert_dynamic_query = f"""
            INSERT INTO {target_table}
            (source_id, batch_id, ListingKey, openhousedate, openhousestarttime, openhouseendtime, y_creation_date, y_update_date)
            SELECT {select_sql}
            FROM {temp_table}
            where source_id = {source_id}
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

        # source_data["Openhouse_records_downloaded"] = len(openhouse_data)
        # source_data["Openhouse_records_inserted"] = len(rows_inserted)
        return rows_inserted

    except Exception as e:
        homelisting_conn.rollback()
        logger.error(
            {
                "step": "PROCESS_OPENHOUSE_FAILED",
                "error": str(e),
                "source_id": source_id,
                "batch_id": batch_id,
            }
        )
        raise


def download_all_Openhouse_func(
    source_data, cursor, rdsconnection, ls_cursor, homelisting_conn
):
    """
    Description:
        Purge open house records from the database that are no longer provided
        by the source.

    Parameters:
        source_data (dict):                    Dictionary containing at least 'source_id'.
        cursor (psycopg2.cursor):              Database cursor for executing queries.
        rdsconnection (psycopg2.connection):   Database connection object for serverless RDS.
        ls_cursor(psycopg2.cursor):            Homelisting Database cursor for executing queries
        homelisting_conn(psycopg2.connection): Database connection object for Homelisting DB.

    Returns:
        dict with Count of downloaded and inserted open house records in stage.direct_idx_openhouse_sync.

    Created:    2025-12-17                  Create By : Ammar Azkar
    """

    # -------------------------------------------------------------
    # 1. Read metadata from Lambda event
    # -------------------------------------------------------------
    source_id = source_data["source_id"]
    auth = source_data["auth"]
    loginurl = auth["loginUrl"]
    token = auth["password"]
    batch_id = source_data["batch_id"]

    # -------------------------------------------------------------
    # 2. Build Spark OpenHouse API endpoint and headers
    # -------------------------------------------------------------

    headers = {"Authorization": f"Bearer {token}"}
    loginurl = loginurl.replace("$metadata", "")
    url_endpoint = loginurl + "OpenHouse"

    # -------------------------------------------------------------
    # 3. Build query parameters (last 1 day data)
    # -------------------------------------------------------------
    top = 1000

    # Current timestamp for creation/update
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Calculate -1 day timestamp in UTC (ISO 8601)
    last_day_time = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    print(last_day_time)

    params = {
        "$filter": f"OpenHouseStartTime ge {last_day_time}",
        "$select": "ListingKey,OpenHouseStartTime,OpenHouseEndTime",
        "$top": top,
    }

    next_url = url_endpoint

    logger.info({"step": "API_REQUEST", "endpoint": url_endpoint, "params": params})
    # -------------------------------------------------------------
    # 4. Call Spark API and handle OData pagination
    # -------------------------------------------------------------

    openhouse_data = []
    openhouse = []
    page_count = 1

    while next_url:
        try:
            response = requests.get(
                url=next_url,
                params=params if next_url == url_endpoint else None,
                headers=headers,
            )
            response.raise_for_status()

            data = response.json()
            data["url_endpoint"] = next_url
            # data.update(params)

            openhouse.append(data)
            openhouse_data.extend(data.get("value", []))

            logger.info(
                {
                    "step": "API_PAGE",
                    "page": page_count,
                    "records_in_page": len(data),
                    "total_downloaded_so_far": len(openhouse_data),
                }
            )

            # OData pagination
            next_url = data.get("@odata.nextLink")

            # params should be used only for the first call
            params = None

        except requests.exceptions.RequestException:
            time.sleep(5)
            response = requests.get(
                url=next_url, params=params if params else None, headers=headers
            )
            response.raise_for_status()

            data = response.json()
            data["url_endpoint"] = next_url
            openhouse.append(data)
            openhouse_data.extend(data.get("value", []))
            next_url = data.get("@odata.nextLink")

    logger.info(
        {
            "source_id": source_id,
            "batch_id": batch_id,
            "step": "API_Download COMPLETE",
            "total_pages": page_count,
            "total_records_downloaded": len(openhouse_data),
        }
    )
    # -------------------------------------------------------------
    # 5. Transform API response to match DB schema
    # -------------------------------------------------------------

    if openhouse_data:

        rows_inserted = process_openhouse_data(
            openhouse_data,
            source_id,
            batch_id,
            current_time,
            ls_cursor,
            cursor,
            rdsconnection,
            homelisting_conn,
        )

        source_data["Openhouse_records_downloaded"] = len(openhouse_data)
        source_data["Openhouse_records_inserted"] = rows_inserted
    else:
        # print("No OpenHouse records found for the last 1 day.")
        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "step": "Download Openhouse Records",
                "records_inserted": "No OpenHouse records found for the last 1 day.",
            }
        )

    return source_data


def lambda_handler(event, context):

    source_data = event
    serverless_db_con = None
    pentaho_db_con = None
    cursor_rds = None
    cursor_homelisting = None
    event["success"] = False
    try:

        sqlExecLimit = context.get_remaining_time_in_millis()
        rdsDatabase = os.environ.get("rdsDatabase")
        listingDatabase = os.environ.get("listingDatabase")
        db_secret_dev = SecretManagerHelper.get_secret(rdsDatabase, "us-west-2")
        db_secret_stage = SecretManagerHelper.get_secret(listingDatabase, "us-west-2")
        serverless_db_con = db_conn(db_secret_dev, sqlExecLimit)
        pentaho_db_con = db_conn(db_secret_stage, sqlExecLimit)
        cursor_rds = serverless_db_con.cursor()
        cursor_homelisting = pentaho_db_con.cursor()

        # call def download_all_Openhouse_func
        final_response = download_all_Openhouse_func(
            source_data,
            cursor_rds,
            serverless_db_con,
            cursor_homelisting,
            pentaho_db_con,
        )
        event["success"] = True
        return final_response

    except Exception as e:

        log_msg = {"Error": str(e), "Error At Line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)

        return source_data

    finally:
        if cursor_homelisting:
            cursor_homelisting.close()
        if cursor_rds:
            cursor_rds.close()
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()
