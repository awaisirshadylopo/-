"""RAPI Inactive OpenHouse Validation and Data Download"""

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
import re

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_token(auth):
    """
    Exchange RAPI client credentials for a bearer token.

    auth dict must contain:
        loginUrl  – token endpoint
        user      – client_id
        password  – client_secret
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
    }
    payload = json.dumps({
        "grant_type":    "client_credentials",
        "client_id":     str(auth["user"]),
        "client_secret": str(auth["password"]),
        "audience":      "rcapi.realcomp.com",
    })
    response = requests.post(
        url=auth["loginUrl"], headers=headers, data=payload, timeout=30
    )
    response.raise_for_status()
    return response.json()["access_token"]


class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])
        except ClientError as e:
            raise e

# Date-cast utility 
def ensure_date_cast(expr: str) -> str:
    """
    Ensures SQL expression is cast to ::date BEFORE any AS alias
    (case-insensitive).
    """
    if not expr:
        return expr

    expr = expr.strip()

    match = re.search(r"\s+as\s+(\w+)\s*$", expr, flags=re.IGNORECASE)

    if match:
        alias     = match.group(1)
        base_expr = expr[: match.start()].strip()
    else:
        alias     = None
        base_expr = expr

    if base_expr.lower().endswith("::date"):
        casted = base_expr
    else:
        casted = f"({base_expr})::date"

    return f"{casted} AS {alias}" if alias else casted



def fetch_openhouse_mappings(rds_cursor, mapping_table, source_id, batch_id):

    EXPECTED_TARGETS = {
        "date":       "openhousedate::timestamp::date AS openhousedate",
        "start_time": "OpenHouseStartTime::timestamp::time AS start_time",
        "end_time":   "OpenHouseEndTime::timestamp::time AS end_time",
    }

    fetch_mapping_query = f"""
        SELECT replace(target_column, '"', '') AS target_column,
               CASE WHEN business_transformation ~* 'case'
                    THEN NULL
                    ELSE business_transformation
               END AS business_transformation
        FROM etl.mappings
        WHERE resource_name ~* 'openhouse'
          AND replace(target_column, '"', '') IN ('date', 'start_time', 'end_time')
          AND source_id = %(source_id)s
        ORDER BY
            CASE replace(target_column, '"', '')
                WHEN 'date'       THEN 1
                WHEN 'start_time' THEN 2
                WHEN 'end_time'   THEN 3
            END;
    """

    rds_cursor.execute(fetch_mapping_query, {"source_id": source_id})
    rows = rds_cursor.fetchall()

    found_mappings = {
        row[0]: row[1] for row in rows if row[1] is not None and row[1] != "null"
    }
    missing_targets = set(EXPECTED_TARGETS) - set(found_mappings)

    for target in missing_targets:
        logger.warning(f"Using default mapping for missing openhouse column '{target}'")
        found_mappings[target] = EXPECTED_TARGETS[target]

    found_mappings["date"] = ensure_date_cast(found_mappings["date"])

    mapping_expressions = [
        found_mappings["date"],
        found_mappings["start_time"],
        found_mappings["end_time"],
    ]

    logger.info({
        "source_id":           source_id,
        "batch_id":            batch_id,
        "etl table":           mapping_table,
        "mapping_expressions": mapping_expressions,
    })

    return mapping_expressions

def db_conn(db_secret, sqlExecLimit):
    try:
        connection = psycopg2.connect(
            database=db_secret.get("dbname"),
            user=db_secret.get("username"),
            password=db_secret.get("password"),
            host=db_secret.get("host"),
            port=db_secret.get("port"),
            options=f"-c statement_timeout={int(sqlExecLimit)}",
        )
        return connection
    except Exception as e:
        logger.error({
            "Message":       "Connection not established",
            "Error":         e,
            "Error At line": traceback.format_exc(),
        })


# Transform + load                                          
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
        # ── 1. Load raw data into DataFrame and rename columns ────────────────
        df_openhouse_upload = pd.DataFrame(openhouse_data)

        df_openhouse_upload = df_openhouse_upload.rename(columns={
            "OpenHouseStartTime": "openhousestarttime",
            "OpenHouseEndTime":   "openhouseendtime",
            "OpenHouseDate":      "openhousedate",
        })

        logger.info({
            "source_id":        source_id,
            "batch_id":         batch_id,
            "step":             "TRANSFORM_START",
            "records_received": len(df_openhouse_upload),
        })

        # ── 2. Ensure required columns exist and add metadata columns ─────────
        required_cols = [
            "ListingKey", "openhousedate", "openhousestarttime", "openhouseendtime",
        ]
        for col in required_cols:
            if col not in df_openhouse_upload.columns:
                df_openhouse_upload[col] = None

        df_openhouse_upload["source_id"]       = source_id
        df_openhouse_upload["batch_id"]        = batch_id
        df_openhouse_upload["y_creation_date"] = current_time
        df_openhouse_upload["y_update_date"]   = current_time

        
        df_openhouse_upload = df_openhouse_upload[
            ["source_id", "batch_id", "ListingKey"]
            + [
                col for col in df_openhouse_upload.columns
                if col not in [
                    "source_id", "batch_id", "ListingKey",
                    "y_creation_date", "y_update_date",
                ]
            ]
            + ["y_creation_date", "y_update_date"]
        ]

        logger.info({
            "source_id":                source_id,
            "batch_id":                 batch_id,
            "step":                     "TRANSFORM_COMPLETE",
            "records_ready_for_insert": len(df_openhouse_upload),
        })

        
        create_temp_table_sql = f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            source_id          INT,
            batch_id           BIGINT,
            ListingKey         TEXT,
            openhousedate      DATE,
            openhousestarttime TEXT,
            openhouseendtime   TEXT,
            y_creation_date    TIMESTAMP,
            y_update_date      TIMESTAMP
        ) ON COMMIT PRESERVE ROWS;
        """
        ls_cursor.execute(create_temp_table_sql)
        homelisting_conn.commit()

        logger.info({
            "source_id": source_id,
            "batch_id":  batch_id,
            "step":      "TEMP_TABLE_CHECK",
            "message":   f"Temp table `{temp_table}` checked/created successfully.",
        })

        # ── 5. Insert data into temporary table
        records = df_openhouse_upload.to_dict("records")

        insert_temp_query = f"""
        INSERT INTO {temp_table}
        (source_id, batch_id, ListingKey, openhousedate, openhousestarttime,
         openhouseendtime, y_creation_date, y_update_date)
        VALUES (%(source_id)s, %(batch_id)s, %(ListingKey)s, %(openhousedate)s,
                %(openhousestarttime)s, %(openhouseendtime)s,
                %(y_creation_date)s, %(y_update_date)s)
        """
        try:
            ls_cursor.executemany(insert_temp_query, records)
            homelisting_conn.commit()

            ls_cursor.execute(
                f"SELECT COUNT(1) FROM {temp_table} WHERE source_id = %s AND batch_id = %s",
                (source_id, batch_id),
            )
            temp_table_count = ls_cursor.fetchone()[0]

            logger.info({
                "source_id":              source_id,
                "batch_id":               batch_id,
                "step":                   "TEMP_INSERT_COMPLETE",
                "records_sent_to_insert": len(records),
                "records_in_temp_table":  temp_table_count,
                "table":                  temp_table,
                "count_mismatch":         len(records) != temp_table_count,
            })
        except Exception as e:
            homelisting_conn.rollback()
            logger.error({
                "source_id": source_id,
                "batch_id":  batch_id,
                "step":      "TEMP_INSERT_FAILED",
                "error":     str(e),
                "table":     temp_table,
            })
            raise

        # ── 6. Fetch dynamic mappings ─────────────────────────────────────────
        mapping_expressions = fetch_openhouse_mappings(
            rds_cursor, mapping_table, source_id, batch_id
        )

        # ── 7. Delete previous records in target table ────────────────────────
        delete_previous_query = f"""
            DELETE FROM {target_table}
            WHERE source_id = %(source_id)s
        """
        ls_cursor.execute(delete_previous_query, {"source_id": source_id})
        homelisting_conn.commit()

        
        select_columns = (
            ["source_id", "batch_id", "ListingKey"]
            + mapping_expressions
            + ["y_creation_date", "y_update_date"]
        )
        select_sql = ", ".join(select_columns)

        logger.info({
            "source_id":         source_id,
            "batch_id":          batch_id,
            "step":              "DYNAMIC_SELECT_SQL",
            "select_insert_sql": select_sql,
        })

        insert_dynamic_query = f"""
            INSERT INTO {target_table}
            (source_id, batch_id, ListingKey, openhousedate, openhousestarttime,
             openhouseendtime, y_creation_date, y_update_date)
            SELECT {select_sql}
            FROM {temp_table} o
            WHERE source_id = {source_id}
              AND batch_id   = {batch_id}
        """
        logger.info({
            "source_id":  source_id,
            "batch_id":   batch_id,
            "step":       "DYNAMIC_INSERT_SQL",
            "insert_sql": insert_dynamic_query,
        })

        ls_cursor.execute(insert_dynamic_query)
        homelisting_conn.commit()
        rows_inserted = ls_cursor.rowcount

        ls_cursor.execute(
            f"SELECT COUNT(1) FROM {target_table} WHERE source_id = %s",
            (source_id,),
        )
        target_table_count = ls_cursor.fetchone()[0]

        logger.info({
            "source_id":               source_id,
            "batch_id":                batch_id,
            "step":                    "DB_INSERT_COMPLETE",
            "rows_inserted_this_run":  rows_inserted,
            "total_rows_in_target":    target_table_count,
            "table":                   target_table,
            "rows_lost_in_transform":  None,
        })

        return rows_inserted

    except Exception as e:
        homelisting_conn.rollback()
        logger.error({
            "step":      "PROCESS_OPENHOUSE_FAILED",
            "error":     str(e),
            "source_id": source_id,
            "batch_id":  batch_id,
        })
        raise


def download_all_Openhouse_func(
    source_data, cursor, rdsconnection, ls_cursor, homelisting_conn
):

    # ── 1. Read metadata from Lambda event ────────────────────────────────────
    source_id = source_data["source_id"]
    auth      = source_data["auth"]
    batch_id  = source_data["batch_id"]

    # ── 2. Build RAPI OpenHouse endpoint ──────────────────────────────────────
    #   RAPI uses auth["metadataUrl"] and replaces "$metadata" with "OpenHouse".
    base_url     = auth["metadataUrl"]
    url_endpoint = base_url.replace("$metadata", "OpenHouse")
    logger.info({
        "source_id": source_id,
        "batch_id":  batch_id,
        "message":   "RAPI OpenHouse URL built",
        "url":       url_endpoint,
    })

    # ── 3. Obtain bearer token via RAPI OAuth flow ────────────────────────────
    token = create_token(auth)
    headers = {
        "User-Agent":    "Ylopo",
        "Authorization": f"Bearer {token}",
    }

    
    top           = 200
    current_time  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    last_day_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    params = {
        "$filter": f"OpenHouseStartTime ge {last_day_time}",
        "$select": "ListingKey,OpenHouseDate,OpenHouseStartTime,OpenHouseEndTime",
        "$top":    top,
    }

    source_total_count = None
    
    openhouse_data = []
    page_count     = 0
    skip           = 0
    params["$count"] = "true"
    total_source_count=None

    while True:
        page_count += 1
        params["$skip"] = skip
        response = None
        try:
            response = requests.get(
                url=url_endpoint,
                params=params,
                headers=headers,
            )
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error({
                "source_id": source_id,
                "batch_id":  batch_id,
                "step":      "API_REQUEST_FAILED — refreshing token and retrying",
                "page":      page_count,
                "skip":      skip,
                "error":     str(e),
                "response":  response.text if response else None,
            })
            time.sleep(30)
            token = create_token(auth)
            headers["Authorization"] = f"Bearer {token}"
            response = requests.get(
                url=url_endpoint,
                params=params,
                headers=headers,
            )
            response.raise_for_status()

        data         = response.json()
        if skip==0:
            total_source_count=data.get("@odata.count")
        page_records = data.get("value", [])
        openhouse_data.extend(page_records)

        # Stop when the page is not full — no more records left
        if len(page_records) < top:
            break

        skip += top

    logger.info({
        "source_id":                source_id,
        "batch_id":                 batch_id,
        "total_source_count":      total_source_count,
        "step":                     "API_DOWNLOAD_COMPLETE",
        "filter_applied":           f"OpenHouseStartTime ge {last_day_time}",
        "total_pages_fetched":      page_count,
        "total_skip_used":          skip,
        "total_records_downloaded": len(openhouse_data),
    })

    
    if openhouse_data:
        rows_inserted = process_openhouse_data(
            openhouse_data,
            source_id,
            batch_id,
            current_time,
            ls_cursor,        # homelisting cursor
            cursor,           # serverless / RDS cursor
            rdsconnection,    # serverless / RDS connection
            homelisting_conn, # homelisting connection
        )
        source_data["Openhouse_records_downloaded"] = len(openhouse_data)
        source_data["Openhouse_records_inserted"]   = rows_inserted

        
        logger.info({
            "source_id":                  source_id,
            "batch_id":                   batch_id,
            "step":                       "FINAL_RECONCILIATION",
            "filter_applied":             f"OpenHouseStartTime ge {last_day_time}",
            "source_reported_count":      total_source_count,
            "api_records_downloaded":     len(openhouse_data),
            "count_gap_from_source":      (total_source_count - len(openhouse_data)) if total_source_count is not None else "N/A",
            "records_inserted_to_target": rows_inserted,
            "records_lost_in_transform":  len(openhouse_data) - rows_inserted,
        })
    else:
        logger.info({
            "source_id":        source_id,
            "batch_id":         batch_id,
            "step":             "Download Openhouse Records",
            "records_inserted": "No OpenHouse records found for the last 1 day.",
        })

    return source_data


def lambda_handler(event, context):

    source_data        = event
    serverless_db_con  = None
    pentaho_db_con     = None
    cursor_rds         = None
    cursor_homelisting = None
    event["success"]   = False

    try:
        sqlExecLimit    = context.get_remaining_time_in_millis()
        rdsDatabase     = os.environ.get("rdsDatabase")
        listingDatabase = os.environ.get("listingDatabase")

        db_secret_dev   = SecretManagerHelper.get_secret(rdsDatabase,    "us-west-2")
        db_secret_stage = SecretManagerHelper.get_secret(listingDatabase, "us-west-2")

        serverless_db_con  = db_conn(db_secret_dev,   sqlExecLimit)
        pentaho_db_con     = db_conn(db_secret_stage, sqlExecLimit)
        cursor_rds         = serverless_db_con.cursor()
        cursor_homelisting = pentaho_db_con.cursor()

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
        log_msg = {
            "Error":         str(e),
            "Error At Line": traceback.format_exc(),
        }
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