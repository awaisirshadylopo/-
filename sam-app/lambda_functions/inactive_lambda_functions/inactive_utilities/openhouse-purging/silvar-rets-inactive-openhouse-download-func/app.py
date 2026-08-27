"""Silvar Rets Inactive OpenHouse Data Download Lambda"""

import json
import re
import os
import logging
import traceback
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import boto3  # type: ignore
import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
import psycopg2
from psycopg2 import extras


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



def fetch_secrets(secret_name):
    """Getting Secrets"""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def db_conn(db_secret, sql_exec_limit):
    """Ylopo Database Connection Function"""
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
            options=f"-c statement_timeout={sql_exec_limit}",
        )
        return connection
    except Exception as e:
        raise Exception(e)


def login(data):
    """
    Silvar RETS login
    """
    login_url = data["loginUrl"]
    password  = data["password"]
    username  = data["user"]
    headers   = data["headers"]

    session = requests.Session()
    session.auth = HTTPDigestAuth(username, password)
    session.headers = headers

    response      = session.get(login_url)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
        rets_data["session"] = session
        return rets_data

    except Exception as e:
        log_msg = {
            "response_status_code": response.status_code,
            "response_text": response_text,
            "Error": e,
        }
        raise Exception(log_msg)


def request_source(data):
    """
    Download a single page of data from the RETS server.
    Handles the 'no records found' reply text gracefully.
    """
    session       = data["session"]
    search_url    = data["Search"]
    query_params  = data["query_params"]

    response      = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root          = ET.fromstring(response_text)
        count_element = root.find(".//COUNT")
        data_count    = int(count_element.get("Records"))  # type: ignore

        columns   = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]  # type: ignore
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except Exception as e:
        try:
            root       = ET.fromstring(response_text)
            reply_text = root.attrib.get("ReplyText", "")
            if "no records found" in reply_text.lower():
                return pd.DataFrame(), 0

            log_msg = {
                "response_text":        response_text,
                "query_params":         query_params,
                "response_status_code": response.status_code,
                "Error":                e,
            }
            raise Exception(log_msg)
        except Exception as inner_e:
            log_msg = {
                "response_status_code": response.status_code,
                "response_text":        response_text,
                "query_params":         query_params,
                "Error":                inner_e,
            }
            raise Exception(log_msg)


def download_all_openhouse(source_data, cursor_rds):
    """
    Download all upcoming / recent OpenHouse records from the Silvar RETS server.
    Paginates with Offset — but only sends Offset on page 2+ because Silvar
    rejects Offset=0 with ReplyCode 20203.
    """
    source_id   = source_data["source_id"]
    source_name = source_data["source_name"]

    # Silvar-specific field names (single source, safe to hard-code)
    select_fields = "PropertyID,StartDateTime,StartDateTime,EndDateTime"

    current_datetime      = datetime.now()
    one_day_ago           = current_datetime - timedelta(days=1)
    one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%d")

    hitting_query = f"(StartDateTime={one_day_ago_formatted}+)"

    # Base params — Offset intentionally excluded here; added per-page below
    base_query_params = {
        "SearchType": "OpenHouse",
        "Query":      hitting_query,
        "Select":     select_fields,
        "QueryType":  "DMQL2",
        "Format":     "COMPACT-DECODED",
        "Count":      "1",
        "Limit":      1000,
    }

    # Fetch OpenHouse class names configured for this source
    query = f"""
        SELECT class_name FROM dev.class_metadata
        WHERE source_id = {source_id}
          AND resource_name ~* 'openhouse'
          AND download_flag IS TRUE;
    """
    cursor_rds.execute(query)
    classes = [row[0] for row in cursor_rds.fetchall()]

    temp_openhouse_df = pd.DataFrame()
    login_response    = login(source_data["auth"])

    for class_name in classes:
        skip = 0

        while True:
            query_params = {**base_query_params, "Class": class_name}

            # Only send Offset after the first page —
            # Silvar rejects Offset=0 with ReplyCode 20203
            if skip > 0:
                query_params["Offset"] = skip

            login_response["query_params"] = query_params

            df, data_count = request_source(login_response)

            if data_count > 0:
                temp_openhouse_df = pd.concat(
                    [temp_openhouse_df, df], ignore_index=True
                )
            elif skip == 0:
                log_msg = {
                    "source_id":   source_id,
                    "source_name": source_name,
                    "Message":     f"No OpenHouse records found for class_name: {class_name}",
                }
                logger.warning(log_msg)

            skip += len(df)
            if skip >= data_count:
                break

        log_msg = {
            "source_id":   source_id,
            "source_name": source_name,
            "hitting_query" :     hitting_query,
            "Message":     f"{skip} OpenHouse records downloaded for class_name: {class_name}",
        }
        logger.info(log_msg)

    return temp_openhouse_df, select_fields

def columns_renaming(final_df, source_id, resource_name, system_names, cursor_rds):
    """Rename DataFrame columns using field_metadata mappings."""
    system_names_sql = "'" + system_names.replace(",", "','") + "'"
    renaming_cols = f"""
        SELECT DISTINCT lower(long_name), system_name
        FROM dev.field_metadata
        WHERE source_id = {source_id}
          AND resource_name ~* '{resource_name}'
          AND system_name IN ({system_names_sql});
    """
    cursor_rds.execute(renaming_cols)
    renamed_columns = cursor_rds.fetchall()

    for long_name, system_name in renamed_columns:
        final_df.rename(columns={system_name: long_name}, inplace=True)

    return final_df


def add_default_columns(source_id, batch_id, openhouse_df):
    """Prepend source_id, batch_id and timestamp columns."""
    current_datetime = datetime.now()
    meta_df = pd.DataFrame(
        {
            "source_id":       [int(source_id)],
            "batch_id":        [int(batch_id)],
            "y_creation_date": [current_datetime],
            "y_update_date":   [current_datetime],
        }
    )
    meta_df      = pd.concat([meta_df] * len(openhouse_df), ignore_index=True)
    openhouse_df = openhouse_df.reset_index(drop=True)
    openhouse_df = pd.concat([meta_df, openhouse_df], axis=1)
    return openhouse_df



def fetch_openhouse_mappings(source_id, cursor_rds):
    """
    Fetch the four business-transformation expressions for OpenHouse
    from etl.mappings, ordered: source_listing_id, date, start_time, end_time.
    """
    fetch_mapping_query = f"""
        SELECT business_transformation FROM etl.mappings
        WHERE source_id = {source_id}
          AND resource_name ~* 'OpenHouse'
          AND replace(target_column, '"', '') IN ('date', 'start_time', 'end_time', 'source_listing_id')
        ORDER BY
            CASE replace(target_column, '"', '')
                WHEN 'source_listing_id' THEN 0
                WHEN 'date'              THEN 1
                WHEN 'start_time'        THEN 2
                WHEN 'end_time'          THEN 3
            END;
    """
    cursor_rds.execute(fetch_mapping_query)
    return [row[0] for row in cursor_rds.fetchall()]


# ---------------------------------------------------------------------------
# Date cast helper
# ---------------------------------------------------------------------------

def ensure_date_cast(expr: str) -> str:
    """
    Ensures a SQL expression is cast to ::date BEFORE any AS alias
    (case-insensitive). Used to guarantee OpenHouseDate is stored as
    a date regardless of how the business_transformation is written.
    """
    if not expr:
        return expr
    expr  = expr.strip()
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


# ---------------------------------------------------------------------------
# Insert + transform into staging
# ---------------------------------------------------------------------------

def insert_and_transform_openhouse_data(
    source_data,
    openhouse_df,
    system_names,
    homelisting_connection,
    cursor_homelisting,
    cursor_rds,
):
    """
    1. Rename columns via field_metadata.
    2. Load into a per-session temp table.
    3. Delete old rows from stage.direct_idx_openhouse_sync for this source.
    4. Insert transformed rows using business-transformation expressions.
    """
    source_id          = source_data["source_id"]
    batch_id           = source_data["batch_id"]
    temp_table         = f"temp_openhouse_sync_{source_id}"
    stage_target_table = "stage.direct_idx_openhouse_sync"

    # Rename columns to canonical long_name values
    openhouse_df = columns_renaming(
        openhouse_df, source_id, "OpenHouse", system_names, cursor_rds
    )
    openhouse_df = openhouse_df.replace('', None)

    # Build & populate the temp table (all columns as TEXT)
    temp_table_fields = ",".join([f"{col} TEXT" for col in openhouse_df.columns])

    cursor_homelisting.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            {temp_table_fields}
        ) ON COMMIT PRESERVE ROWS;
    """)

    cursor_homelisting.execute(f"""
        ALTER TABLE {temp_table}
            ADD COLUMN IF NOT EXISTS y_update_date   TIMESTAMP,
            ADD COLUMN IF NOT EXISTS y_creation_date TIMESTAMP,
            ADD COLUMN IF NOT EXISTS batch_id        BIGINT,
            ADD COLUMN IF NOT EXISTS source_id       INT;
    """)
    homelisting_connection.commit()

    openhouse_df = add_default_columns(source_id, batch_id, openhouse_df)

    insertion_columns     = ",".join(list(openhouse_df.columns))
    insertion_data_values = [tuple(row) for row in openhouse_df.values]
    extras.execute_values(
        cursor_homelisting,
        f"INSERT INTO {temp_table} ({insertion_columns}) VALUES %s",
        insertion_data_values,
    )
    homelisting_connection.commit()

    # Fetch field-mapping expressions
    openhouse_business_transformations = fetch_openhouse_mappings(source_id, cursor_rds)

    # Purge stale rows then insert fresh data
    cursor_homelisting.execute(
        f"DELETE FROM {stage_target_table} WHERE source_id = {source_id};"
    )
    homelisting_connection.commit()

    cursor_homelisting.execute(f"""
        INSERT INTO {stage_target_table}
            (source_id, batch_id, y_creation_date, y_update_date,
             ListingKey, OpenHouseDate, OpenHouseStartTime, OpenHouseEndTime)
        SELECT DISTINCT
            source_id, batch_id, y_creation_date, y_update_date,
            {openhouse_business_transformations[0]},
            {ensure_date_cast(openhouse_business_transformations[1])},
            {openhouse_business_transformations[2]},
            {openhouse_business_transformations[3]}
        FROM {temp_table} o
        WHERE source_id = {source_id}
          AND batch_id  = {batch_id};
    """)
    homelisting_connection.commit()

    log_msg = {
        "source_id":          source_id,
        "source_name":        source_data["source_name"],
        "stage_target_table": stage_target_table,
        "inserted_count":     len(openhouse_df),
    }
    logger.info(log_msg)

def lambda_handler(event, context):
    """Main Lambda Handler Function"""

    source_data = event

    try:
        listing_database = os.environ.get("listingDatabase")
        rds_database     = os.environ.get("rdsDatabase")
        sql_exec_limit   = context.get_remaining_time_in_millis()

        db_secret_rds     = fetch_secrets(rds_database)
        db_secret_listing = fetch_secrets(listing_database)

        rds_connection         = db_conn(db_secret_rds, sql_exec_limit)
        homelisting_connection = db_conn(db_secret_listing, sql_exec_limit)

        cursor_rds         = rds_connection.cursor()         # type: ignore
        cursor_homelisting = homelisting_connection.cursor() # type: ignore

        # Fetch OpenHouse records from the Silvar RETS server
        openhouse_df, system_names = download_all_openhouse(source_data, cursor_rds)

        openhouse_df = openhouse_df.drop_duplicates()

        if openhouse_df.empty:
            logger.info({
                "source_id": source_data["source_id"],
                "source_name": source_data["source_name"],
                "message": "No OpenHouse records – skipping processing"
            })
            source_data["openhouse_records_downloaded"] = 0
            source_data["success"] = True
            return source_data



        # Transform and load into the staging table
        insert_and_transform_openhouse_data(
            source_data,
            openhouse_df,
            system_names,
            homelisting_connection,
            cursor_homelisting,
            cursor_rds,
        )

        source_data["openhouse_records_downloaded"] = len(openhouse_df)
        source_data["success"] = True

    except Exception as e:
        log_msg = {
            "success":       False,
            "Error":         str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)

    finally:
        try:
            cursor_rds.close()
            rds_connection.close()
        except Exception:
            pass
        try:
            cursor_homelisting.close()
            homelisting_connection.close()
        except Exception:
            pass

    return source_data