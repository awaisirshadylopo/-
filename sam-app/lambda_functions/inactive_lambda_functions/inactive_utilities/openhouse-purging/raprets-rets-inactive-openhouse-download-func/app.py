"""Raprets Rets Inactive OpenHouse Download Lambda"""

import json
import os
import re
import logging
import traceback
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import boto3
import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
import psycopg2
from psycopg2 import extras
from urllib.parse import urljoin, urlparse

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
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
        log_msg = {
            "Message": "Connection not established",
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        logger.exception("Error while establishing connection: %s", log_msg)


def login(source_id, data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    headers = data.get("headers", {})

    session = requests.Session()
    if source_id != 445:
        session.headers.update(headers)
    session.auth = HTTPDigestAuth(username, password)
    response = session.get(login_url)

    if response.status_code == 200:
        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
            rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))

            # Fix relative URLs returned by the RETS server
            parsed = urlparse(login_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            for key in ("Search", "GetObject", "Logout", "GetMetadata"):
                if key in rets_data and rets_data[key].startswith("/"):
                    rets_data[key] = urljoin(base_url, rets_data[key])

            logger.info("Login successful!")
            rets_data["session"] = session
            return rets_data

        except Exception as e:
            log_msg = {
                "response_text": response_text,
                "response_status_code": response.status_code,
                "Error": e,
                "Error At line": traceback.format_exc(),
            }
            raise Exception(log_msg) from e

    else:
        log_msg = {
            "Message": "Login Failed",
            "response_text": response.text,
            "response_status_code": response.status_code,
        }
        raise Exception(
            f"Login failed with status code {log_msg['response_status_code']}"  # fixed key name
        )


def request_source(data):
    """Data Download from Rets Server Function"""

    session = data["session"]
    search_url = data["Search"]
    query_params = data["query_params"]
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)

        # Check for "no records" reply codes before attempting to parse data
        reply_code = root.attrib.get("ReplyCode", "0")
        reply_text = root.attrib.get("ReplyText", "")
        if reply_code == "20201" or "no records" in reply_text.lower():
            return pd.DataFrame(), 0

        count_element = root.find(".//COUNT")
        if count_element is None:
            raise Exception("COUNT element not found in response")
        data_count = int(count_element.get("Records"))

        columns = root.find("./COLUMNS").text.split("\t")[1:-1]

        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)

        return df_temp, data_count

    except (Exception, ET.ParseError, AttributeError) as e:
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText", "")
        if "no records" in reply_text.lower():  # case-insensitive fallback
            return pd.DataFrame(), 0

        log_msg = {
            "Message": "Data not downloaded",
            "response_text": response_text,
            "response_status_code": response.status_code,
        }
        log_msg.update(query_params)
        raise Exception(log_msg)


def add_default_columns(source_id, batch_id, openhouse_df):
    current_datetime = datetime.now()
    meta_df = pd.DataFrame(
        {
            "source_id": [int(source_id)],
            "batch_id": [int(batch_id)],
            "y_creation_date": current_datetime,
            "y_update_date": current_datetime,
        }
    )
    meta_df = pd.concat([meta_df] * len(openhouse_df), ignore_index=True)
    openhouse_df = openhouse_df.reset_index(drop=True)
    openhouse_df = pd.concat([meta_df, openhouse_df], axis=1)

    return openhouse_df


def download_all_openhouse(source_data, cursor_rds):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]

    current_datetime = datetime.now()
    one_day_ago = current_datetime - timedelta(days=1)
    one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%d")

    if source_id in (619,):
        select_fields = f"ListingRid,OpenHouseDate,StartTime,EndTime"
        hitting_query = f"(OpenHouseDate={one_day_ago_formatted}+)"

    elif source_id in (445,):
        select_fields = f"Listing_MUI,OpenHouseDate,StartTime,EndTime"
        hitting_query = f"(OpenHouseDate={one_day_ago_formatted}+)"
    elif source_id in (477,):
        select_fields = (
            f"ListingRid,OpenHouseRid,StartDateTime,StartDateTime,EndDateTime"
        )
        hitting_query = f"(StartDateTime={one_day_ago_formatted}+)"
    else:
        select_fields = f"ListingRid,StartDateTime,StartDateTime,EndDateTime"
        hitting_query = f"(StartDateTime={one_day_ago_formatted}+)"

    query_params = {
        "SearchType": "OpenHouse",
        "Query": hitting_query,
        "Select": select_fields,
        # "Limit": 1000,
    }

    query = f" select class_name from dev.class_metadata where source_id = {source_id} and resource_name ~*'openhouse' and download_flag is true; "
    cursor_rds.execute(query)
    classes = [row[0] for row in cursor_rds.fetchall()]

    temp_openhouse_df = pd.DataFrame()
    login_response = login(source_id, source_data["auth"])

    try:
        for class_name in classes:
            query_params["Class"] = class_name
            skip = 0

            while True:
                query_params["Offset"] = skip
                login_response["query_params"] = query_params

                df, data_count = request_source(login_response)

                if data_count > 0:
                    temp_openhouse_df = pd.concat(
                        [temp_openhouse_df, df], ignore_index=True
                    )

                elif skip == 0:
                    log_msg = {
                        "source_id": source_id,
                        "source_name": source_name,
                        "Message": f"No OpenHouse records found for class_name: {class_name}",
                    }
                    logger.warning(log_msg)
                if len(df) == 0:
                    break
                skip = skip + len(df)
                if skip >= data_count:
                    break

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Message": f"{skip} OpenHouse records found for class_name: {class_name}",
            }
            logger.info(log_msg)

        return temp_openhouse_df, select_fields
    except Exception as e:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "Message": "Error while downloading OpenHouse data",
            "success": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        logger.error(log_msg)
        raise Exception(log_msg)


def columns_renaming(final_df, source_id, resource_name, system_names, cursor_rds):
    """Renaming columns in the DataFrame based on metadata from the database."""

    system_names = "'" + system_names.replace(",", "','") + "'"
    renaming_cols = f"""select distinct lower(long_name), system_name from dev.field_metadata
    where source_id = {source_id} and resource_name ~* '{resource_name}' and system_name in ({system_names});"""
    cursor_rds.execute(renaming_cols)
    renamed_columns = cursor_rds.fetchall()

    for elem in renamed_columns:
        long_name = elem[0]
        system_name = elem[1]

        final_df.rename(columns={system_name: long_name}, inplace=True)

    return final_df


def fetch_openhouse_mappings(source_id, cursor_rds):

    fetch_mapping_query = f"""
        SELECT business_transformation FROM etl.mappings
        WHERE source_id = {source_id} AND resource_name ~*'OpenHouse' AND replace(target_column, '"', '')  IN ('date', 'start_time', 'end_time', 'source_listing_id')
        ORDER BY
            CASE replace(target_column, '"', '') 
                WHEN 'source_listing_id' THEN 0
                WHEN 'date' THEN 1
                WHEN 'start_time' THEN 2
                WHEN 'end_time' THEN 3
            END;
    """

    cursor_rds.execute(fetch_mapping_query)
    business_transformations = [row[0] for row in cursor_rds.fetchall()]

    return business_transformations


def insert_and_transform_openhouse_data(
    source_data,
    openhouse_df,
    system_names,
    homelisting_connection,
    cursor_homelisting,
    cursor_rds,
):

    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    temp_table = f"temp_openhouse_sync_{source_id}"
    stage_target_table = "stage.direct_idx_openhouse_sync"

    openhouse_df = columns_renaming(
        openhouse_df, source_id, "OpenHouse", system_names, cursor_rds
    )

    temp_table_fields = ",".join([f"{col} TEXT" for col in openhouse_df.columns])

    query = f""" CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            {temp_table_fields}
        ) ON COMMIT PRESERVE ROWS; """
    cursor_homelisting.execute(query)

    query = f""" ALTER TABLE {temp_table} 
            ADD COLUMN IF NOT EXISTS y_update_date TIMESTAMP,
            ADD COLUMN IF NOT EXISTS y_creation_date TIMESTAMP,
            ADD COLUMN IF NOT EXISTS batch_id BIGINT,
			ADD COLUMN IF NOT EXISTS source_id INT; """
    cursor_homelisting.execute(query)
    homelisting_connection.commit()

    openhouse_df = add_default_columns(source_id, batch_id, openhouse_df)

    insertion_columns = ",".join(list(openhouse_df.columns))
    insertion_data_values = [tuple(row) for row in openhouse_df.values]

    insert_query = f"""INSERT INTO {temp_table} ({insertion_columns}) VALUES %s """
    extras.execute_values(cursor_homelisting, insert_query, insertion_data_values)
    homelisting_connection.commit()

    openhouse_business_transformations = fetch_openhouse_mappings(source_id, cursor_rds)

    query = f" delete from {stage_target_table} where source_id = {source_id}; "
    cursor_homelisting.execute(query)
    homelisting_connection.commit()

    query = f"""
        INSERT INTO {stage_target_table}
        (source_id, batch_id, y_creation_date, y_update_date, ListingKey, OpenHouseDate, OpenHouseStartTime, OpenHouseEndTime)
        SELECT distinct source_id,batch_id,y_creation_date,y_update_date,
            {openhouse_business_transformations[0]},
            {openhouse_business_transformations[1]},
            {openhouse_business_transformations[2]},
            {openhouse_business_transformations[3]}
        FROM {temp_table} o
        where source_id = {source_id}
            AND batch_id = {batch_id}
    """
    cursor_homelisting.execute(query)
    homelisting_connection.commit()


def lambda_handler(event, context):
    """Main Lambda Handler Function"""

    try:

        source_data = event

        # <making_database_connections>
        listing_database = os.environ.get("listingDatabase")
        rds_database = os.environ.get("rdsDatabase")
        sql_exec_limit = context.get_remaining_time_in_millis()
        db_secret_rds = fetch_secrets(rds_database)
        db_secret_listing = fetch_secrets(listing_database)
        rds_connection = db_conn(db_secret_rds, sql_exec_limit)
        homelisting_connection = db_conn(db_secret_listing, sql_exec_limit)
        cursor_rds = rds_connection.cursor()  # type: ignore
        cursor_homelisting = homelisting_connection.cursor()  # type: ignore

        # <fetching_openhouse_data>
        openhouse_df, system_names = download_all_openhouse(source_data, cursor_rds)
        openhouse_df = openhouse_df.drop_duplicates()

        # <insertion_and_data_transformation>
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
            "success": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)

    finally:
        if cursor_rds:
            cursor_rds.close()
            rds_connection.close()
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()

    return source_data
