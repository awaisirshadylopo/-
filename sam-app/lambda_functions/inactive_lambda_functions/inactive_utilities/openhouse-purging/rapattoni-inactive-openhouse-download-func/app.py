"""Rapattoni API OpenHouse Download Lambda"""

import json
import os
import logging
import traceback
from datetime import datetime, timedelta
import boto3  # type: ignore
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from urllib.parse import urlparse

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


# Create token
def create_token(loginUrl, username, password):
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    payload = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": username,
        "client_secret": password,
    }
    try:
        response = requests.post(loginUrl, headers=headers, data=payload)
        response.raise_for_status()
        token_data = response.json()
        token = token_data["access_token"]

        parsed = urlparse(loginUrl)
        # netloc = parsed.netloc.replace("apiidentity", "api")
        netloc = parsed.netloc.replace("apiidentity", "api").replace(
            "identityapi", "api"
        )
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("Unexpected loginUrl format")
        mls_code = parts[0]  # SACM in your case
        loginUrl = f"{parsed.scheme}://{netloc}/{mls_code}/RESO/OData"
        loginUrl = f"{loginUrl}/$metadata"

        return loginUrl, token
    except requests.exceptions.RequestException as e:
        raise Exception(
            f"Token generation failed: {response.status_code}, {response.text}"
        )


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
    loginurl = source_data["auth"]["loginUrl"]
    token = source_data["auth"]["password"]

    current_datetime = datetime.now()
    one_day_ago = current_datetime - timedelta(days=1)
    one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%dT%H:%M:%S")

    query = f" select class_name from dev.class_metadata where source_id = {source_id} and resource_name ~*'openhouse' and download_flag is true; "
    cursor_rds.execute(query)
    classes = [row[0] for row in cursor_rds.fetchall()]

    if source_id in (1001, 884):
        select_fields = "ListingKeyNumeric,OpenHouseStartTime,OpenHouseEndTime"
        # loginurl, token = create_token(auth_url, username, auth_token)
        token = source_data["auth"]["access_token"]
    else:
        select_fields = "ListingKey,OpenHouseDate,OpenHouseStartTime,OpenHouseEndTime"

    loginurl = loginurl.replace("$metadata", "OpenHouse")
    headers = {"Authorization": f"Bearer {token}"}
    top = 200

    params = {
        "$filter": f"OpenHouseStartTime ge {one_day_ago_formatted}",
        "$select": select_fields,
        "$top": top,
        "$count": "true",
        "$skip": 0,
    }

    temp_openhouse_list = []

    for class_name in classes:
        skip = 0

        if source_id in (884, 1001):
            params["Class"] = class_name
            params["$filter"] = params["$filter"] + "-00:00"

        while True:
            params["$skip"] = skip
            response = None

            try:
                response = requests.get(
                    url=loginurl, params=params, headers=headers, timeout=30
                )
                response.raise_for_status()
                data = json.loads(response.text)

                source_count = data.get("@odata.count")
                temp_openhouse_list.extend(data["value"])

                skip += top
                if skip >= source_count:
                    break

            except requests.exceptions.RequestException as e:
                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": class_name,
                    "Error": e,
                    "Error At line": traceback.format_exc(),
                    "Response": response.text,
                }
                logger.error(log_msg)
                raise

    openhouse_df = pd.DataFrame(temp_openhouse_list)
    columns_to_keep = select_fields.split(",")
    openhouse_df = openhouse_df[
        columns_to_keep
    ]  # keep required columns and drop the rest coming from source

    return openhouse_df, select_fields


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

    # 1 Try given source_id
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

    # creation of temp table
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

    # insertion in temp table
    insertion_columns = ",".join(list(openhouse_df.columns))
    insertion_data_values = [tuple(row) for row in openhouse_df.values]

    insert_query = f"""INSERT INTO {temp_table} ({insertion_columns}) VALUES %s """
    extras.execute_values(cursor_homelisting, insert_query, insertion_data_values)
    homelisting_connection.commit()

    openhouse_business_transformations = fetch_openhouse_mappings(source_id, cursor_rds)

    # deletion from staging table
    query = f" delete from {stage_target_table} where source_id = {source_id}; "
    cursor_homelisting.execute(query)
    homelisting_connection.commit()

    # insertion in staging table
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
