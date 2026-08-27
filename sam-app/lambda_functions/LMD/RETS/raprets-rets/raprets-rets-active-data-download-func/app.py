"""Raprets Rets Active Data Download Lambda"""

import json
import re
import os
import io
import math
import logging
import traceback
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import boto3
import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
import psycopg2
from psycopg2 import extras
from urllib.parse import urlparse
import ast

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s - %(message)s", force=True)


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
    except ConnectionError as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


def login(data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    headers = data.get("headers", {})
    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    # Send login request
    response = session.get(login_url, headers=headers)

    # Check for successful login
    response_text = response.text
    try:
        root = ET.fromstring(response_text)
        rets_response_element = root.find("RETS-RESPONSE")

        if rets_response_element is not None and rets_response_element.text:
            rets_response_text = rets_response_element.text.strip()
            rets_data = dict(
                re.findall(r"(\w+)=([^\n\r]*)", rets_response_text)
            )  # This line will make a dictionary of the response returned by the API after logging in.
            logger.info("Login successful!")
            rets_data["session"] = session
            rets_data["loginUrl"] = login_url
            return rets_data
        else:
            raise Exception(root)

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.get("ReplyText")
        raise Exception(reply_text)


def execute_query(connection, query, cursor, query_mode=None):
    """Execute a SQL query on the given connection and cursor."""

    log_msg = {"Executed Query": query}
    logger.info(log_msg)
    cursor.execute(query)

    data = ""
    # if query_mode None define that query is for selection otherwise query mode  is 'insert'
    if query_mode is None:
        data = cursor.fetchone()
    elif query_mode == "All":
        data = cursor.fetchall()
    else:
        try:
            generated_id = cursor.fetchone()
            connection.commit()
            return generated_id
        except psycopg2.ProgrammingError:
            connection.commit()
            return None

    return data


def align_row(values, columns):
    values = [v.strip() if isinstance(v, str) else v for v in values]

    if values and values[0] == "":
        values = values[1:]
    if values and values[-1] == "":
        values = values[:-1]

    if len(values) > len(columns):
        return values[: len(columns)]
    if len(values) < len(columns):
        values += [None] * (len(columns) - len(values))

    return values


def data_download(data, auth, headers, max_retries=1):
    session = data["session"]

    login_url = data["loginUrl"]

    if "raprets" in login_url:
        search_url = login_url.split("6103")[0] + "6103" + data["Search"]
    else:
        search_url = data["Search"]

    query_params = data["query_params"]
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"

    for attempt in range(max_retries + 1):
        response = session.get(search_url, headers=headers, params=query_params)
        response_text = response.text

        try:
            root = ET.fromstring(response_text)

            if root.tag == "RETS" and root.attrib.get("ReplyCode") != "0":
                reply_code = root.attrib.get("ReplyCode")
                reply_text = root.attrib.get("ReplyText")

                if reply_code == "20203":
                    logger.warning(
                        "Session invalid, re-logging in... attempt %d", attempt + 1
                    )
                    # Re-login and retry
                    new_login_data = login(auth)
                    if not new_login_data:
                        raise ValueError(f"Login failed during retry: {reply_text}")
                    data.update(new_login_data)
                    session = data["session"]
                    continue
                elif reply_code == "20201":
                    logger.info("No records found for this query.")
                    return pd.DataFrame(), 0
                else:
                    raise ValueError(f"RETS Error {reply_code}: {reply_text}")

            # Extracting Columns
            columns_element = root.find(".//COLUMNS")

            columns = columns_element.text.split("\t")
            if columns[0] == "":
                columns = columns[1:]
            if columns and columns[-1] == "":
                columns = columns[:-1]

            # Extracting data rows
            data_rows = []
            for data_element in root.findall(".//DATA"):
                row_values = data_element.text.split("\t")
                if row_values[0] == "":
                    row_values = row_values[1:]
                if row_values and row_values[-1] == "":
                    row_values = row_values[:-1]
                # while row_values and row_values[-1] == "":
                #     row_values.pop()
                # while row_values and row_values[0] == "":
                #     row_values.pop(0)
                # Handle column mismatch
                if len(row_values) > len(columns):
                    row_values = row_values[: len(columns)]

                # elif len(row_values) < len(columns):
                #     row_values.extend([None] * (len(columns) - len(row_values)))

                data_rows.append(row_values)

            df_temp = pd.DataFrame(data_rows, columns=columns)

            df_temp.insert(0, "query_params", str(query_params))
            df_temp.insert(0, "search_url", str(search_url))

            return df_temp, len(data_rows)

        except Exception as e:
            log_msg = {
                "response_text": response_text,
                "query_params": query_params,
                "Error": e,
            }
            raise ValueError(log_msg) from e


def get_count(data, headers, source_id, source_name, archival_params):
    session = data["session"]
    query_params = data["query_params"]

    login_url = data["loginUrl"]
    search_path = data["Search"]

    if "raprets" in login_url:
        search_url = login_url.split("6103")[0] + "6103" + data["Search"]
    else:
        search_url = data["Search"]

    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    count_value = None
    response = session.get(search_url, params=query_params, headers=headers)

    response_text = response.text
    try:
        root = ET.fromstring(response_text)
        count_element = root.find(".//COUNT")
        count_value = int(count_element.get("Records"))  # type: ignore

        df = pd.DataFrame(
            [
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "total_count": count_value,
                    "search_url": str(search_url),
                    "query_params": str(query_params),
                }
            ]
        )

        Upload_data_into_S3_DataLake(df, archival_params, "Request")

        return count_value
    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.get("ReplyText")
        raise Exception(reply_text)


def formatted_date(date):
    """Format the date to ISO 8601 format with UTC timezone."""
    # if no date found then use dummy date
    dummy_date = "1990-01-01 00:00:00"
    naive_datetime = datetime.strptime(dummy_date, "%Y-%m-%d %H:%M:%S")
    original_datetime_utc = naive_datetime.replace(tzinfo=timezone.utc)
    formatted_datetime_utc = original_datetime_utc.strftime("%Y-%m-%dT%H:%M:%S")

    # data may be None or a tuple
    if not date:
        return formatted_datetime_utc

    if isinstance(date, str):
        try:
            # Handle datetime string with milliseconds (e.g., "2024-10-23 08:30:28.334")
            original_datetime_aware = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
            original_datetime_aware = original_datetime_aware.replace(
                tzinfo=timezone.utc
            )
            return original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # If it doesn't match the expected format, return the default date
            return formatted_datetime_utc

    # If the input `date` is a tuple or any other format, handle it
    if isinstance(date, tuple):
        return formatted_date(date[0])  # Recursively call if the date is in a tuple

    # If it's a datetime object, convert it to the correct format
    if isinstance(date, datetime):
        date = date.replace(tzinfo=timezone.utc)  # Ensure it's in UTC timezone
        return date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    # If none of the above, return the default formatted date
    return formatted_datetime_utc


def get_max_last_modified_date(
    serverless_db_con,
    source_id,
    cursor_serverless,
    cursor_pentaho,
    flow,
    rolling_window_offset=None,
):
    """Get the maximum last modified date for a given source_id and flow type."""
    query_for_last_modified_date_serverless = None

    if flow == "backlog":
        query_for_last_modified_date_serverless = f"""
            SELECT bl_start_date::timestamp(0), last_media_modified_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {source_id}"""

    elif flow == "sold":
        query = f"""SELECT last_modified_date, batch_id, sold_date
        FROM stage.serverless_idx_loads WHERE source_id = {source_id};"""
        cursor_serverless.execute(query)
        data = cursor_serverless.fetchall()
        df = pd.DataFrame(data, columns=["last_modified_date", "batch_id", "sold_date"])

        currect_lmd = df["last_modified_date"][0]
        #
        previous_batch_id = df["batch_id"][0]
        sold_date = df["sold_date"][0]
        query = f"""select max(modification_timestamp)::timestamp from listing
        WHERE source_id = {source_id} and batch_id = {previous_batch_id};"""
        cursor_pentaho.execute(query)
        data = cursor_pentaho.fetchall()
        df = pd.DataFrame(data, columns=["max_modification_timestamp"])
        previous_lmd = df["max_modification_timestamp"][0]
        #
        default_modification_timestamp = "1990-01-01 00:00:00"

        modification_timestamp = ""

        if currect_lmd == previous_lmd:
            modification_timestamp = default_modification_timestamp

        else:

            query = f"""select min(sold_date), max(sold_date) from listing_p_sold
            where source_id = {source_id} and batch_id = {previous_batch_id};"""
            cursor_pentaho.execute(query)
            data = cursor_pentaho.fetchall()
            df = pd.DataFrame(data, columns=["min_sold_date", "max_sold_date"])
            min_sold_date = df["min_sold_date"][0]
            max_sold_date = df["max_sold_date"][0]

            if min_sold_date == max_sold_date:
                query = f"""select max(modification_timestamp)::timestamp from listing_p_sold
                where source_id = {source_id} and batch_id = {previous_batch_id};"""
                cursor_pentaho.execute(query)
                data = cursor_pentaho.fetchall()
                df = pd.DataFrame(data, columns=["max_modification_timestamp"])
                max_modification_timestamp = df["max_modification_timestamp"][0]
                modification_timestamp = max_modification_timestamp
            else:
                modification_timestamp = default_modification_timestamp

        formatted_date_serverless = formatted_date(modification_timestamp)

        return formatted_date_serverless, str(sold_date)

    elif flow == "respecs":
        query_for_last_modified_date_serverless = f"""
            SELECT respecs_start_date::timestamp(0), last_media_modified_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {source_id}"""

    elif flow == "full_load":
        query_for_last_modified_date_serverless = f"""
            SELECT full_load_date::timestamp(0), last_media_modified_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {source_id}"""

    elif flow == "rolling_window":
        # if rolling_window_offset is not None:
        # then we need to subtract the rolling_window_offset from the last_modified_date
        query_for_last_modified_date_serverless = f"""
            SELECT last_modified_date::timestamp(0) - interval '{rolling_window_offset} hours', last_media_modified_date::timestamp(0) 
            FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    else:
        query_for_last_modified_date_serverless = f"""
        SELECT
            last_modified_date::timestamp(0), last_media_modified_date::timestamp(0)
        FROM 
            stage.serverless_idx_loads
        WHERE
            source_id = {source_id}
        """

    max_date_serverless = execute_query(
        serverless_db_con, query_for_last_modified_date_serverless, cursor_serverless
    )
    last_media_modified_date = max_date_serverless[1] if max_date_serverless[1] else datetime.now(timezone.utc)  # type: ignore
    last_modified_date = formatted_date(max_date_serverless[0])  # type: ignore
    last_media_modified_date = formatted_date(last_media_modified_date)

    return last_modified_date, last_media_modified_date


def get_hitting_query(source_data, flow_type):
    """Construct the hitting query for the data source."""
    data_source_info = source_data["source_info"]
    status_column = data_source_info["status_column"]
    last_modified_date = source_data["last_modification_date"]
    propertyType = data_source_info["propertyType"]
    propertyType_column = data_source_info["propertyType_column"]
    modification_column = data_source_info["modification_column"]
    listingkeycol = data_source_info["listingkey_column"]

    dt = datetime.strptime(last_modified_date, "%Y-%m-%dT%H:%M:%S.%f")
    last_modified_date = dt.strftime("%Y-%m-%dT%H:%M:%S")

    if flow_type == "sold":
        sold_column = data_source_info["sold_column"]
        sold_status = data_source_info["sold_status"]
        sold_date = source_data["sold_date"]

        return f"({sold_column}={sold_date}+),({modification_column}={last_modified_date}+),({status_column}=|{sold_status})"

    elif flow_type == "full_load" or flow_type == "respecs":
        active_status = data_source_info["active_status"]
        return f"({modification_column}={last_modified_date}+),({status_column}=|{active_status}),({listingkeycol}=0+),~({propertyType}=|{propertyType_column})"

    else:
        last_media_modified_date = source_data["last_media_modification_date"]
        return f"({modification_column}={last_modified_date}+),({listingkeycol}=0+)"


def download_preparation(cursor_serverless, source_id):
    """creating DataFrame for Data Download"""

    download_query = f"""SELECT
    	cmd.source_id,
        cmd.resource_name,
        cmd.class_name,
        field_metadata.column_normalized,field_metadata.long_name_normalized
        FROM
        dev.class_metadata cmd
        INNER JOIN (
            select
            	fmd.source_id,
                fmd.resource_name,
                fmd.class_name,
                fmd.key_value,
                STRING_AGG(fmd.system_name, ',') column_normalized,
                STRING_AGG(fmd.long_name, '|') long_name_normalized
            FROM 
                dev.field_metadata as fmd where fmd.download_flag=true and fmd.active_flag=true
            GROUP BY 
                fmd.resource_name,
                fmd.class_name,
                fmd.source_id,
                fmd.key_value
        ) AS field_metadata
        ON 
        cmd.resource_name = field_metadata.resource_name
        AND cmd.class_name = field_metadata.class_name
        and cmd.source_id = field_metadata.source_id 
        where
            cmd.source_id ='{source_id}'
            and cmd.active_flag=true
            and cmd.download_flag=true order by cmd.id;
        """

    cursor_serverless.execute(download_query)
    source_data = cursor_serverless.fetchall()
    columns = [desc[0] for desc in cursor_serverless.description]
    data_for_download = pd.DataFrame(source_data, columns=columns)

    data_for_download["target_table"] = data_for_download.apply(
        lambda row: "ps_rets" + "_" + row["resource_name"] + "_" + str(source_id),
        axis=1,
    )

    # Sort by the custom sort key first, and then by 'resource_name'
    data_for_download["sort_key"] = data_for_download["resource_name"].apply(
        lambda x: 0 if x == "Property" else 1
    )
    data_for_download = (
        data_for_download.sort_values(by=["sort_key"])
        .drop(columns="sort_key")
        .reset_index(drop=True)
    )

    return data_for_download


def clean_value(value):
    """Clean the value by checking for NaN or specific strings and returning None if found.
    Args:
        value: The value to be cleaned.
    Returns:
        The cleaned value, which is None if the original value is NaN or a specific string.
    """
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


def table_creation_and_loading(
    df,
    table_name,
    source_id,
    source_name,
    resource_name,
    cursor_serverless,
    serverless_db_con,
):
    """Loading DataFrame to the table in serverless database"""
    df.fillna(pd.NaT)
    df.fillna("")
    df_instance_filtered = df.apply(lambda col: col.map(clean_value))
    column_names = f"""SELECT column_name FROM information_schema.columns
    WHERE table_name ~* '{table_name}' and column_name not in ('id')"""
    cursor_serverless.execute(column_names)
    table_column_names = [column[0] for column in cursor_serverless.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    df_cols = list(df_instance_filtered.columns)

    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = f"""ALTER TABLE idx_stage.{table_name}
            ADD COLUMN  IF NOT EXISTS {n} TEXT"""
            cursor_serverless.execute(alter_query)
            insert_query = f""" INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name)
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}');
                """
            cursor_serverless.execute(insert_query)
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": resource_name,
                "Alter Query": alter_query,
                "Insert Query": insert_query,
                "Message": f"Added new column {n} to {table_name}",
            }
            logger.info(log_msg)
            serverless_db_con.commit()

    cols = ",".join(list(df_instance_filtered.columns))
    cols = cols.replace(",order,", ',"order",')
    data_values = [tuple(row) for row in df_instance_filtered.values]
    insert_query = f"""
    INSERT INTO idx_stage.{table_name} ({cols}) VALUES %s
    """
    extras.execute_values(cursor_serverless, insert_query, data_values)
    serverless_db_con.commit()


def adding_extra_columns(
    df, batch_creation_date, source_id, batch_id, source_creation_date
):
    cols_to_insert = {
        "source_id": int(source_id),
        "batch_id": int(batch_id),
        "source_last_update_date": source_creation_date,
        "y_creation_date": batch_creation_date,
        "y_last_update_date": batch_creation_date,
        "source_creation_date": source_creation_date,
    }
    for col, val in reversed(
        list(cols_to_insert.items())
    ):  # reversed to keep column order
        if col in df.columns:
            df[col] = val  # update value
        else:
            df.insert(0, col, val)
    return df


def download_data_for_given_list(
    chunks_list,
    resource_name,
    class_name,
    response,
    auth,
    source_id,
    source_name,
    column_name,
    headers,
):

    agent_col = response["agent_column"]
    office_col = response["office_column"]

    last_modified_date = response["last_modified_date"]
    dt = datetime.strptime(last_modified_date, "%Y-%m-%dT%H:%M:%S.%f")
    last_modified_date = dt.strftime("%Y-%m-%dT%H:%M:%S")

    data_df = pd.DataFrame()

    for sub_chunk in chunks_list:
        limit = len(sub_chunk)
        sub_chunk = '"' + '","'.join(list(filter(None, sub_chunk))) + '"'
        if resource_name in ["ActiveAgent", "Agent"]:
            query = f"({agent_col}={sub_chunk})"
        else:
            query = f"({office_col}={sub_chunk})"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": query,
            "count": 1,
            "Limit": limit,
        }
        response["query_params"] = query_params

        # Data Downloading
        df, count = data_download(response, auth, headers=headers, max_retries=3)
        if count and count == 0:
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "resource_name": resource_name,
                "class_name": class_name,
                "download_count": count,
                "Query": query,
            }
            logger.warning(log_msg)
        else:
            df["class_name"] = class_name
            data_df = pd.concat([data_df, df], ignore_index=True)

    return data_df


def chunks_creation(df, key_column_names, chunk_size):
    value_keys = []

    # Combine all values from valid columns
    for column in key_column_names:
         if column in df.columns:
            values = (
                df[column]
                .dropna()
                .astype(str)
                .str.strip()
                .str.replace("'", "", regex=False)
                .str.replace("&", "", regex=False)
            )
            

            # Remove empty and null-like values
            values = values[
                (values != "") &
                (~values.str.lower().isin(["nan", "none", "null"]))
            ]

            value_keys.extend(values.tolist())
     # Remove duplicates while preserving order
    unique_value_keys = list(dict.fromkeys(value_keys))

    # Split into chunks
    key_chunks = [
        unique_value_keys[i:i + chunk_size]
        for i in range(0, len(unique_value_keys), chunk_size)
    ]

    return key_chunks


def columns_renaming(final_df, source_id, resource_name, cursor_serverless):
    """Renaming columns in the DataFrame based on metadata from the database."""
    renaming_cols = f"""select distinct lower(long_name), system_name from dev.field_metadata
    where source_id = {source_id} and resource_name = '{resource_name}' ;"""
    cursor_serverless.execute(renaming_cols)
    renamed_columns = cursor_serverless.fetchall()

    if renamed_columns[0] is None:
        return final_df
    else:
        for elem in renamed_columns:
            long_name = elem[0]
            system_name = elem[1]

            final_df.rename(columns={system_name: long_name}, inplace=True)

        return final_df


def Upload_data_into_S3_DataLake(
    df_upload,
    archival_params,
    skip=None,
):

    source_id = archival_params["source_id"]
    source_type = archival_params["source_type"]
    source_name = archival_params["source_name"]
    resource_name = archival_params["resource_name"]
    batch_id = archival_params["batch_id"]
    # Construct filename and folder path
    filename = f"{source_name}_{resource_name}.parquet"

    if skip is not None:  # requesting source for temp_table
        filename = f"{source_name}_{resource_name}_{skip}.parquet"

    folder_path = f"{source_type}/{source_id}_{source_name}/{resource_name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload.columns = df_upload.columns.map(lambda x: x.replace(".", "_"))

    # Step 3: Clean values — convert to str, then handle null-like strings
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    # Convert DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")

    # Optional: Ensure folders exist (not strictly required in S3 since it's flat storage)
    # You can create a zero-byte object as folder markers if needed
    def ensure_folder_structure(s3, bucket, path):
        parts = path.strip("/").split("/")
        cumulative_path = ""
        for part in parts:
            cumulative_path += part + "/"
            s3.put_object(Bucket=bucket, Key=cumulative_path)  # Creates "folder"

    ensure_folder_structure(s3, bucket_name, folder_path)

    # Upload parquet file
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())


def download_temp_table(
    source_data,
    response,
    auth,
    get_classes,
    flow_type,
    cursor_serverless,
    serverless_db_con,
):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    headers = source_data["auth"].get("headers", {})
    listingkeycol = source_data["source_info"]["listingkey_column"]
    modification_column = source_data["source_info"]["modification_column"]
    source_type = source_data["source_info"]["source_type"]

    hitting_query = get_hitting_query(source_data, flow_type)

    all_temp_data = pd.DataFrame()

    resource_name = "Property"
    table_name = "temp_table"
    lmd_date = source_data["last_modification_date"]
    lmd_date = str(lmd_date).split(".", 1)[0].replace(":", "").replace("-", "")
    temp_data = pd.DataFrame()

    archival_params = {
            "source_id": source_id,
            "source_type": source_type,
            "source_name": source_name,
            "resource_name": resource_name,
            "batch_id": f"{lmd_date}_temp",
        }

    for class_name in get_classes:

        select = f"{listingkeycol},{modification_column}"
        if flow_type == "sold":
            sold_column = source_data["source_info"]["sold_column"]
            select = select + f",{sold_column}"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": hitting_query,
            "Select": select,
        }

        response["query_params"] = query_params

        count = get_count(response, headers, source_id, source_name, archival_params)

        if count == 0:
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "resource_name": resource_name,
                "class_name": class_name,
                "download_count": count,
                "Query": hitting_query,
            }
            logger.info(log_msg)

            continue

        top = 1000
        skip = 0
        rounds = math.ceil(count / top)
        while rounds > 0:
            query_params = {
                "SearchType": resource_name,
                "Class": class_name,
                "Query": hitting_query,
                "Select": select,
                "Limit": top,
                "Offset": skip,
            }
            response["query_params"] = query_params

            # get df and count
            df, _ = data_download(response, auth, headers, max_retries=3)

            df.rename(
                columns={
                    f"{listingkeycol}": "listingkey",
                    f"{modification_column}": "modification_timestamp",
                },
                inplace=True,
            )

            temp_data = pd.concat([temp_data, df], ignore_index=True)

            skip = len(temp_data)

            if skip + top > count:
                top = count - skip
                if top <= 0:
                    break
            rounds -= 1
            if skip + top >= 1000000:
                break

        if len(temp_data) > 0:
            # temp_data.insert(0, "class_name", class_name)
            all_temp_data = pd.concat([all_temp_data, temp_data], ignore_index=True)

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "resource_name": resource_name,
            "class_name": class_name,
            "download_count": len(temp_data),
            "Query": hitting_query,
        }
        logger.info(log_msg)

    # temp_data = pd.concat([temp_data, final_df], ignore_index=True)
    if len(all_temp_data) != 0:
        if flow_type == "respecs":
            all_temp_data.insert(0, "respecs_flag", True)
        elif flow_type == "sold":
            all_temp_data.rename(
                columns={f"{sold_column}": "sold_date"},
                inplace=True,
            )

        all_temp_data.insert(0, "source_id", int(source_id))

        all_temp_data = all_temp_data.drop(["search_url", "query_params"], axis=1)

        all_temp_data.drop_duplicates(inplace=True)

        Upload_data_into_S3_DataLake(all_temp_data, archival_params)

        table_creation_and_loading(
            all_temp_data,
            table_name,
            source_id,
            source_name,
            resource_name,
            cursor_serverless,
            serverless_db_con,
        )
    else:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "resource_name": resource_name,
            "download_count": 0,
        }
        logger.info(log_msg)
        return False
    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "resource_name": resource_name,
        "download_count": len(all_temp_data),
        "Query": hitting_query,
    }
    logger.info(log_msg)

    return True


def photo_count(data, url, listing_col, picture_col, photo_type_column):

    loading_list = []
    for index, row in data.iterrows():
        id = row[listing_col]
        count = row[picture_col]
        for i in range(0, int(count)):
            photo_url = f"{url}?Type={photo_type_column}&Resource=Property&id={id}:{i+1}&Location=0"
            loading_list.append(
                {
                    "class_name": "Media",
                    "content_id": int(id),
                    "object_id": int(i),
                    "url": photo_url,
                }
            )

    return pd.DataFrame(loading_list)


def request_and_load_tables(
    source_data,
    response,
    auth,
    cursor_serverless,
    serverless_db_con,
    prefix,
    data_for_download,
):

    def clean_extra_columns(df):
        cols_lower = [c.lower().strip() for c in df.columns]
        columns_to_remove = [
            "source_creation_date",
            "y_creation_date",
            "source_last_update_date",
            "y_last_update_date",
            "batch_id",
            "source_id",
        ]
        drop_cols = [
            df.columns[i] for i, c in enumerate(cols_lower) if c in columns_to_remove
        ]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        return df

    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_name = source_data["source_name"]
    limit = prefix.get("limit", 1000)
    flow_type = source_data["flow_type"]
    headers = source_data["auth"].get("headers", {})
    bl_flag = source_data["batch_execution_params"]["bl_flag"]

    temp_respecs_flag = "f"
    orderby_column = "modification_timestamp"
    orderby_type = "asc"

    if flow_type in ["lmd", "rolling_window"] and bl_flag is True:
        orderby_type = "desc"
    elif flow_type == "respecs":
        temp_respecs_flag = "t"
    elif flow_type == "sold":
        orderby_column = "sold_date"

    query = f""" select
        distinct on ({orderby_column}::timestamp, listingkey)
        listingkey::int as listingkey
        from idx_stage.temp_table
        where source_id ={source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}'
        order by {orderby_column}::timestamp {orderby_type}, listingkey
        Limit {limit};
        """

    cursor_serverless.execute(query)
    temp_table_data = cursor_serverless.fetchall()
    listing_key = [val[0] for val in temp_table_data]

    if len(listing_key) == 0:
        log_msg = {
            "message": f"no data in idx_stage.temp_table for the query: {query}",
            "source_id": source_id,
            "source_name": source_name,
        }
        logger.info(log_msg)
        return False

    chunk_size = 10
    listing_chunk_size = 100
    media_chunk_size = 10
    member_mls_ids_chunks = []
    member_keys_chunks = []
    office_mls_ids_chunks = []
    office_keys_chunks = []
    listings = []
    listings_key_for_media = []

    listings.extend(
        [
            listing_key[i : i + listing_chunk_size]
            for i in range(0, len(listing_key), listing_chunk_size)
        ]
    )

    listings_key_for_media.extend(
        [
            listing_key[i : i + media_chunk_size]
            for i in range(0, len(listing_key), media_chunk_size)
        ]
    )

    listingkeycol = source_data["source_info"]["listingkey_column"]
    o_listingkeycol = source_data["source_info"]["o_listingkey_column"]
    p_listingkeycol = source_data["source_info"]["p_listingkey_column"]
    modification_column = source_data["source_info"]["modification_column"]
    source_type = source_data["source_info"]["source_type"]
    last_modified_date = source_data["last_modification_date"]
    picture_count = source_data["source_info"]["photo_count"]
    photo_type_column = source_data["source_info"].get("photo_type_column", None)

    # Agent information to download data
    agent_mlsid_columns = source_data["source_info"].get("agent_mlsid_columns")
    agent_mlsid_columns = (
        ast.literal_eval(agent_mlsid_columns)
        if isinstance(agent_mlsid_columns, str)
        else agent_mlsid_columns
    )

    agent_key_columns = source_data["source_info"].get("agent_key_columns")
    agent_key_columns = (
        ast.literal_eval(agent_key_columns)
        if isinstance(agent_key_columns, str)
        else agent_key_columns
    )

    agent_col = source_data["source_info"].get("agent_column")

    # Office information to download data
    office_mlsid_columns = source_data["source_info"].get("office_mlsid_columns")
    office_mlsid_columns = (
        ast.literal_eval(office_mlsid_columns)
        if isinstance(office_mlsid_columns, str)
        else office_mlsid_columns
    )

    office_key_columns = source_data["source_info"].get("office_key_columns")
    office_key_columns = (
        ast.literal_eval(office_key_columns)
        if isinstance(office_key_columns, str)
        else office_key_columns
    )

    office_col = source_data["source_info"].get("office_column")

    response["agent_column"] = agent_col
    response["office_column"] = office_col
    response["last_modified_date"] = last_modified_date

    media_get_object = source_data["source_info"]["media_get_object"]

    dt = datetime.strptime(last_modified_date, "%Y-%m-%dT%H:%M:%S.%f")
    last_modified_date = dt.strftime("%Y-%m-%dT%H:%M:%S")

    source_creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    media_download_set = pd.DataFrame()
    classes = None

    archival_params = {
        "source_id": source_id,
        "source_type": source_type,
        "source_name": source_name,
        "batch_id": batch_id,
    }

    for _, row in data_for_download.iterrows():
        target_table = row["target_table"]
        resource_name = row["resource_name"]
        class_name = row["class_name"]
        class_name_list = row["class_name"].splitlines()

        data_df = pd.DataFrame()

        if row["resource_name"] == "Property":

            for listing_chunk in listings:
                listing_chunk = ",".join(str(i) for i in listing_chunk)
                for classes in class_name_list:
                    if classes == "ALLL":
                        query = f"({listingkeycol}={listing_chunk})"
                    else:
                        query = f"({modification_column}=1990-01-01+),({listingkeycol}={listing_chunk})"

                    query_params = {
                        "SearchType": resource_name,
                        "Class": classes,
                        "Query": query,
                        "count": 1,
                        "Limit": listing_chunk_size,
                    }
                    response["query_params"] = query_params

                    # Data Downloading
                    df, count = data_download(response, auth, headers, max_retries=3)

                    if count == 0:
                        log_msg = {
                            "source_id": source_id,
                            "source_name": source_name,
                            "resource_name": resource_name,
                            "class_name": classes,
                            "download_count": count,
                            "Query": query,
                        }
                        logger.warning(log_msg)

                    else:
                        df = clean_extra_columns(df)
                        df["class_name"] = classes
                        data_df = pd.concat([data_df, df], ignore_index=True)

            # Making agent chunks for mlsids
            if agent_mlsid_columns:
                member_mls_ids_chunks = chunks_creation(
                    data_df, agent_mlsid_columns, chunk_size
                )
            else:
                member_mls_ids_chunks = []
            # Making agent chunks for keys
            if agent_key_columns:
                member_keys_chunks = chunks_creation(
                    data_df, agent_key_columns, chunk_size
                )
            else:
                member_keys_chunks = []

            # Making office chunks for mlsids
            if office_mlsid_columns:
                office_mls_ids_chunks = chunks_creation(
                    data_df, office_mlsid_columns, chunk_size
                )
            else:
                office_mls_ids_chunks = []
            # Making office chunks for keys
            if office_key_columns:
                office_keys_chunks = chunks_creation(
                    data_df, office_key_columns, chunk_size
                )
            else:
                office_keys_chunks = []

        elif row["resource_name"] in ["ActiveAgent", "Agent"]:
            for class_name in class_name_list:
                agent_df_mls = download_data_for_given_list(
                    member_mls_ids_chunks,
                    resource_name,
                    class_name,
                    response,
                    auth,
                    source_id,
                    source_name,
                    "ListingAgentMLSID",
                    headers,
                )
                agent_df_keys = download_data_for_given_list(
                    member_keys_chunks,
                    resource_name,
                    class_name,
                    response,
                    auth,
                    source_id,
                    source_name,
                    "ListingAgentNumber",
                    headers,
                )
                agent_df = pd.concat([agent_df_mls, agent_df_keys], ignore_index=True)
                agent_df = clean_extra_columns(agent_df)
                data_df = pd.concat([data_df, agent_df], ignore_index=True)

        elif row["resource_name"] in ["Office", "ActiveOffice"]:
            for class_name in class_name_list:
                office_df_mls = download_data_for_given_list(
                    office_mls_ids_chunks,
                    resource_name,
                    class_name,
                    response,
                    auth,
                    source_id,
                    source_name,
                    "ListingOfficeMLSID",
                    headers,
                )
                office_df_keys = download_data_for_given_list(
                    office_keys_chunks,
                    resource_name,
                    class_name,
                    response,
                    auth,
                    source_id,
                    source_name,
                    "ListingOfficeNumber",
                    headers,
                )
                office_df = pd.concat(
                    [office_df_mls, office_df_keys], ignore_index=True
                )
                office_df = clean_extra_columns(office_df)
                data_df = pd.concat([data_df, office_df], ignore_index=True)

        elif row["resource_name"] == "Media" and not media_get_object:
            for listing_chunk in listings_key_for_media:
                listing_chunk = ",".join(str(i) for i in listing_chunk)
                for classes in class_name_list:
                    query = f"({p_listingkeycol}={listing_chunk})"

                    query_params = {
                        "SearchType": resource_name,
                        "Class": classes,
                        "Query": query,
                        "Limit": (media_chunk_size * 50),
                    }
                    response["query_params"] = query_params
                    # Data Downloading
                    df, count = data_download(response, auth, headers, max_retries=3)
                    if count == 0:
                        log_msg = {
                            "source_id": source_id,
                            "source_name": source_name,
                            "resource_name": resource_name,
                            "class_name": classes,
                            "download_count": count,
                            "Query": query,
                        }
                        logger.warning(log_msg)

                    else:
                        df = clean_extra_columns(df)
                        df["class_name"] = classes
                        data_df = pd.concat([data_df, df], ignore_index=True)

        elif row["resource_name"] in [
            "OpenHouse",
            "CustomProperty",
            "PropertyRooms",
            "PropertyUnitTypes",
        ]:
            for listing_chunk in listings:
                listing_chunk = ",".join(str(i) for i in listing_chunk)
                for classes in class_name_list:
                    query = f"({o_listingkeycol}={listing_chunk})"

                    query_params = {
                        "SearchType": resource_name,
                        "Class": classes,
                        "Query": query,
                        "Limit": (listing_chunk_size * 10),
                    }
                    response["query_params"] = query_params

                    # Data Downloading
                    df, count = data_download(response, auth, headers, max_retries=3)
                    if count == 0:
                        log_msg = {
                            "source_id": source_id,
                            "source_name": source_name,
                            "resource_name": resource_name,
                            "class_name": classes,
                            "download_count": count,
                            "Query": query,
                        }
                        logger.warning(log_msg)

                    else:
                        df = clean_extra_columns(df)
                        df["class_name"] = classes
                        data_df = pd.concat([data_df, df], ignore_index=True)

        # Right after downloading the property for each class this part will run if the media_get_object is true.
        if row["resource_name"] == "Property" and media_get_object:
            # Put the listing and photo_count column here.

            if data_df.empty:
                continue

            media_download_set = data_df[
                [f"{listingkeycol}", f"{picture_count}"]
            ].copy()

            # It will make the GetObject URL and to pass it to photo_count()
            get_object_url = response["GetObject"]
            login_url = response["loginUrl"]

            if "raprets" in login_url:
                get_object_url = login_url.split("6103")[0] + "6103" + get_object_url

            media_df = photo_count(
                media_download_set,
                get_object_url,
                listing_col=listingkeycol,
                picture_col=picture_count,
                photo_type_column=photo_type_column,
            )
            media_df = adding_extra_columns(
                media_df, batch_creation_date, source_id, batch_id, source_creation_date
            )
            table_creation_and_loading(
                media_df,
                f"ps_rets_media_{source_id}",
                source_id,
                source_name,
                "Media",
                cursor_serverless,
                serverless_db_con,
            )

            archival_params["resource_name"] = "Media"
            Upload_data_into_S3_DataLake(media_df, archival_params)

            del media_df

        if len(data_df) != 0:
            data_df = data_df.drop_duplicates()
            data_df = columns_renaming(
                data_df, source_id, resource_name, cursor_serverless
            )
            data_df.columns = data_df.columns.str.lower()
            data_df = data_df.loc[:, ~data_df.columns.duplicated()]
            data_df = adding_extra_columns(
                data_df,
                batch_creation_date,
                source_id,
                batch_id,
                source_creation_date,
            )

            archival_params["resource_name"] = resource_name
            Upload_data_into_S3_DataLake(data_df, archival_params)

            data_df = data_df.drop(["search_url", "query_params"], axis=1)

            table_creation_and_loading(
                data_df,
                target_table,
                source_id,
                source_name,
                resource_name,
                cursor_serverless,
                serverless_db_con,
            )

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "resource_name": resource_name,
            "class_name": classes,
            "download_count": len(data_df),
            "Message": f"Inserted into {target_table}",
        }
        logger.info(log_msg)

    return True


def validation_func(serverless_db_con, cursor_serverless, cursor_pentaho, source_data):
    """Validation Function"""

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    data_source_info = source_data["source_info"]
    auth = source_data["auth"]

    try:

        runtime_count = source_data["runtime_count"]
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params["bl_flag"]
        itr_value = batch_execution_params["itr_value"]
        respecs_flag = batch_execution_params["respecs_flag"]
        flow_type = data_source_info["flow_type"]
        rolling_window_batch = data_source_info["rolling_window_batch"]
        rolling_window_offset = None

        if flow_type not in ["sold", "full_load"]:

            if respecs_flag is True and (runtime_count % itr_value != 0):
                flow_type = "respecs"

            elif bl_flag is True and (runtime_count % itr_value != 0):
                flow_type = "backlog"

            elif runtime_count % rolling_window_batch == 0:
                rolling_window_offset = data_source_info["rolling_window_offset"]
                flow_type = "rolling_window"

        temp_respecs_flag = "f"
        if flow_type == "respecs":
            temp_respecs_flag = "t"

        source_data["flow_type"] = flow_type
        source_data["status_column"] = data_source_info["status_column"]
        # source_data['modification_column'] = data_source_info['modification_column']
        max_last_modified_date = None
        last_media_modified_date = None
        if flow_type == "sold":
            max_last_modified_date, sold_date = get_max_last_modified_date(
                serverless_db_con,
                source_id,
                cursor_serverless,
                cursor_pentaho,
                flow_type,
            )
            source_data["sold_date"] = sold_date

            source_data["sold_column"] = data_source_info["sold_column"]
            source_data["download_status"] = data_source_info["sold_status"]
            data_source_info["sold_date"] = sold_date
        else:
            if flow_type == "full_load":
                source_data["download_status"] = data_source_info["active_status"]

            # last modified date for source_id execution to download data from that date
            max_last_modified_date, last_media_modified_date = (
                get_max_last_modified_date(
                    serverless_db_con,
                    source_id,
                    cursor_serverless,
                    cursor_pentaho,
                    flow_type,
                    rolling_window_offset,
                )
            )

            source_data["last_media_modification_date"] = last_media_modified_date

        source_data["last_modification_date"] = max_last_modified_date

        query = f"""
            select distinct class_name from dev.class_metadata c
            where c.source_id = {source_id} and c.download_flag = true and c.resource_name ='Property'
            order by 1;
        """
        get_classes = execute_query(serverless_db_con, query, cursor_serverless, "All")
        get_classes = [row[0] for row in get_classes]  # type: ignore

        query = f""" DELETE FROM idx_stage.temp_table
        where source_id = {source_id} and download_flag = 'f' ; """

        cursor_serverless.execute(query)
        serverless_db_con.commit()

        log_message = {
            "source_id": source_id,
            "source_name": source_name,
            "deleted_count": cursor_serverless.rowcount,
            "Query": query,
        }
        logger.info(log_message)

        # Common function to get total count for all classes
        def get_total_count():
            query = f""" select count(distinct listingkey) from idx_stage.temp_table 
                where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """

            cursor_serverless.execute(query)
            result = cursor_serverless.fetchone()[0]
            total_count = int(result)
            return int(result)

        # Set temp_table_status based on conditions
        total_count = get_total_count()

        source_data["temp_table_status"] = False if total_count > 0 else True

        # always download temp_table for lmd batch during backlog (with last_modified_date)
        # always download temp_table for rolling_window batch (with last_modified_date)
        if (bl_flag is True and flow_type == "lmd") or flow_type == "rolling_window":
            source_data["temp_table_status"] = True

        if source_data["temp_table_status"]:
            # server session creation
            response = login(auth)
            status = download_temp_table(
                source_data,
                response,
                auth,
                get_classes,
                flow_type,
                cursor_serverless,
                serverless_db_con,
            )
            source_data["status"] = status
            total_count = get_total_count()

        log_message = {
            "source_id": source_id,
            "source_name": source_name,
            "flow_type": flow_type,
            "temp_table_status": source_data["temp_table_status"],
            "total_count": total_count,
        }
        logger.info(log_message)

        source_data["row_count"] = total_count

        query = f""" select max(modification_timestamp) from idx_stage.temp_table
            where source_id = {source_id} and download_flag = 't'  and respecs_flag = '{temp_respecs_flag}' """

        cursor_serverless.execute(query)
        latest_listing_date = cursor_serverless.fetchone()[0]
        latest_listing_date = (
            max_last_modified_date
            if latest_listing_date is None
            else formatted_date(latest_listing_date)
        )
        source_data["latest_listing_date"] = latest_listing_date
        source_data["download_flag"] = True

        return source_data

    except (
        psycopg2.Error,
        requests.RequestException,
        ET.ParseError,
        ValueError,
        KeyError,
        AttributeError,
    ) as e:
        # Logging an error message
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }

        # Returning an error response
        source_data.update(log_msg)
        logger.error(source_data)

        return source_data


def data_download_func(cursor_serverless, serverless_db_con, source_data):
    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    run_host = source_data["run_host"]
    source_name = source_data["source_name"]
    flow = source_data["flow_type"]
    prefix = source_data["source_info"]
    limit = prefix.get("limit", 1000)
    auth = source_data["auth"]
    source_type = prefix.get("source_type")
    mls_board = prefix.get("mls_board")
    batch_creation_date = source_data["batch_creation_date"]
    last_modified_date = source_data["last_modification_date"]
    source_data["status"] = False

    try:
        response = login(auth)
        # ---Run this portion to download data in Classes
        data_for_download = download_preparation(cursor_serverless, source_id)

        download_status = request_and_load_tables(
            source_data,
            response,
            auth,
            cursor_serverless,
            serverless_db_con,
            prefix,
            data_for_download,
        )

        success_responce = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "batch_creation_date": batch_creation_date,
            "batch_id": batch_id,
            "flow_type": flow,
            "limit": limit,
            "source_type": source_type,
            "last_refresh_date": last_modified_date,
            "status": download_status,
            "temp_table_status": source_data["temp_table_status"],
            "bl_flag": source_data["batch_execution_params"]["bl_flag"],
            "run_host": run_host,
            "success": False,
        }

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "Data Download Successfully",
        }
        logger.info(log_msg)
        return success_responce

    except (
        psycopg2.Error,
        requests.RequestException,
        ET.ParseError,
        ValueError,
        KeyError,
        AttributeError,
    ) as e:
        # Logging an error message
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }

        # Returning an error response
        source_data.update(log_msg)
        logger.error(source_data)

        return source_data


def lambda_handler(event, context):
    """Main Lambda Handler Function"""
    logger.info(event)

    listing_database = os.environ.get("listingDatabase")
    rds_database = os.environ.get("rdsDatabase")
    sql_exec_limit = context.get_remaining_time_in_millis()
    db_secret_rds = fetch_secrets(rds_database)
    db_secret_listing = fetch_secrets(listing_database)
    serverless_db_con = db_conn(db_secret_rds, sql_exec_limit)
    pentaho_db_con = db_conn(db_secret_listing, sql_exec_limit)
    cursor_serverless = serverless_db_con.cursor()  # type: ignore
    cursor_pentaho = pentaho_db_con.cursor()  # type: ignore

    response = None
    try:

        download_flag = event.get("download_flag")

        # LAST MODIFIED DATE FOR SOURCE_ID EXECUTION TO DOWNLOAD DATA FROM THAT DATE
        if download_flag:

            response = data_download_func(
                cursor_serverless,
                serverless_db_con,
                event,
            )

        else:
            response = validation_func(
                serverless_db_con,
                cursor_serverless,
                cursor_pentaho,
                event,
            )

    except (
        psycopg2.Error,
        requests.RequestException,
        ET.ParseError,
        ValueError,
        KeyError,
    ) as e:
        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(log_msg)

        logger.error(event)
        response = event

    finally:
        cursor_pentaho.close()
        cursor_serverless.close()
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()

    return response
