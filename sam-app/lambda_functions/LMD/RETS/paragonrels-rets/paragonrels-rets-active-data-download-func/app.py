"""Paragonrels Rets Active Data Download Lambda"""

import json
import re
import os
import io
import math
import logging
import traceback
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import boto3  # type: ignore
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2 import extras
import certifi
import ssl
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

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
    except ConnectionError as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


class HostHeaderSSLAdapter(HTTPAdapter):
    def __init__(self, dest_host, **kwargs):
        self.dest_host = dest_host
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        kwargs["ssl_context"] = context
        kwargs["server_hostname"] = self.dest_host  # Force SNI to match
        self.poolmanager = PoolManager(*args, **kwargs)


def login_rets(loginUrl, username, password, USER_AGENT):
    session = requests.Session()

    parsed_url = urlparse(loginUrl)
    cert_hostname = "rets.paragonrels.com"

    adapter = HostHeaderSSLAdapter(dest_host=cert_hostname)
    session.mount("https://", adapter)

    session.auth = (username, password)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "RETS-Version": "RETS/1.7.2",
            "Accept": "*/*",
            "Host": parsed_url.netloc,
        }
    )

    try:
        response = session.get(loginUrl)

        response.raise_for_status()
        return response, session

    except requests.exceptions.SSLError as e:
        logger.error(f"SSL verification failed: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None


def login(data):
    loginUrl = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    source_id = data["source_id"]
    source_name = data["name"]
    USER_AGENT = "Python/3.8 RETS Client/1.0"

    # Create a session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    headers = {"rets-version": "RETS/1.8"}
    session.headers.update(headers)
    session.auth = auth

    response = None
    # Send login request
    if source_id in [798, 604]:
        response, session = login_rets(loginUrl, username, password, USER_AGENT)
    else:
        response = session.get(loginUrl)

    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))

        logger.info("Login successful!")
        rets_data["source_id"] = source_id
        rets_data["name"] = source_name
        rets_data["session"] = session
        return rets_data

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.get("ReplyText")
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "response_text": reply_text,
            "response_status_code": response.status_code,
            "Error": e,
        }
        raise Exception(log_msg)


def logout(loginUrl, session):

    logoutUrl = loginUrl.replace("/login", "/logout")  # type: ignore
    response = session.get(logoutUrl)
    response_text = response.text

    if "success" in response_text.lower():
        logger.info("Logout successful!")


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


def get_object(data, chunks_list):
    session = data["session"]
    media_list = []

    for sub_chunk in chunks_list:
        for media_key in sub_chunk:
            query = f"{media_key}:*"
            query_params = {"Resource": "Property", "Type": "Photo", "ID": query}

            search_url = data["Login"]
            search_url = search_url.split("/rets")[0] + data["GetObject"]
            query_params["Location"] = 1
            response = session.get(search_url, params=query_params)
            response_text = response.text

            try:
                # Regular expressions to capture each record's block, excluding those with 'RETS-Error'
                record_pattern = re.compile(r"(Content-ID: \d+.*?)(?=--|\Z)", re.DOTALL)
                content_id_pattern = re.compile(r"Content-ID: (\d+)")
                object_id_pattern = re.compile(r"Object-ID: (\d+)")
                location_pattern = re.compile(r"Location: (.+)")

                # Extract records
                records = record_pattern.findall(response_text)

                for record in records:
                    # Skip record if it contains 'RETS-Error'
                    if "RETS-Error" in record:
                        continue

                    # Extract Content-ID, Object-ID, and Location
                    content_id = content_id_pattern.search(record).group(1)  # type: ignore
                    object_id = object_id_pattern.search(record).group(1)  # type: ignore
                    location = location_pattern.search(record).group(1).strip()  # type: ignore

                    media_list.append(
                        {
                            "Content_ID": content_id,
                            "Object_ID": object_id,
                            "Url": location,
                        }
                    )

            except Exception as e:
                log_msg = {
                    "response_text": response_text,
                    "query_params": query_params,
                    "response_status_code": response.status_code,
                    "Error": e,
                }
                raise Exception(log_msg)

    return pd.DataFrame(media_list)


def data_download(data):
    session = data["session"]
    query_params = data["query_params"]
    search_url = data["Login"]
    if "paragonrels" in search_url:
        search_url = search_url.split("/rets")[0] + data["Search"]
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        # Extract column names
        count_element = root.find(".//COUNT")
        data_count = int(count_element.get("Records"))  # type: ignore
        # Extract original columns
        original_columns = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore

        # Keep only first occurrence of duplicate columns
        seen = set()
        keep_indexes = []
        columns = []

        for idx, col in enumerate(original_columns):
            col = col.strip()

            if col not in seen:
                seen.add(col)
                keep_indexes.append(idx)
                columns.append(col)

        # Extract rows
        data_rows = []

        for data_element in root.findall("./DATA"):
            original_row = data_element.text.split("\t")[1:-1]  # type: ignore

            # Remove duplicate column values using same indexes
            row = [
                original_row[i] if i < len(original_row) else None
                for i in keep_indexes
            ]

            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)

        # safety cleanup
        df_temp = df_temp.loc[:, ~df_temp.columns.duplicated()]


        return df_temp, data_count

    except Exception as e:
        try:
            root = ET.fromstring(response_text)
            reply_text = root.attrib.get("ReplyText")
            if "No records found" in reply_text:  # type: ignore
                logger.warning(f"{reply_text} Warning {e}")
                return pd.DataFrame(), 0

            log_msg = {
                "response_text": response_text,
                "query_params": query_params,
                "response_status_code": response.status_code,
                "Error": e,
            }
            raise Exception(log_msg)
        except Exception as e:
            log_msg = {
                "response_text": response_text,
                "query_params": query_params,
                "response_status_code": response.status_code,
                "Error": e,
            }
            raise Exception(log_msg)


def get_count(data):
    session = data["session"]
    query_params = data["query_params"]
    search_url = data["Login"]
    if "paragonrels" in search_url:
        search_url = search_url.split("/rets")[0] + data["Search"]
        # query_params['rets-version']= str(data['Login']).split('rets-version=')[1]

    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "2"

    count_value = None
    response = session.get(search_url, params=query_params)
    response_text = response.text
    # logger.error(f"{response_text}")
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

    if flow_type == "sold":
        sold_column = data_source_info["sold_column"]
        sold_status = data_source_info["sold_status"]
        sold_date = source_data["sold_date"]
        return f"({sold_column}={sold_date}+),(L_UpdateDate={last_modified_date}+),({status_column}=|{sold_status})"

    elif flow_type == "full_load" or flow_type == "respecs":
        active_status = data_source_info["active_status"]
        return (
            f"(L_UpdateDate={last_modified_date}+),({status_column}=|{active_status})"
        )

    else:
        return f"(L_UpdateDate={last_modified_date}+)"


def download_preparation(cursor_serverless, source_id, last_modified_date):
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
                STRING_AGG(fmd.system_name, ',') column_normalized,
                STRING_AGG(fmd.long_name, '|') long_name_normalized
            FROM 
                dev.field_metadata as fmd where fmd.download_flag=true and fmd.active_flag=true
            GROUP BY 
                fmd.resource_name,
                fmd.class_name,
                fmd.source_id
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

    df = df.drop(
        ["search_url", "query_params"], axis=1, errors="ignore"
    )  # drop archival columns

    df = df.drop_duplicates()
    df.fillna(pd.NaT)
    df.fillna("")
    df_instance_filtered = df.apply(lambda col: col.map(clean_value))

    column_names = f"""SELECT column_name FROM information_schema.columns
    WHERE table_name ~* '{table_name}' and column_name not in ('id')"""
    cursor_serverless.execute(column_names)
    table_column_names = [column[0] for column in cursor_serverless.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    # df_instance_filtered = df_instance_filtered.reindex(columns=table_column_names)

    # Remove duplicate columns after normalization
    df_instance_filtered = df_instance_filtered.loc[
        :, ~df_instance_filtered.columns.duplicated()
    ]
    df_cols = list(df_instance_filtered.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols
    # Remove duplicate columns from final list
    table_column_names = list(dict.fromkeys(table_column_names))

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

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "table_name": table_name,
        "download_count": len(df),
    }
    logger.info(log_msg)


def adding_extra_columns(
    generic_df, batch_creation_date, formatted_datetime, source_id, class_name, batch_id
):
    """Adding extra columns to the DataFrame Which are required for the data processing"""

    meta_df = pd.DataFrame(
        {
            "source_id": [int(source_id)],
            "batch_id": [int(batch_id)],
            "class_name": [class_name],
            "source_last_update_date": [formatted_datetime],
            "y_creation_date": [batch_creation_date],
            "y_last_update_date": [batch_creation_date],
            "source_creation_date": [formatted_datetime],
        }
    )
    # Repeat metadata row to match the number of rows in generic_df
    meta_df = pd.concat([meta_df] * len(generic_df), ignore_index=True)
    # Reset index of generic_df to align
    generic_df = generic_df.reset_index(drop=True)
    # Combine metadata and original data
    generic_df = pd.concat([meta_df, generic_df], axis=1)
    return generic_df


def download_class_data_for_given_date(
    response,
    source_id,
    source_name,
    source_type,
    resource_name,
    class_name,
    query,
    batch_id_path,
    cursor_serverless,
    serverless_db_con,
    select_fields=None,
):

    search_url = response["Login"]
    query_params = {
        "SearchType": resource_name,
        "Class": class_name,
        "Query": query,
    }

    response["query_params"] = query_params
    count = get_count(response)

    data_df = pd.DataFrame(
        [
            {
                "source_id": source_id,
                "source_name": source_name,
                "total_count": count,
                "search_url": str(search_url),
                "query_params": str(query_params),
            }
        ]
    )

    Upload_data_into_S3_DataLake(
        data_df,
        source_id,
        source_type,
        source_name,
        batch_id_path,
        resource_name,
        "Request",
    )  # lmd_temp as batch_id; just for folder path

    if count == 0:
        return pd.DataFrame()

    top = 1000
    skip = 0
    rounds = math.ceil(count / top)

    class_df = pd.DataFrame()

    while rounds > 0:

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": query,
            "Limit": top,
            "Offset": skip,
        }

        if select_fields is not None:
            query_params["Select"] = select_fields

        response["query_params"] = query_params

        data_df, count = data_download(response)

        if "temp" in str(batch_id_path):
            data_df.rename(
                columns={
                    "L_ListingID": "listingkey",
                    "L_UpdateDate": "modification_timestamp",
                    "L_Last_Photo_updt": "media_modification_timestamp",
                },
                inplace=True,
            )
        else:
            data_df = columns_renaming(
                data_df,
                source_id,
                resource_name,
                class_name,
                cursor_serverless,
                serverless_db_con,
            )

        data_df["search_url"] = str(search_url)
        data_df["query_params"] = str(query_params)

        class_df = pd.concat([class_df, data_df], ignore_index=True)

        skip += top
        rounds -= 1

    return class_df


def download_data_for_given_list(
    response,
    column_name,
    chunks_list,
    resource_name,
    class_name,
):

    data_df = pd.DataFrame()
    search_url = response["Login"]

    for sub_chunk in chunks_list:

        sub_chunk = (
            str(sub_chunk)
            .replace("[", "")
            .replace("]", "")
            .replace("'", "")
            .replace(" ", "")
        )
        query = f"({column_name}={sub_chunk})"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": query,
        }
        response["query_params"] = query_params

        df, count = data_download(response)

        if count and count == 0:
            continue
        else:

            df["search_url"] = str(search_url)
            df["query_params"] = str(query_params)

            data_df = pd.concat([data_df, df], ignore_index=True)

    return data_df


def chunks_creation(df, key_column_names, chunk_size):
    value_keys = []

    for column in key_column_names:
        if column in df.columns:
            value_keys.extend(df[column].dropna().to_list())

        filtered_keys = [key for key in value_keys if key != ""]

        unique_value_keys = (
            pd.Series(filtered_keys, dtype=object).unique().tolist()
        )  # Remove duplicates and preserve order

        key_chunks = [
            unique_value_keys[i : i + chunk_size]
            for i in range(0, len(unique_value_keys), chunk_size)
        ]  # Split into chunks of chunk_size

    return key_chunks  # type: ignore


def Upload_data_into_S3_DataLake(
    df_upload,
    source_id,
    source_type,
    source_name,
    batch_id,
    resource_name,
    skip=None,
):
    # Construct filename and folder path
    filename = f"{source_name}_{resource_name}.parquet"

    if skip is not None:  # requesting source for temp_table
        filename = f"{source_name}_{resource_name}_{skip}.parquet"

    folder_path = f"{source_type}/{source_id}_{source_name}/{resource_name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload.columns = df_upload.columns.map(lambda x: x.replace(".", "_"))
    duplicate_columns = df_upload.columns[df_upload.columns.duplicated()].tolist()

    if duplicate_columns:
        logger.warning(
            f"Removing duplicate columns before parquet upload: {duplicate_columns}"
        )

    df_upload = df_upload.loc[:, ~df_upload.columns.duplicated()]
    # Step 3: Clean values — convert to str, then handle null-like strings
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    # Convert DataFrame to Parquet in memory
    buffer = io.BytesIO()
    # Remove duplicate columns before parquet upload

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
    get_classes,
    flow_type,
    cursor_serverless,
    serverless_db_con,
):
    try:
        """download_temp_table"""
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_type = source_data["source_info"]["source_type"]

        hitting_query = get_hitting_query(source_data, flow_type)
        temp_data = pd.DataFrame()

        lmd_date = source_data["last_modification_date"]
        lmd_date = str(lmd_date).split(".", 1)[0].replace(":", "").replace("-", "")

        for class_name in get_classes:

            resource_name = "Property"
            table_name = "temp_table"

            select_columns = f"L_ListingID,L_UpdateDate,L_Last_Photo_updt"
            if flow_type == "sold":
                sold_column = source_data["source_info"]["sold_column"]
                select_columns = select_columns + f",{sold_column}"


            class_df = download_class_data_for_given_date(
                response,
                source_id,
                source_name,
                source_type,
                resource_name,
                class_name,
                hitting_query,
                f"{lmd_date}_temp",
                cursor_serverless,
                serverless_db_con,
                select_columns,
            )

            temp_data = pd.concat([temp_data, class_df], ignore_index=True)

        if flow_type == "respecs":
            temp_data.insert(0, "respecs_flag", True)
        elif flow_type == "sold":
            temp_data.rename(
                columns={f"{sold_column}": "sold_date"},
                inplace=True,
            )

        temp_data.insert(0, "source_id", int(source_id))

        if len(temp_data) != 0:

            Upload_data_into_S3_DataLake(
                temp_data,
                source_id,
                source_type,
                source_name,
                f"{lmd_date}_temp",
                resource_name,
            )

            table_creation_and_loading(
                temp_data,
                table_name,
                source_id,
                source_name,
                resource_name,
                cursor_serverless,
                serverless_db_con,
            )

            return True

        return False
    except:
        raise


def columns_renaming(
    data_df, source_id, resource_name, class_name, cursor_serverless, serverless_db_con
):
    renamed_df = data_df.copy()

    renaming_cols = """select distinct lower(long_name), system_name from dev.field_metadata where source_id = {0} and resource_name = '{1}' and class_name = '{2}' and download_flag is true;""".format(
        source_id, resource_name, class_name
    )
    cursor_serverless.execute(renaming_cols)
    renamed_columns = cursor_serverless.fetchall()

    if renamed_columns[0] is None:
        return renamed_df
    else:
        for elem in renamed_columns:
            long_name = elem[0]
            system_name = elem[1]

            renamed_df.rename(columns={system_name: long_name}, inplace=True)

        return renamed_df


def request_and_load_tables(
    source_data,
    response,
    cursor_serverless,
    serverless_db_con,
    data_for_download,
):
    """request_and_load_tables"""

    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_name = source_data["source_name"]
    limit = source_data["source_info"].get("limit", 1000)
    source_type = source_data["source_info"]["source_type"]
    flow_type = source_data["flow_type"]
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

    chunk_size = 200
    listing_chunk_size = 200
    media_chunk_size = 50
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

    agent_ids_chunks = []
    office_ids_chunks = []
    get_object_flag = False

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    for _, row in data_for_download.iterrows():

        data_df = pd.DataFrame()
        resource_name = row["resource_name"]
        class_name = row["class_name"]
        chunks_list = []
        key_column = ""

        if resource_name == "OpenHouse":
            # Get current timestamp and subtract one day
            openhouse_column = source_data["source_info"]["openhouse_column"]
            current_timestamp = datetime.now(timezone.utc)
            one_day_ago = current_timestamp - pd.Timedelta(days=1)
            one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            hitting_query = f"({openhouse_column}={one_day_ago_formatted}+)"

            data_df = download_class_data_for_given_date(
                response,
                source_id,
                source_name,
                source_type,
                resource_name,
                class_name,
                hitting_query,
                batch_id,
                cursor_serverless,
                serverless_db_con,
            )

        else:
            if resource_name == "Property":
                chunks_list = listings
                key_column = "L_ListingID"

            elif resource_name == "ActiveAgent" or resource_name == "Agent":
                chunks_list = agent_ids_chunks
                key_column = "U_AgentID"

            elif resource_name == "ActiveOffice" or resource_name == "Office":
                chunks_list = office_ids_chunks
                key_column = "O_OfficeID"

            elif resource_name == "Media":
                chunks_list = listings_key_for_media
                key_column = "L_ListingID"

            if resource_name == "Media" and source_id != 372:

                if get_object_flag is False:
                    data_df = get_object(response, chunks_list)
                    get_object_flag = True

            else:
                data_df = download_data_for_given_list(
                    response,
                    key_column,
                    chunks_list,
                    resource_name,
                    class_name,
                )

        if len(data_df) == 0:  # No data for resource/class
            continue
        else:  # move towards insertion in prestage tables

            if resource_name == "Property":
                # extract agent/office rank wise IDs from property
                columns_list = [
                    "L_ListAgent1",
                    "L_ListAgent2",
                    "L_SellingAgent1",
                    "L_SellingAgent2",
                    "Listing_Agent_ID",
                ]
                agent_ids_chunks.extend(
                    chunks_creation(data_df, columns_list, chunk_size)
                )
                columns_list = [
                    "L_ListOffice1",
                    "L_ListOffice2",
                    "L_SellingOffice1",
                    "L_SellingOffice2",
                ]
                office_ids_chunks.extend(
                    chunks_creation(data_df, columns_list, chunk_size)
                )

            data_df = adding_extra_columns(
                data_df,
                batch_creation_date,
                formatted_datetime,
                source_id,
                class_name,
                batch_id,
            )

            data_df = columns_renaming(
                data_df,
                source_id,
                resource_name,
                class_name,
                cursor_serverless,
                serverless_db_con,
            )

            Upload_data_into_S3_DataLake(
                data_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                resource_name,
            )

            table_creation_and_loading(
                data_df,
                f"ps_rets_{resource_name}_{source_id}",
                source_id,
                source_name,
                resource_name,
                cursor_serverless,
                serverless_db_con,
            )

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
            auth["source_id"] = source_id
            auth["name"] = source_name
            response = login(auth)

            status = download_temp_table(
                source_data,
                response,
                get_classes,
                flow_type,
                cursor_serverless,
                serverless_db_con,
            )

            if response["session"]:
                logout(auth["loginUrl"], response["session"])

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


def data_download_func(
    cursor_serverless,
    serverless_db_con,
    source_data,
):
    """Data Download Function"""
    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    run_host = source_data["run_host"]
    source_name = source_data["source_name"]
    flow = source_data["flow_type"]
    source_info = source_data["source_info"]
    limit = source_info.get("limit", 1000)
    auth = source_data["auth"]
    source_type = source_info.get("source_type")
    mls_board = source_info.get("mls_board")
    batch_creation_date = source_data["batch_creation_date"]
    last_modified_date = source_data["last_modification_date"]
    source_data["status"] = False

    try:
        auth["source_id"] = source_id
        auth["name"] = source_name
        response = login(auth)
        # ---Run this portion to download data in Temp Table

        # ---Run this portion to download data in Classes
        data_for_download = download_preparation(
            cursor_serverless, source_id, last_modified_date
        )

        download_status = request_and_load_tables(
            source_data,
            response,
            cursor_serverless,
            serverless_db_con,
            data_for_download,
        )

        if response["session"]:
            logout(auth["loginUrl"], response["session"])

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
        AttributeError,
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
