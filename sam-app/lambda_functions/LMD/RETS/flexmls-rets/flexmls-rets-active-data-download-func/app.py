"""Flexmls Rets Active Data Download Lambda"""

import json
import re
import os
import io
import logging
import traceback
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import boto3  # type: ignore
import pandas as pd
import requests
from requests.auth import HTTPDigestAuth
import psycopg2
from psycopg2 import extras

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


def login(data):
    """Rets Server Login Function"""
    login_url = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    headers = {"RETS-Version": "RETS/1.7.2"}

    # Create a session
    session = requests.Session()
    session.headers.update(headers)
    session.auth = HTTPDigestAuth(username, password)
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
            rets_data["Search"] = "http://retsgw.flexmls.com" + rets_data["Search"]

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
            f"Login failed with status code {log_msg['Response Status Code']}"
        )


def data_download(data):
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

        df_temp.insert(0, "query_params", str(query_params))
        df_temp.insert(0, "search_url", str(search_url))

        return df_temp, data_count

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if "No Records Found" in reply_text:  # type: ignore
            return pd.DataFrame(), 0

        log_msg = {
            "response_status_code": response.status_code,
            "response_text": response_text,
            "query_params": query_params,
        }
        raise Exception(log_msg)


def get_count(data):
    """Get Count of Records from Rets Server Function"""
    session = data["session"]
    query_params = data["query_params"]
    search_url = data["Search"]

    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "2"

    count_value = None
    response = session.get(search_url, params=query_params)
    try:
        response_text = response.text
        root = ET.fromstring(response_text)
        count_element = root.find(".//COUNT")
        count_value = int(count_element.get("Records"))  # type: ignore
        return count_value

    except (ET.ParseError, AttributeError, Exception) as e:
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if "No records found" in reply_text:  # type: ignore
            logger.warning("%s", reply_text)
            return 0
        log_msg = {
            "Message": "Count not fetched",
            "response_text": response_text,
            "params": query_params,
            "response_status_code": response.status_code,
            "Error At line": traceback.format_exc(),
            "Error": str(e),
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
            original_datetime_aware = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            original_datetime_aware = original_datetime_aware.replace(
                tzinfo=timezone.utc
            )
            return original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            # If it doesn't match the expected format, return the default date
            return formatted_datetime_utc

    # If the input `date` is a tuple or any other format, handle it
    if isinstance(date, tuple):
        return formatted_date(date[0])  # Recursively call if the date is in a tuple

    # If it's a datetime object, convert it to the correct format
    if isinstance(date, datetime):
        date = date.replace(tzinfo=timezone.utc)  # Ensure it's in UTC timezone
        return date.strftime("%Y-%m-%dT%H:%M:%S")

    # If none of the above, return the default formatted date
    return formatted_datetime_utc


def get_max_last_modified_date(
    source_id,
    cursor_rds,
    flow_type,
    rolling_window_offset=None,
):
    """Get the maximum last modified date for a given source_id and flow_type ."""

    if flow_type == "sold":
        query = f"""SELECT sold_date, last_modified_date FROM stage.serverless_idx_loads WHERE source_id = {source_id};"""
        cursor_rds.execute(query)
        data = cursor_rds.fetchone()

        sold_date = data[0]
        last_modified_date = data[1]

        return formatted_date(last_modified_date), str(sold_date)

    query_for_last_modified_date_serverless = None

    if flow_type == "backlog":
        query_for_last_modified_date_serverless = f""" SELECT bl_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id} """

    elif flow_type == "respecs":
        query_for_last_modified_date_serverless = f""" SELECT respecs_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "rolling_window":
        query_for_last_modified_date_serverless = f""" SELECT last_modified_date::timestamp(0) - interval '{rolling_window_offset} hours' FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "full_load":
        query_for_last_modified_date_serverless = f""" SELECT full_load_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    else:
        query_for_last_modified_date_serverless = f""" SELECT last_modified_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id} """

    cursor_rds.execute(query_for_last_modified_date_serverless)
    max_date_serverless = cursor_rds.fetchone()[0]

    last_modified_date = formatted_date(max_date_serverless)  # type: ignore

    return last_modified_date


def get_hitting_query(source_data, flow_type):
    """Construct the hitting query for the data source."""
    source_info = source_data["source_info"]
    status_column = source_info["status_column"]
    photo_timestamp_column = source_info["photo_timestamp_column"]
    last_modified_date = source_data["last_modification_date"]
    modification_column = source_info["modification_column"]

    if flow_type == "sold":
        sold_column = source_info["sold_column"]
        sold_status = source_info["sold_status"]
        sold_date = source_data["sold_date"]

        return f"({sold_column}={sold_date}+),({status_column}=|{sold_status})"

    elif flow_type == "full_load" or flow_type == "respecs":
        active_status = source_info["active_status"]
        return f"({modification_column}={last_modified_date}+),({status_column}=|{active_status})"

    else:
        return f"(({modification_column}={last_modified_date}+))"


def download_preparation(cursor_rds, source_id):
    """creating DataFrame for Data Download"""

    download_query = f"""SELECT
    	cmd.source_id,
        cmd.resource_name,
        cmd.class_name
        FROM
        dev.class_metadata cmd
        where
            cmd.source_id ='{source_id}'
            and cmd.active_flag=true
            and cmd.download_flag=true order by cmd.id;
        """

    cursor_rds.execute(download_query)
    source_data = cursor_rds.fetchall()
    columns = [desc[0] for desc in cursor_rds.description]
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


def table_creation_and_loading(
    data_df,
    table_name,
    source_id,
    source_name,
    resource_name,
    cursor_rds,
    rds_db_connection,
):
    """Loading DataFrame to the table in serverless database"""

    data_df = data_df.drop(["search_url", "query_params"], axis=1)
    data_df = data_df.drop_duplicates()

    data_df.fillna(pd.NaT)
    data_df.fillna("")
    df_instance_filtered = data_df.apply(lambda col: col.map(clean_value))

    column_names = f"""SELECT column_name FROM information_schema.columns
    WHERE table_name ~* '{table_name}' and column_name not in ('id')"""
    cursor_rds.execute(column_names)
    table_column_names = [column[0] for column in cursor_rds.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    df_cols = list(df_instance_filtered.columns)

    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = f"""ALTER TABLE idx_stage.{table_name}
            ADD COLUMN  IF NOT EXISTS {n} TEXT"""
            cursor_rds.execute(alter_query)
            insert_query = f""" INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name, system_name)
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}', '{n}');
                """
            cursor_rds.execute(insert_query)
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": resource_name,
                "Alter Query": alter_query,
                "Insert Query": insert_query,
                "Message": f"Added new column {n} to {table_name}",
            }
            logger.info(log_msg)
            rds_db_connection.commit()

    cols = ",".join(list(df_instance_filtered.columns))
    cols = cols.replace(",order,", ',"order",').replace(",table,", ',"table",')
    data_values = [tuple(row) for row in df_instance_filtered.values]
    insert_query = f"""
    INSERT INTO idx_stage.{table_name} ({cols}) VALUES %s
    """
    extras.execute_values(cursor_rds, insert_query, data_values)
    rds_db_connection.commit()


def adding_extra_columns(
    generic_df, batch_creation_date, source_id, class_name, batch_id, formatted_datetime
):
    """Adding extra columns to the DataFrame Which are required for the data processing"""

    generic_df.insert(0, "source_creation_date", formatted_datetime)
    generic_df.insert(0, "y_last_update_date", batch_creation_date)
    generic_df.insert(0, "y_creation_date", batch_creation_date)
    generic_df.insert(0, "source_last_update_date", formatted_datetime)
    generic_df.insert(0, "batch_id", int(batch_id))
    generic_df.insert(0, "class_name", class_name)
    generic_df.insert(0, "source_id", int(source_id))
    return generic_df


def download_data_for_given_list(
    chunks_list,
    resource_name,
    class_name,
    response,
    column_name,
):

    data_df = pd.DataFrame()

    for sub_chunk in chunks_list:

        processed_values = [str(value) for value in sub_chunk]
        sub_chunk = ",".join(processed_values)

        query = f"({column_name}={sub_chunk})"
        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": query,
        }
        response["query_params"] = query_params

        # Data Downloading
        df, count = data_download(response)

        if count and count == 0:
            continue
        else:
            data_df = pd.concat([data_df, df], ignore_index=True)

    return data_df


def columns_renaming(final_df, source_id, resource_name, class_name, cursor_rds):
    """Renaming columns in the DataFrame based on metadata from the database."""
    renaming_cols = f"""select distinct lower(long_name), system_name from dev.field_metadata
    where source_id = {source_id} and resource_name = '{resource_name}' and class_name = '{class_name}' ;"""
    cursor_rds.execute(renaming_cols)
    renamed_columns = cursor_rds.fetchall()

    if renamed_columns[0] is None:
        return final_df
    else:
        for elem in renamed_columns:
            long_name = elem[0]
            system_name = elem[1]

            final_df.rename(columns={system_name: long_name}, inplace=True)

        return final_df


def download_temp_table(
    source_data,
    response,
    get_classes,
    flow_type,
    cursor_rds,
    rds_db_connection,
):
    """download_temp_table"""
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    hitting_query = get_hitting_query(source_data, flow_type)
    modification_column = source_data["source_info"]["modification_column"]
    key_column = source_data["source_info"]["key_column"]
    photo_timestamp_column = source_data["source_info"]["photo_timestamp_column"]
    source_type = source_data["source_info"]["source_type"]

    resource_name = "Property"
    table_name = "temp_table"
    search_url = response["Search"]

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

        select = f"{key_column},{modification_column},{photo_timestamp_column}"
        if flow_type == "sold":
            sold_column = source_data["source_info"]["sold_column"]
            select = select + f",{sold_column}"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": hitting_query,
            "Select": select,
        }

        if flow_type == "sold":
            sold_column = source_data["source_info"]["sold_column"]
            query_params["Select"] = query_params["Select"] + f",{sold_column}"

        response["query_params"] = query_params
        count = get_count(response)

        df = pd.DataFrame(
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

        Upload_data_into_S3_DataLake(df, archival_params, "Request")

        if count == 0:
            continue

        top = 1000
        skip = 0

        while skip <= count:
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
            df, _ = data_download(response)
            df.rename(
                columns={
                    f"{key_column}": "listingkey",
                    f"{modification_column}": "modification_timestamp",
                    f"{photo_timestamp_column}": "media_modification_timestamp",
                },
                inplace=True,
            )

            temp_data = pd.concat([temp_data, df], ignore_index=True)

            skip = skip + top

    if len(temp_data) != 0:

        Upload_data_into_S3_DataLake(temp_data, archival_params)

        if flow_type == "respecs":
            temp_data.insert(0, "respecs_flag", True)
        elif flow_type == "sold":
            temp_data.rename(
                columns={f"{sold_column}": "sold_date"},
                inplace=True,
            )
        temp_data.insert(0, "source_id", int(source_id))

        table_creation_and_loading(
            temp_data,
            table_name,
            source_id,
            source_name,
            resource_name,
            cursor_rds,
            rds_db_connection,
        )

    return True


def chunks_creation(df, key_column_names, chunk_size):
    value_keys = []

    for column in key_column_names:
        if column in df.columns:
            df_ = df[column].apply(clean_value)
            df_.drop_duplicates(inplace=True)
            value_keys.extend(df_.dropna().to_list())

    # Remove empty strings and duplicates
    filtered_keys = [key for key in value_keys if key not in [None, "", " "]]
    unique_value_keys = pd.Series(filtered_keys, dtype=object).unique().tolist()

    key_chunks = [
        unique_value_keys[i : i + chunk_size]
        for i in range(0, len(unique_value_keys), chunk_size)
    ]

    return key_chunks


def request_and_load_tables(
    source_data,
    response,
    data_for_download,
    cursor_rds,
    rds_db_connection,
):

    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_name = source_data["source_name"]
    limit = source_data["source_info"]["limit"]
    flow_type = source_data["flow_type"]
    bl_flag = source_data["batch_execution_params"]["bl_flag"]
    source_type = source_data["source_info"]["source_type"]

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
        listingkey as listingkey
        from idx_stage.temp_table
        where source_id ={source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}'
        order by {orderby_column}::timestamp {orderby_type}, listingkey
        Limit {limit};
        """

    cursor_rds.execute(query)
    temp_table_data = cursor_rds.fetchall()
    listing_key = [val[0] for val in temp_table_data]

    chunk_size = 100
    member_keys_chunks = []
    office_keys_chunks = []
    listings = []

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    listings.extend(
        [
            listing_key[i : i + chunk_size]
            for i in range(0, len(listing_key), chunk_size)
        ]
    )

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
        data_df = pd.DataFrame()

        if resource_name == "Property":
            if listings:
                data_df = download_data_for_given_list(
                    listings,
                    resource_name,
                    class_name,
                    response,
                    "LIST_1",
                )

            if not data_df.empty:
                member_keys_chunks.extend(
                    chunks_creation(
                        data_df,
                        [
                            "LIST_63",
                            "LIST_62",
                            "LIST_6",
                            "LIST_5",
                        ],
                        chunk_size,
                    )
                )
                office_keys_chunks.extend(
                    chunks_creation(
                        data_df,
                        [
                            "LIST_106",
                            "LIST_165",
                            "LIST_166",
                            "LIST_61",
                        ],
                        chunk_size,
                    )
                )

        elif resource_name == "ActiveAgent":
            if member_keys_chunks:
                data_df = download_data_for_given_list(
                    member_keys_chunks,
                    resource_name,
                    class_name,
                    response,
                    "MEMBER_0",
                )

        elif resource_name == "Office":
            if office_keys_chunks:
                data_df = download_data_for_given_list(
                    office_keys_chunks,
                    resource_name,
                    class_name,
                    response,
                    "OFFICE_0",
                )

        elif resource_name == "Room":
            if listings:
                data_df = download_data_for_given_list(
                    listings,
                    resource_name,
                    class_name,
                    response,
                    "ROOM_1",
                )

        elif resource_name == "OpenHouse":
            last_modified_date = source_data["last_modification_date"]
            query_params = {
                "SearchType": resource_name,
                "Class": class_name,
                "Query": f"(EVENT6={last_modified_date}+)",
                "Limit": 10000,
            }
            response["query_params"] = query_params
            data_df, _ = data_download(response)

        elif resource_name == "Media":
            loginUrl = source_data["auth"]["loginUrl"]
            data_df = get_object_url(loginUrl, listings, response["session"])

        else:
            continue

        # Process and load the data
        if not data_df.empty:

            data_df = columns_renaming(
                data_df, source_id, resource_name, class_name, cursor_rds
            )
            data_df = adding_extra_columns(
                data_df,
                batch_creation_date,
                source_id,
                class_name,
                batch_id,
                formatted_datetime,
            )

            archival_params["resource_name"] = resource_name
            archival_params["class_name"] = class_name
            Upload_data_into_S3_DataLake(data_df, archival_params)

            table_creation_and_loading(
                data_df,
                target_table,
                source_id,
                source_name,
                resource_name,
                cursor_rds,
                rds_db_connection,
            )

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "resource_name": resource_name,
                "class_name": class_name,
                "download_count": len(data_df),
                "Message": f"Inserted into {target_table}",
            }
            logger.info(log_msg)

    return True


def get_object_url(loginUrl, listings, session):

    search_url = loginUrl.replace("Login", "GetObject")
    query_params = {"Resource": "Property", "Type": "HiRes", "Location": "1"}

    try:
        media_data = []

        for each_chunk in listings:
            for each_listing in each_chunk:
                each_listing = str(each_listing).replace("'", "")
                query_params["ID"] = f"{each_listing}:*"

                response = session.get(search_url, params=query_params)
                response_text = response.text

                # Split by boundary pattern (starts with --FLEX...)
                blocks = re.split(r"--FLEX[\w]+", response_text)

                # Define the fields you care about
                fields = [
                    "Content-ID",
                    "Content-Type",
                    "Object-ID",
                    "Location",
                    "Content-Description",
                    "Preferred",
                ]

                for block in blocks:
                    if "Content-Type" in block and "Location" in block:
                        entry = {}
                        for field in fields:
                            match = re.search(rf"{field}:\s*(.*)", block)
                            entry[field.replace("-", "_")] = (
                                match.group(1).strip() if match else None
                            )

                        entry.update(
                            {
                                "search_url": search_url,
                                "query_params": query_params.copy(),
                            }
                        )
                        media_data.append(entry)

        return pd.DataFrame(media_data)

    except Exception as e:
        try:
            root = ET.fromstring(response_text)
            reply_text = root.attrib.get("ReplyText")
            logger.error(f"{reply_text} Error {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"{response_text} Error {e}")
            return pd.DataFrame()


def validation_func(rds_db_connection, cursor_rds, source_data):
    """Validation Function"""

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    auth = source_data["auth"]
    runtime_count = source_data["runtime_count"]
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params["bl_flag"]
    itr_value = batch_execution_params["itr_value"]
    respecs_flag = batch_execution_params["respecs_flag"]
    flow_type = source_info["flow_type"]
    rolling_window_batch = source_info["rolling_window_batch"]
    rolling_window_offset = None

    if flow_type not in ["sold", "full_load"]:

        if respecs_flag is True and (runtime_count % itr_value != 0):
            flow_type = "respecs"

        elif bl_flag is True and (runtime_count % itr_value != 0):
            flow_type = "backlog"

        elif runtime_count % rolling_window_batch == 0:
            rolling_window_offset = source_info["rolling_window_offset"]
            flow_type = "rolling_window"

    temp_respecs_flag = "f"
    if flow_type == "respecs":
        temp_respecs_flag = "t"

    source_data["flow_type"] = flow_type
    max_last_modified_date = None

    if flow_type == "sold":
        max_last_modified_date, sold_date = get_max_last_modified_date(
            source_id,
            cursor_rds,
            flow_type,
        )
        source_data["sold_date"] = sold_date

    else:
        max_last_modified_date = get_max_last_modified_date(
            source_id,
            cursor_rds,
            flow_type,
            rolling_window_offset,
        )

    source_data["last_modification_date"] = max_last_modified_date

    query = f""" DELETE FROM idx_stage.temp_table
    where source_id = {source_id} and download_flag = 'f' ; """

    cursor_rds.execute(query)
    rds_db_connection.commit()

    log_message = {
        "source_id": source_id,
        "source_name": source_name,
        "deleted_count": cursor_rds.rowcount,
        "Query": query,
    }
    logger.info(log_message)

    # Common function to get total count for all classes
    def get_total_count():
        query = f""" select count(distinct listingkey) from idx_stage.temp_table 
            where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """

        cursor_rds.execute(query)
        result = cursor_rds.fetchone()[0]
        # total_count = int(result)
        return int(result)

    # Set temp_table_status based on conditions
    total_count = get_total_count()

    source_data["temp_table_status"] = False if total_count > 0 else True

    # always download temp_table for lmd, rolling_window batch during backlog (with last_modified_date)
    if (bl_flag is True and flow_type == "lmd") or flow_type == "rolling_window":
        source_data["temp_table_status"] = True

    if source_data["temp_table_status"]:
        # server session creation
        query = f"""
            select distinct class_name from dev.class_metadata c
            where c.source_id = {source_id} and c.download_flag = true and c.resource_name ='Property'
            order by 1;
        """
        cursor_rds.execute(query)
        get_classes = [row[0] for row in cursor_rds.fetchall()]  # type: ignore

        response = login(auth)
        status = download_temp_table(
            source_data,
            response,
            get_classes,
            flow_type,
            cursor_rds,
            rds_db_connection,
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

    cursor_rds.execute(query)
    latest_listing_date = cursor_rds.fetchone()[0]
    latest_listing_date = (
        max_last_modified_date
        if latest_listing_date is None
        else formatted_date(latest_listing_date)
    )
    source_data["latest_listing_date"] = latest_listing_date
    source_data["download_flag"] = True

    return source_data


def data_download_func(
    cursor_rds,
    rds_db_connection,
    source_data,
):

    data_for_download = download_preparation(cursor_rds, source_data["source_id"])

    response = login(source_data["auth"])

    download_status = request_and_load_tables(
        source_data,
        response,
        data_for_download,
        cursor_rds,
        rds_db_connection,
    )

    success_responce = {
        "source_id": source_data["source_id"],
        "source_name": source_data["source_name"],
        "mls_board": source_data["source_info"].get("mls_board"),
        "batch_creation_date": source_data["batch_creation_date"],
        "batch_id": source_data["batch_id"],
        "flow_type": source_data["flow_type"],
        "limit": source_data["source_info"].get("limit"),
        "source_type": source_data["source_info"].get("source_type"),
        "last_refresh_date": source_data["last_modification_date"],
        "status": download_status,
        "temp_table_status": source_data["temp_table_status"],
        "run_host": source_data["run_host"],
        "success": False,
        "bl_flag": source_data["batch_execution_params"]["bl_flag"],
    }

    return success_responce


def lambda_handler(event, context):
    """Main Lambda Handler Function"""

    rds_database = os.environ.get("rdsDatabase")
    sql_exec_limit = context.get_remaining_time_in_millis()
    db_secret_rds = fetch_secrets(rds_database)
    rds_db_connection = db_conn(db_secret_rds, sql_exec_limit)
    cursor_rds = rds_db_connection.cursor()  # type: ignore

    source_data = event
    try:

        if source_data.get("download_flag"):

            source_data = data_download_func(
                cursor_rds,
                rds_db_connection,
                source_data,
            )

        else:
            source_data = validation_func(
                rds_db_connection,
                cursor_rds,
                source_data,
            )

    except Exception as e:
        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)

    finally:
        if cursor_rds:
            cursor_rds.close()
            rds_db_connection.close()

    return source_data
