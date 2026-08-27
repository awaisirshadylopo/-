"""Unified RETS Active Data Download Lambda"""

import json
import re
import os
import math
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
import io

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
    except:
        raise


def login(data):
    login_url = data["loginUrl"]
    username = data["user"]
    password = data["password"]

    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    headers = data.get("headers", {})
    session.auth = auth
    response = None

    try:
        # Send login request
        response = session.get(login_url, headers=headers)

        if response.status_code == 200:

            login_data = {
                "session": session,
                "Login": login_url,
                "Search": login_url.replace("login", "search"),
                "GetObject": login_url.replace("login", "getObject"),
            }

            return login_data
        else:
            raise Exception(
                {
                    "response_status_code": response.status_code,
                    "response_text": response.text,
                }
            )

    except Exception as e:
        log_msg = {
            "response_status_code": response.status_code,
            "response_text": response.text,
            "Error": e,
            "Error At Line": traceback.format_exc(),
        }
        raise Exception(log_msg)


def get_object_media(session, get_object_url, photo_type_column, listing_chunks):

    query_params = {"Resource": "Property", "Type": photo_type_column, "Location": "1"}

    media_data = []

    for sub_chunk in listing_chunks:
        for listing_id in sub_chunk:
            listing_id = str(listing_id).replace("'", "")
            query_params["ID"] = f"{listing_id}:*"

            response = session.get(get_object_url, params=query_params)
            response_text = response.text

            # Split by boundary pattern (starts with --FLEX...)
            blocks = re.split(r"--[\w]+", response_text)

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
                    media_data.append(entry)

    # Convert to DataFrame
    data_df = pd.DataFrame(media_data)
    return data_df


def data_download(request_data, query_type):
    response = None
    session = request_data["session"]
    query_params = request_data["query_params"]
    search_url = request_data["Search"]

    query_params["QueryType"] = query_type
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"

    response = session.get(search_url, params=query_params)
    response_text = response.text

    if "no records found" in response_text.lower():
        return pd.DataFrame()

    try:
        root = ET.fromstring(response_text)
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore

        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]  # type: ignore
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)

        if (
            query_params.get("Offset") is None
        ):  # when requesting for other classes (not for temp_table); data archival logic
            df_temp["search_url"] = str(search_url)
            df_temp["query_params"] = str(query_params)

        return df_temp

    except:
        raise Exception(
            {
                "response_status_code": response.status_code,
                "response_text": response_text,
            }
        )


def get_class_count(request_data, query_type):
    session = request_data["session"]
    query_params = request_data["query_params"]
    search_url = request_data["Search"]
    query_params["QueryType"] = query_type
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "2"

    count_value = None
    response = session.get(search_url, params=query_params)
    response_text = response.text

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
    if not date:
        date = "1990-01-01 00:00:00"
    date = str(date).split(".")[0]
    original_datetime_aware = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    formatted_time = original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S")
    return formatted_time


def get_lmd(
    source_id,
    rds_cursor,
    homelisting_cursor,
    flow_type,
    rolling_window_offset=None,
):
    """Get the maximum last modified date for a given source_id and flow type."""

    if flow_type == "sold":
        modification_timestamp = "1990-01-01 00:00:00"
        query = f"SELECT sold_date, batch_id from stage.serverless_idx_loads where source_id = {source_id} limit 1"
        rds_cursor.execute(query)
        sold_date, batch_id = rds_cursor.fetchone()  # batch_id of last executed batch

        # if previous batch has same sold_date in all listings; then get max(modification_timestamp) to avoid loop in API request
        query = f"""SELECT CASE WHEN min(sold_date) = max(sold_date) THEN max(modification_timestamp)::timestamp(0) ELSE NULL END AS max_modification_timestamp
        from listing_p_sold where source_id = {source_id} and batch_id = {batch_id} """
        homelisting_cursor.execute(query)
        max_modification_timestamp = homelisting_cursor.fetchone()[0]

        if max_modification_timestamp:
            modification_timestamp = max_modification_timestamp

        return formatted_date(modification_timestamp), str(sold_date)

    else:
        if flow_type == "lmd":
            timestamp_column = "last_modified_date"
        elif flow_type == "rolling_window":
            timestamp_column = (
                f"last_modified_date - interval '{rolling_window_offset} hours'"
            )
        elif flow_type == "full_load":
            timestamp_column = "full_load_date"
        elif flow_type == "backlog":
            timestamp_column = "bl_start_date"
        elif flow_type == "respecs":
            timestamp_column = "respecs_start_date"

        lmd_query = f"SELECT {timestamp_column} FROM stage.serverless_idx_loads WHERE source_id = {source_id} limit 1"

        rds_cursor.execute(lmd_query)
        max_date_serverless = rds_cursor.fetchone()[0]
        last_modified_date = formatted_date(max_date_serverless)  # type: ignore

        return last_modified_date


def get_hitting_query(source_data, flow_type):
    """Construct the hitting query for the data source."""
    source_info = source_data["source_info"]
    status_column = source_info["status_column"]
    last_modified_date = source_data["last_modification_date"]
    listing_modification_timestamp_column = source_info[
        "listing_modification_timestamp_column"
    ]

    if flow_type == "sold":
        sold_column = source_info["sold_column"]
        sold_status = source_info["sold_status"]
        sold_date = source_data["sold_date"]
        return f"({sold_column}={sold_date}+),({listing_modification_timestamp_column}={last_modified_date}+),({status_column}=|{sold_status})"

    elif flow_type == "full_load" or flow_type == "respecs":
        active_status = source_info["active_status"]
        return f"({listing_modification_timestamp_column}={last_modified_date}+),({status_column}=|{active_status})"

    else:
        return f"({listing_modification_timestamp_column}={last_modified_date}+)"


def download_preparation(rds_cursor, source_id):
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

    rds_cursor.execute(download_query)
    source_data = rds_cursor.fetchall()
    columns = [desc[0] for desc in rds_cursor.description]
    classes_and_fields_data = pd.DataFrame(source_data, columns=columns)

    classes_and_fields_data["target_table"] = classes_and_fields_data.apply(
        lambda row: "ps_rets" + "_" + row["resource_name"] + "_" + str(source_id),
        axis=1,
    )

    # Sort by the custom sort key first, and then by 'resource_name'
    classes_and_fields_data["sort_key"] = classes_and_fields_data[
        "resource_name"
    ].apply(lambda x: 0 if x == "Property" else 1)
    classes_and_fields_data = (
        classes_and_fields_data.sort_values(by=["sort_key"])
        .drop(columns="sort_key")
        .reset_index(drop=True)
    )

    return classes_and_fields_data


def clean_value(value):
    """Clean NONE values"""
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
    rds_cursor,
    rds_connection,
):
    """Loading DataFrame to the table in serverless database"""
    df = df.drop_duplicates()
    df.fillna(pd.NaT)
    df.fillna("")
    df_instance_filtered = df.apply(lambda col: col.map(clean_value))
    column_names = f"""SELECT lower(column_name) FROM information_schema.columns
    WHERE table_name = '{table_name}' and column_name not in ('id')"""
    rds_cursor.execute(column_names)
    table_column_names = [column[0] for column in rds_cursor.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    df_cols = list(df_instance_filtered.columns)

    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = f"""ALTER TABLE idx_stage.{table_name}
            ADD COLUMN  IF NOT EXISTS {n} TEXT"""
            rds_cursor.execute(alter_query)
            insert_query = f""" INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name)
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}');
                """
            rds_cursor.execute(insert_query)
            rds_connection.commit()

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": resource_name,
                "Alter Query": alter_query,
                "Insert Query": insert_query,
                "Message": f"Added new column {n} to {table_name}",
            }
            logger.info(log_msg)

    cols = ",".join(list(df_instance_filtered.columns))
    cols = cols.replace(",order,", ',"order",')
    data_values = [tuple(row) for row in df_instance_filtered.values]

    insert_query = f"""
    INSERT INTO idx_stage.{table_name} ({cols}) VALUES %s
    """

    extras.execute_values(rds_cursor, insert_query, data_values)
    rds_connection.commit()

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "table_name": table_name,
        "download_count": len(df),
    }
    logger.info(log_msg)


def adding_extra_columns(
    data_df, source_id, class_name, batch_id, batch_creation_date, formatted_datetime
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
    # Repeat metadata row to match the number of rows in data_df
    meta_df = pd.concat([meta_df] * len(data_df), ignore_index=True)
    # Reset index of data_df to align
    data_df = data_df.reset_index(drop=True)
    # Combine metadata and original data
    data_df = pd.concat([meta_df, data_df], axis=1)
    return data_df


def download_data_for_given_list(
    source_id,
    resource_name,
    class_name,
    request_data,
    query_type,
    key_column_name,
    chunks_list,
):

    data_df = pd.DataFrame()

    for sub_chunk in chunks_list:

        if source_id == 683:
            """example query:
                query = (ListingId=123456-123456)|(ListingId=456789-456789)|(ListingId=789012-789012)
            for all resources and classes of 683
            """

            query = ""
            for key in sub_chunk:
                query = query + f"({key_column_name}={key}-{key})|"
            query = query[:-1]

        elif source_id == 533:
            """example query:
                query = (ListingId="123456","456789","789012")
            for all resources and classes of 533
            """

            sub_chunk = (
                str(sub_chunk)
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "")
                .replace("'", '"')
            )
            query = f"({key_column_name}={sub_chunk})"

        elif source_id == 777:
            """example query:
                query = (ListingId=123456,456789,789012) for Agent and Office of 777
                and
                query = (ListingId=123456-123456)|(ListingId=456789-456789)|(ListingId=789012-789012)
            for all other resources and classes of 777
            """

            query = ""
            for key in sub_chunk:
                query = query + f"({key_column_name}={key}-{key})|"
            query = query[:-1]

            if resource_name.lower() in ["office", "agent"]:
                sub_chunk = (
                    str(sub_chunk)
                    .replace("[", "")
                    .replace("]", "")
                    .replace(" ", "")
                    .replace("'", "")
                )
                query = f"({key_column_name}={sub_chunk})"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": query,
            "Limit": 1000,
        }
        request_data["query_params"] = query_params

        # Data Downloading
        df = data_download(request_data, query_type)
        if len(df) == 0:
            continue
        else:
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
    source_data, request_data, property_classes, flow_type, rds_cursor, rds_connection
):
    """download_temp_table"""
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    listing_key_column = source_info["listing_key_column"]
    listing_modification_timestamp_column = source_info[
        "listing_modification_timestamp_column"
    ]
    photo_modification_timestamp_column = source_info[
        "photo_modification_timestamp_column"
    ]
    query_type = source_info["query_type"]

    hitting_query = get_hitting_query(source_data, flow_type)
    temp_table_df = pd.DataFrame()

    # data_df = None

    for class_name in property_classes:

        resource_name = "Property"
        table_name = "temp_table"

        select = f"{listing_key_column},{listing_modification_timestamp_column},{photo_modification_timestamp_column}"

        if flow_type == "sold":
            sold_column = source_data["source_info"]["sold_column"]
            select = select + f",{sold_column}"

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": hitting_query,
            "Select": select,
        }

        request_data["query_params"] = query_params

        class_count = get_class_count(request_data, query_type)

        if class_count == 0:
            continue  # continue to the next class of property

        top = 1000
        skip = 0

        query_params = {
            "SearchType": resource_name,
            "Class": class_name,
            "Query": hitting_query,
            "Select": select,
            "Limit": top,
            "Offset": skip,
        }

        search_url = request_data["Search"]
        source_type = source_data["source_info"]["source_type"]
        lmd_date = source_data["last_modification_date"]
        lmd_date = str(lmd_date).split(".", 1)[0].replace(":", "").replace("-", "")

        data_df = pd.DataFrame(
            [
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "total_count": class_count,
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
            f"{lmd_date}_temp",
            resource_name,
            "Request",
        )  # lmd_temp as batch_id; just for folder path

        while True:
            query_params["Offset"] = skip
            request_data["query_params"] = query_params

            # get data-frame
            data_df = data_download(request_data, query_type)
            temp_table_df = pd.concat([temp_table_df, data_df], ignore_index=True)

            data_df["source_id"] = source_id
            data_df["source_creation_date"] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            data_df["search_url"] = str(search_url)
            data_df["query_params"] = str(query_params)

            skip += top
            if skip + top >= class_count:
                break

    # if data_df is not None:
    #     del data_df
    # del data_df # release memory; no longer needed.

    if flow_type == "respecs":
        temp_table_df.insert(0, "respecs_flag", True)
    elif flow_type == "sold":
        temp_table_df.rename(
            columns={f"{sold_column}": "sold_date"},
            inplace=True,
        )

    temp_table_df.insert(0, "source_id", int(source_id))

    # Keep only required columns (removes extra columns returned by default from source)
    temp_table_df = temp_table_df[
        [
            col
            for col in temp_table_df.columns
            if col
            in [
                "source_id",
                listing_key_column,
                listing_modification_timestamp_column,
                photo_modification_timestamp_column,
                "sold_date",
            ]
        ]
    ]

    temp_table_df.rename(
        columns={
            listing_key_column: "listingkey",
            listing_modification_timestamp_column: "modification_timestamp",
            photo_modification_timestamp_column: "media_modification_timestamp",
        },
        inplace=True,
    )

    if len(temp_table_df) != 0:

        Upload_data_into_S3_DataLake(
                temp_table_df,
                source_id,
                source_type,
                source_name,
                f"{lmd_date}_temp",
                resource_name,
            )  # lmd_temp as batch_id; just for folder path

        table_creation_and_loading(
            temp_table_df,
            table_name,
            source_id,
            source_name,
            resource_name,
            rds_cursor,
            rds_connection,
        )

        return True

    return False


def columns_renaming(
    final_df, source_id, resource_name, class_name, rds_cursor, rds_connection
):
    renaming_cols = """select distinct lower(long_name), system_name from dev.field_metadata where source_id = {0} and resource_name = '{1}' and class_name = '{2}' and download_flag is true;""".format(
        source_id, resource_name, class_name
    )
    rds_cursor.execute(renaming_cols)
    renamed_columns = rds_cursor.fetchall()

    if renamed_columns[0] is None:
        return final_df
    else:
        for elem in renamed_columns:
            long_name = elem[0]
            system_name = elem[1]

            final_df.rename(columns={system_name: long_name}, inplace=True)

        return final_df


def request_and_load_tables(source_data, request_data, rds_connection, rds_cursor):
    """request_and_load_tables"""

    # get basic info for downloading
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    flow_type = source_data["flow_type"]
    source_info = source_data["source_info"]
    limit = source_info.get("limit", 1000)
    source_type = source_info["source_type"]
    chunk_size = source_info["chunk_size"]
    batch_id = source_data["batch_id"]
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
        listingkey as listingkey
        from idx_stage.temp_table
        where source_id ={source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}'
        order by {orderby_column}::timestamp {orderby_type}, listingkey
        Limit {limit};
        """

    rds_cursor.execute(query)
    temp_table_data = rds_cursor.fetchall()

    listing_keys = [val[0] for val in temp_table_data]

    listing_chunks = []
    agent_ids_chunks = []
    office_ids_chunks = []
    listing_chunks.extend(
        [
            listing_keys[i : i + chunk_size]
            for i in range(0, len(listing_keys), chunk_size)
        ]
    )

    query = """ SELECT resource_name, class_name 
        FROM dev.class_metadata WHERE source_id = {0} AND active_flag = 't' AND download_flag = 't'
        ORDER BY (CASE WHEN resource_name ~*'property' THEN 0 ELSE 1 END)
    """.format(source_id)
    rds_cursor.execute(query)
    download_classes = rds_cursor.fetchall()

    if "media" not in str(download_classes).lower():
        download_classes.append(("Media", "Media"))

    del listing_keys, temp_table_data, query, orderby_type, temp_respecs_flag, flow_type

    # get key columns for downloading classes data
    media_download_method = source_info["media_download_method"]
    agent_rank_columns = source_info["agent_rank_columns"]
    office_rank_columns = source_info["office_rank_columns"]
    listing_key_column = source_info["listing_key_column"]
    query_type = source_info["query_type"]

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime(
        "%Y-%m-%d %H:%M:%S"
    )  # for y_creation_date and y_last_update_date

    for resource_name, class_name in download_classes:

        data_df = pd.DataFrame()
        table_name = f"ps_rets_{resource_name.lower()}_{source_id}"

        # data request for a class
        if resource_name == "Property":

            data_df = download_data_for_given_list(
                source_id,
                resource_name,
                class_name,
                request_data,
                query_type,
                listing_key_column,
                listing_chunks,
            )

            if len(data_df) == 0:
                continue  # listings not returned for chunks for a class; may return for other class

            agent_ids_chunks.extend(
                chunks_creation(data_df, agent_rank_columns, chunk_size)
            )

            office_ids_chunks.extend(
                chunks_creation(data_df, office_rank_columns, chunk_size)
            )

        elif resource_name in ["Agent", "Member"]:

            agent_key_column = source_info["agent_key_column"]
            data_df = download_data_for_given_list(
                source_id,
                resource_name,
                class_name,
                request_data,
                query_type,
                agent_key_column,
                agent_ids_chunks,
            )

            del agent_ids_chunks

        elif resource_name == "Office":

            office_key_column = source_info["office_key_column"]
            data_df = download_data_for_given_list(
                source_id,
                resource_name,
                class_name,
                request_data,
                query_type,
                office_key_column,
                office_ids_chunks,
            )

            del office_ids_chunks

        elif resource_name.lower() == "openhouse":

            openhouse_key_column = source_info["openhouse_key_column"]
            data_df = download_data_for_given_list(
                source_id,
                resource_name,
                class_name,
                request_data,
                query_type,
                openhouse_key_column,
                listing_chunks,
            )

        elif resource_name == "Media":  # if media to download

            if media_download_method == "keyChunks":
                media_key_column = source_info["media_key_column"]
                data_df = download_data_for_given_list(
                    source_id,
                    resource_name,
                    class_name,
                    request_data,
                    query_type,
                    media_key_column,
                    listing_chunks,
                )

            elif media_download_method == "getObject":
                table_name = f"ps_rets_{resource_name.lower()}"
                data_df = get_object_media(
                    request_data["session"],
                    request_data["GetObject"],
                    source_info["photo_type_column"],
                    listing_chunks,
                )

        else:
            pass

        # data loading for a class
        if len(data_df) != 0:
            if not (resource_name == "Media" and media_download_method == "getObject"):
                data_df = columns_renaming(
                    data_df,
                    source_id,
                    resource_name,
                    class_name,
                    rds_cursor,
                    rds_connection,
                )

            data_df = adding_extra_columns(
                data_df,
                source_id,
                class_name,
                batch_id,
                source_data["batch_creation_date"],
                formatted_datetime,
            )

            # Upload raw data to S3 Data Lake
            Upload_data_into_S3_DataLake(
                data_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                resource_name,
            )

            data_df.drop(
                columns=["search_url", "query_params"], inplace=True, errors="ignore"
            )  # dropping columns that were only needed for data archival

            table_creation_and_loading(
                data_df,
                table_name,
                source_id,
                source_name,
                resource_name,
                rds_cursor,
                rds_connection,
            )

    del data_df, listing_chunks

    return True


def validation_func(rds_connection, rds_cursor, homelisting_cursor, source_data):
    """Validation Function"""

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    auth = source_data["auth"]

    try:

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

        if flow_type == "sold":
            max_last_modified_date, sold_date = get_lmd(
                source_id, rds_cursor, homelisting_cursor, flow_type
            )
            source_data["sold_date"] = sold_date

        else:
            max_last_modified_date = get_lmd(
                source_id,
                rds_cursor,
                homelisting_cursor,
                flow_type,
                rolling_window_offset,
            )

        source_data["last_modification_date"] = max_last_modified_date

        query = f""" DELETE FROM idx_stage.temp_table
        where source_id = {source_id} and download_flag = 'f' ; """
        rds_cursor.execute(query)
        rds_connection.commit()

        # Common function to get total count for all classes
        def get_total_count():
            query = f""" select count(distinct listingkey) from idx_stage.temp_table 
                where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """

            rds_cursor.execute(query)
            result = rds_cursor.fetchone()[0]
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
            fetch_classes_query = f"""
                select distinct class_name from dev.class_metadata
                where source_id = {source_id} and download_flag = true and resource_name ='Property'
                order by 1;
            """
            rds_cursor.execute(fetch_classes_query)
            property_classes = rds_cursor.fetchall()
            property_classes = [row[0] for row in property_classes]  # type: ignore

            request_data = login(auth)

            status = download_temp_table(
                source_data,
                request_data,
                property_classes,
                flow_type,
                rds_cursor,
                rds_connection,
            )
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

        rds_cursor.execute(query)
        latest_listing_date = rds_cursor.fetchone()[0]
        latest_listing_date = (
            max_last_modified_date
            if latest_listing_date is None
            else formatted_date(latest_listing_date)
        )
        source_data["latest_listing_date"] = latest_listing_date
        source_data["download_flag"] = True
        source_data["flow_type"] = flow_type

        return source_data

    except:
        raise


def data_download_func(rds_cursor, rds_connection, source_data):
    """Data Download Function"""

    try:

        request_data = login(source_data["auth"])

        download_status = request_and_load_tables(
            source_data, request_data, rds_connection, rds_cursor
        )

        download_event_response = {
            "source_id": source_data["source_id"],
            "source_name": source_data["source_name"],
            "mls_board": source_data["source_info"].get("mls_board"),
            "batch_creation_date": source_data["batch_creation_date"],
            "batch_id": source_data["batch_id"],
            "flow_type": source_data["flow_type"],
            "limit": source_data["source_info"].get("limit", 1000),
            "source_type": source_data["source_info"].get("source_type"),
            "last_refresh_date": source_data["last_modification_date"],
            "status": download_status,
            "temp_table_status": source_data["temp_table_status"],
            "bl_flag": source_data["batch_execution_params"]["bl_flag"],
            "run_host": source_data["run_host"],
            "success": False,
        }

        return download_event_response

    except:
        raise


def lambda_handler(event, context):
    """Main Lambda Handler Function"""

    listing_database = os.environ.get("listingDatabase")
    rds_database = os.environ.get("rdsDatabase")
    sql_exec_limit = context.get_remaining_time_in_millis()
    db_secret_rds = fetch_secrets(rds_database)
    db_secret_listing = fetch_secrets(listing_database)
    rds_connection = db_conn(db_secret_rds, sql_exec_limit)
    homelisting_connection = db_conn(db_secret_listing, sql_exec_limit)
    rds_cursor = rds_connection.cursor()  # type: ignore
    homelisting_cursor = homelisting_connection.cursor()  # type: ignore

    try:

        # LAST MODIFIED DATE FOR SOURCE_ID EXECUTION TO DOWNLOAD DATA FROM THAT DATE
        if event.get("download_flag") is True:

            event_response = data_download_func(
                rds_cursor,
                rds_connection,
                event,
            )

        else:
            event_response = validation_func(
                rds_connection,
                rds_cursor,
                homelisting_cursor,
                event,
            )

    except Exception as e:
        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(log_msg)

        logger.error(event)
        event_response = event

    finally:
        if rds_cursor:
            rds_connection.close()
            rds_cursor.close()

        if homelisting_cursor:
            homelisting_connection.close()
            homelisting_cursor.close()

    return event_response
