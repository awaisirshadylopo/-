# Importing required libraries
import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone
import os
import io
import time
import traceback
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s - %(message)s", force=True)


# FUNCTION TO FETCH SECRETS FROM AWS SECRETS MANAGER
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# FUNCTION TO SET UP A POSTGRESQL DATABASE CONNECTION
def setup_db_connection(secret, sqlExecLimit):
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        options=f"-c statement_timeout={sqlExecLimit}",
    )

    return conn


# Aggregations on temp_table
def get_total_count(source_id, temp_respecs_flag, cursor_rds):
    query = f""" select count(distinct listingkey), min(modification_timestamp),  max(modification_timestamp) 
        from idx_stage.temp_table 
        where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """
    cursor_rds.execute(query)
    result, max_last_modified_date, latest_listing_date = cursor_rds.fetchone()
    return (
        int(result),
        formatted_date(max_last_modified_date),
        formatted_date(latest_listing_date),
    )


def validation_func(
    rds_connection,
    cursor_rds,
    cursor_homelisting,
    source_data,
):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    runtime_count = source_data["runtime_count"]
    source_info = source_data["source_info"]
    flow_type = source_info.get("flow_type", "lmd").lower()
    rolling_window_batch = source_info["rolling_window_batch"]
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params["bl_flag"]
    itr_value = batch_execution_params["itr_value"]
    respecs_flag = batch_execution_params["respecs_flag"]
    rolling_window_offset = None
    temp_respecs_flag = "f"

    if flow_type not in ["sold", "full_load"]:

        if respecs_flag is True and (runtime_count % itr_value != 0):
            flow_type = "respecs"
            temp_respecs_flag = "t"

        elif bl_flag is True and (runtime_count % itr_value != 0):
            flow_type = "backlog"

        elif runtime_count % rolling_window_batch == 0:
            rolling_window_offset = source_info["rolling_window_offset"]
            flow_type = "rolling_window"

    query = f""" DELETE FROM idx_stage.temp_table where source_id = {source_id} and download_flag = 'f' """
    cursor_rds.execute(query)
    rds_connection.commit()

    log_message = {
        "source_id": source_id,
        "source_name": source_name,
        "deleted_count": cursor_rds.rowcount,
        "Query": query,
    }
    logger.info(log_message)

    total_count, max_last_modified_date, latest_listing_date = get_total_count(
        source_id, temp_respecs_flag, cursor_rds
    )

    source_data["temp_table_status"] = (
        True
        if (
            total_count == 0
            or (bl_flag is True and flow_type in ["lmd", "rolling_window"])
        )
        else False
    )

    source_data["last_modification_date"] = (
        max_last_modified_date  # min date from temp_table; when temp_table is not empty
    )
    if flow_type == "sold":
        max_last_modified_date, sold_date = get_lmd(
            source_id, cursor_rds, cursor_homelisting, flow_type
        )
        source_data["sold_date"] = sold_date
    else:
        max_last_modified_date = get_lmd(
            source_id,
            cursor_rds,
            cursor_homelisting,
            flow_type,
            rolling_window_offset,
        )
        source_data["last_modification_date"] = (
            max_last_modified_date  # from serverless_idx_loads when temp_table is empty; will download in temp_table after that date
        )
    if source_data["temp_table_status"]:

        status = request_and_load_temp_table(
            source_data, flow_type, cursor_rds, rds_connection
        )
        source_data["status"] = status
        total_count, max_last_modified_date, latest_listing_date = get_total_count(
            source_id, temp_respecs_flag, cursor_rds
        )

    log_message = {
        "source_id": source_id,
        "source_name": source_name,
        "flow_type": flow_type,
        "temp_table_status": source_data["temp_table_status"],
        "total_count": total_count,
    }
    logger.info(log_message)

    if flow_type == "respecs" and source_data["temp_table_status"] is True:
        # get latest_listing_date for respecs_finish_date (only for API sources)
        query = f""" select max(modification_timestamp::timestamp) from listing_p_active where source_id = {source_id}; """
        cursor_homelisting.execute(query)
        latest_listing_date = cursor_homelisting.fetchone()[0]

    else:
        latest_listing_date = (
            latest_listing_date
            if latest_listing_date and latest_listing_date > max_last_modified_date
            else max_last_modified_date
        )

    source_data["flow_type"] = flow_type
    source_data["latest_listing_date"] = str(latest_listing_date)
    source_data["row_count"] = total_count
    source_data["download_flag"] = True

    logger.info(source_data)

    return source_data


def get_lmd(
    source_id,
    cursor_rds,
    cursor_homelisting,
    flow,
    rolling_window_offset=None,
):

    query_for_last_modified_date_serverless = None

    if flow == "backlog":
        query_for_last_modified_date_serverless = """
            SELECT bl_start_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {}""".format(source_id)

    elif flow == "sold":
        query_for_last_modified_date_serverless = f"SELECT last_modified_date, sold_date FROM stage.serverless_idx_loads WHERE source_id = {source_id};"
        cursor_rds.execute(query_for_last_modified_date_serverless)
        data = cursor_rds.fetchall()
        last_modified_date = data[0][0]
        sold_date = data[0][1]

        formatted_date_serverless = formatted_date(last_modified_date)
        return formatted_date_serverless, str(sold_date)

    elif flow == "respecs":
        query_for_last_modified_date_serverless = """
            SELECT respecs_start_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {}""".format(source_id)

    elif flow == "full_load":
        query_for_last_modified_date_serverless = """
            SELECT full_load_date::timestamp(0) FROM stage.serverless_idx_loads
            WHERE source_id = {}""".format(source_id)

    elif flow == "rolling_window":
        query_for_last_modified_date_serverless = """
            SELECT last_modified_date::timestamp(0) - interval '{1} hours' FROM stage.serverless_idx_loads
            WHERE source_id = {0} """.format(source_id, rolling_window_offset)

    else:
        query_for_last_modified_date_serverless = f"SELECT last_modified_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id} "

    cursor_rds.execute(query_for_last_modified_date_serverless)
    max_date_serverless = cursor_rds.fetchone()[0]

    # max_date_serverless = None if 'None' in str(max_date_serverless) else max_date_serverless
    formatted_date_serverless = formatted_date(max_date_serverless)

    return formatted_date_serverless


def formatted_date(date):
    # if no date found then use dummy date
    dumy_date = "1990-01-01 00:00:00"
    naive_datetime = datetime.strptime(dumy_date, "%Y-%m-%d %H:%M:%S")
    original_datetime_utc = naive_datetime.replace(tzinfo=timezone.utc)
    formatted_datetime_utc = original_datetime_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # data may be None or a tuple
    if date == None:
        return formatted_datetime_utc

    if isinstance(date, str):
        try:
            # Handle datetime string with milliseconds (e.g., "2024-10-23 08:30:28.334")
            original_datetime_aware = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            original_datetime_aware = original_datetime_aware.replace(
                tzinfo=timezone.utc
            )
            return original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            # If it doesn't match the expected format, return the default date
            return formatted_datetime_utc

    # If the input `date` is a tuple or any other format, handle it
    if isinstance(date, tuple):
        return formatted_date(date[0])  # Recursively call if the date is in a tuple

    # If it's a datetime object, convert it to the correct format
    if isinstance(date, datetime):
        date = date.replace(tzinfo=timezone.utc)  # Ensure it's in UTC timezone
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # If none of the above, return the default formatted date
    return formatted_datetime_utc


def data_download_func(
    rds_connection,
    source_data,
):
    flow = source_data.get("flow_type")
    batch_creation_date = source_data.get("batch_creation_date")
    source_info = source_data["source_info"]
    mls_board = source_info.get("mls_board")
    limit = source_info.get("limit", 1000)
    source_type = source_info.get("source_type")
    last_modification_date = source_data.get("last_modification_date")
    source_id = source_data.get("source_id")
    source_name = source_data.get("source_name")
    batch_id = source_data.get("batch_id")
    run_host = source_data.get("run_host")
    auth = source_data.get("auth")
    cursor = rds_connection.cursor()

    try:

        parameters = {
            "source_id": source_id,
            "source_name": source_name,
            "source_type": source_type,
            "batch_id": batch_id,
            "flow_type": flow,
            "last_modification_date": last_modification_date,
            "batch_creation_date": batch_creation_date,
            "rds_cursor": cursor,
            "rds_connection": rds_connection,
            "bl_flag": source_data["batch_execution_params"]["bl_flag"],
            "limit": limit,
        }

        status = api_call_and_load_tables(parameters, auth)

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "Message": "Data Download Successfully",
        }
        logger.info(log_msg)

        response = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "flow_type": flow,
            "source_type": source_type,
            "batch_creation_date": batch_creation_date,
            "batch_id": batch_id,
            "last_refresh_date": last_modification_date,
            "status": status,
            "success": source_data["success"],
            "run_host": run_host,
            "limit": limit,
            "temp_table_status": source_data["temp_table_status"],
            "bl_flag": source_data["batch_execution_params"]["bl_flag"],
        }
        if flow == "sold":
            response["sold_date"] = source_data["sold_date"]
            response["last_refresh_date"] = source_data["sold_date"]

        logger.info(response)
        return response
    except Exception as e:

        final_response = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(final_response)
        logger.error(source_data)
        return source_data


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


def columns_renaming(final_df, source_id, resource_name, cursor_rds):
    renaming_cols = """select distinct renamed_long_name, system_name from dev.field_metadata where source_id = {0} and resource_name = '{1}' ;""".format(
        source_id, resource_name
    )
    cursor_rds.execute(renaming_cols)
    renamed_columns = cursor_rds.fetchall()

    if len(renamed_columns) == 0:
        return final_df
    else:
        for elem in renamed_columns:
            long_name = elem[0]
            system_name = elem[1]

            final_df = final_df.rename(columns={system_name: long_name})

        return final_df


def table_creation_and_loading(
    df_instance,
    table_name,
    source_id,
    source_name,
    cursor_rds,
    rds_connection,
    resource_name,
    flow=None,
):
    df_instance.fillna(pd.NaT)
    df_instance.fillna("")

    df_instance = df_instance.drop(
        columns=["@odata.id", "FeedTypes", "Media"], errors="ignore"
    )
    df_instance = df_instance.apply(lambda col: col.map(remove_characters))
    df_instance_filtered = df_instance.apply(lambda col: col.map(clean_value))
    df_instance_filtered = df_instance_filtered.drop_duplicates()
    if flow == "sold" and "LotSizeDimensions" in df_instance_filtered.columns:
        df_instance_filtered["LotSizeDimensions"] = None

    if table_name not in ["Media"]:
        df_instance_filtered = columns_renaming(
            df_instance_filtered, source_id, resource_name, cursor_rds
        )
    column_names = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}' and column_name not in ('pid','id')""".format(
        table_name.lower()
    )
    cursor_rds.execute(column_names)
    table_column_names = [column[0] for column in cursor_rds.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    df_cols = list(df_instance_filtered.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN if not exists {1} TEXT""".format(
                table_name, n
            )
            cursor_rds.execute(alter_query)
            insert_query = f""" INSERT INTO dev.field_metadata 
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name) 
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}'); 
                """
            cursor_rds.execute(insert_query)
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": resource_name,
                "Message": f"Added new column {n} to {table_name}",
            }
            logger.info(log_msg)

            rds_connection.commit()

    cols = ",".join(list(df_instance_filtered.columns))
    data_values = [tuple(row) for row in df_instance_filtered.values]
    insert_query = """
    INSERT INTO idx_stage.{0} ({1}) VALUES %s
    """.format(table_name, cols)
    if table_name == "ps_bridge_media":
        insert_query = insert_query.replace("order", '"order"')
    extras.execute_values(cursor_rds, insert_query, data_values)

    rds_connection.commit()

    log_msg = {
        "source_id": source_id,
        "table_name": table_name,
        "Rows Inserted": len(df_instance_filtered),
    }

    logger.info(log_msg)


def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    generic_df.insert(0, "source_creation_date", formatted_datetime)
    generic_df.insert(0, "y_last_update_date", batch_creation_date)
    generic_df.insert(0, "y_creation_date", batch_creation_date)
    generic_df.insert(0, "source_last_update_date", formatted_datetime)
    generic_df.insert(0, "batch_id", int(batch_id))
    generic_df.insert(0, "source_id", int(source_id))
    return generic_df


def chunks_download_from_source(data_dict):
    """Download Data from Source Based on Chunks like hite for 20 ListingKeys each time"""
    download_data = []
    source_name = data_dict["source_name"]
    source_id = data_dict["source_id"]
    headers = data_dict["headers"]
    chunks_list = data_dict["values_list"]
    column_name = data_dict["column_name"]
    class_name = data_dict["class_name"]
    url_endpoint = data_dict["api_endpoint"]
    batch_id = data_dict["batch_id"]
    batch_creation_date = data_dict["batch_creation_date"]
    source_type = data_dict["source_type"]
    data_ = []
    for list_ in chunks_list:
        list_ = ", ".join(
            map(lambda item: "'{}'".format(str(item).replace("'", "")), list_)
        )
        params = {}
        params["$filter"] = ("{} in ({})".format(column_name, list_),)
        params["$top"] = 200
        response = None

        try:
            response = requests.get(
                url=url_endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()
            response_headers = response.headers
            data = json.loads(response.text)

        except requests.RequestException:
            time.sleep(5)
            response = requests.get(
                url=url_endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()
            response_headers = response.headers
            data = json.loads(response.text)

        download_data.extend(data["value"])
        # data["url_endpoint"] = url_endpoint
        # data.update(params)
        # data_.append(data)
        for record in data["value"]:
            data_.append({**record, "url_endpoint": url_endpoint, **params})
    

        if int(response_headers["Application-RateLimit-Remaining"]) <= 50:
            current_time = datetime.now(timezone.utc)
            burst_rate_limit_reset = response_headers["Application-RateLimit-Reset"]
            burst_rate_limit_reset_time = datetime.strptime(
                burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            # Convert naive datetime to UTC-aware
            burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                tzinfo=timezone.utc
            )

            wait_time = int(
                (burst_rate_limit_reset_time - current_time).total_seconds()
            )
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": class_name,
                "Message": f"Application-RateLimit-Exceed Waiting for {wait_time} seconds",
            }
            response_headers.update(log_msg)
            logger.error(response_headers)

            return False

        elif int(response_headers["Burst-RateLimit-Remaining"]) <= 15:
            current_time = datetime.now(timezone.utc)
            burst_rate_limit_reset = response_headers["Burst-RateLimit-Reset"]
            burst_rate_limit_reset_time = datetime.strptime(
                burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            # Convert naive datetime to UTC-aware
            burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                tzinfo=timezone.utc
            )

            wait_time = int(
                (burst_rate_limit_reset_time - current_time).total_seconds()
            )
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": class_name,
                "Message": f"Burst-RateLimit-Exceed Waiting for {wait_time} seconds",
            }
            response_headers.update(log_msg)
            logger.warning(response_headers)
            time.sleep(wait_time + 3)

    # data_df = pd.DataFrame(data_)
    data_df = pd.DataFrame(data_)
    data_df = adding_extra_columns(data_df, batch_creation_date, source_id, batch_id)
    Upload_data_into_S3_DataLake(
    data_df, source_id, source_type, source_name, batch_id, class_name
    )
    return download_data


def lmd_download_from_source(data_dict, params):
    """Download Data form Source Base on LMD (Last Modify Date)"""
    download_data = []
    upload_frames = []
    source_name = data_dict["source_name"]
    source_id = data_dict["source_id"]
    headers = data_dict["headers"]
    class_name = data_dict["class_name"]
    url_endpoint = data_dict["api_endpoint"]
    limit = data_dict["limit"]
    batch_id = data_dict["batch_id"]
    batch_creation_date = data_dict["batch_creation_date"]
    source_type = data_dict["source_type"]
    top = 200
    skip = 0
    while skip < limit:

        params["$top"] = top
        params["$skip"] = skip

        try:
            response = requests.get(
                url=url_endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()
            response_headers = response.headers
            data = json.loads(response.text)

        except requests.RequestException:
            time.sleep(5)
            response = requests.get(
                url=url_endpoint, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()
            response_headers = response.headers
            data = json.loads(response.text)

        data["url_endpoint"] = url_endpoint
        data.update(params)
        total_count = data["@odata.count"]
        download_data.extend(data["value"])

        upload_frames.extend([
            {**record, "url_endpoint": url_endpoint, **params}
            for record in data["value"]
        ])

        # Loading into S3 data lake for Property
        # df_upload = pd.DataFrame(data)
        # df_upload = adding_extra_columns(
        #     df_upload, batch_creation_date, source_id, batch_id
        # )
        # Upload_data_into_S3_DataLake(
        #     df_upload, source_id, source_type, source_name, batch_id, class_name, skip
        # )
       

        # If we have received all records, break out of the loop
        if skip + top >= total_count:
            break
        elif skip + top == 10000:
            break

        # download limit for OpenHouse class for dowloading all records in one go
        if skip == 0 and class_name == "OpenHouse" and total_count <= 10000:
            limit = total_count

        # Increment the skip parameter to fetch the next batch
        skip += top

        # Checking Hit limit and dynamic wait time calculating if needed
        if int(response_headers["Application-RateLimit-Remaining"]) <= 50:
            current_time = datetime.now(timezone.utc)
            burst_rate_limit_reset = response_headers["Application-RateLimit-Reset"]
            burst_rate_limit_reset_time = datetime.strptime(
                burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            # Convert naive datetime to UTC-aware
            burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                tzinfo=timezone.utc
            )

            wait_time = int(
                (burst_rate_limit_reset_time - current_time).total_seconds()
            )
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": class_name,
                "Message": f"Application-RateLimit-Exceed Waiting for {wait_time} seconds",
            }
            response_headers.update(log_msg)
            logger.error(response_headers)
            return False

        elif int(response_headers["Burst-RateLimit-Remaining"]) <= 15:
            current_time = datetime.now(timezone.utc)
            burst_rate_limit_reset = response_headers["Burst-RateLimit-Reset"]
            burst_rate_limit_reset_time = datetime.strptime(
                burst_rate_limit_reset, "%Y-%m-%dT%H:%M:%S.%fZ"
            )

            # Convert naive datetime to UTC-aware
            burst_rate_limit_reset_time = burst_rate_limit_reset_time.replace(
                tzinfo=timezone.utc
            )

            wait_time = int(
                (burst_rate_limit_reset_time - current_time).total_seconds()
            )
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": class_name,
                "Message": f"Burst-RateLimit-Exceed Waiting for {wait_time} seconds",
            }
            response_headers.update(log_msg)
            logger.warning(response_headers)
            time.sleep(wait_time + 3)

    if upload_frames:
        df_upload = pd.DataFrame(upload_frames)
        df_upload = adding_extra_columns(
            df_upload, batch_creation_date, source_id, batch_id
        )
        Upload_data_into_S3_DataLake(
            df_upload, source_id, source_type, source_name, batch_id, class_name
        )
    return download_data


def request_and_load_temp_table(source_data, flow_type, cursor_rds, rds_connection):
    """request_and_load_temp_table"""

    source_info = source_data["source_info"]
    status_column = source_info["status_column"]
    last_modified_date = source_data["last_modification_date"]
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_type = source_info["source_type"]

    auth = source_data["auth"]
    url = auth["loginUrl"]
    url = url.replace("$metadata", "Property")
    token = auth["password"]

    Timestamp_column = (
        "APIModificationTimestamp"
        if "navicamls" in url
        else "BridgeModificationTimestamp"
    )

    filter_value = f"{Timestamp_column} ge {last_modified_date}"

    params = {
        "$count": "true",
        "$orderby": f"{Timestamp_column} asc",
        "$select": f"ListingKey,{Timestamp_column},PhotosChangeTimestamp",
    }

    if flow_type == "sold":
        sold_column = source_info["sold_column"]
        sold_status = source_info["sold_status"]
        sold_date = source_data["sold_date"]

        filter_value = (
            f"{sold_column} ge {sold_date} and {status_column} in ({sold_status})"
        )
        params["$orderby"] = f"{sold_column} asc, {Timestamp_column} asc"
        params["$select"] = params["$select"] + f",{sold_column}"

    elif flow_type in ["full_load", "respecs"]:
        active_status = source_info["active_status"]
        filter_value = filter_value + f" and {status_column} in ({active_status})"

    params["$filter"] = (
        filter_value
        + " and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"
    )

    data_list = []
    top = 200
    skip = 0
    params["$top"] = top
    headers = {"Authorization": f"Bearer {token}"}
    total_count = 1000  # initial dummy value

    lmd_date = source_data["last_modification_date"]
    lmd_date = str(lmd_date).split(".", 1)[0].replace(":", "").replace("-", "")

    while skip <= total_count:
        params["$skip"] = skip

        try:
            response = requests.get(url=url, params=params, headers=headers)
            response.raise_for_status()

        except:
            time.sleep(10)
            response = requests.get(url=url, params=params, headers=headers)
            response.raise_for_status()

        data = json.loads(response.text)
        total_count = data.get("@odata.count", 0)
        data_list.extend(data["value"])

        if skip == 0:
            data_dict = [
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "creation_date": datetime.now(timezone.utc),
                    "source_count": total_count,
                    "request_url_endpoint": url,
                    "params": params,
                }
            ]

            df_upload = pd.DataFrame(data_dict)
            Upload_data_into_S3_DataLake(
                df_upload,
                source_id,
                source_type,
                source_name,
                f"{lmd_date}_temp",
                "Temp_Property",
                "Request",
            )

        df_upload = pd.DataFrame(data)
        Upload_data_into_S3_DataLake(
            df_upload,
            source_id,
            source_type,
            source_name,
            f"{lmd_date}_temp",
            "Property",
            skip,
        )

        if (skip + top >= total_count) or skip >= 10000:
            break
        skip += top

    temp_data = pd.DataFrame(data_list)

    if len(temp_data) != 0:

        temp_data = temp_data[
            ["ListingKey", f"{Timestamp_column}", "PhotosChangeTimestamp"]
        ]  # only keep required columns in data-frame

        if flow_type == "respecs":
            temp_data.insert(0, "respecs_flag", True)
        elif flow_type == "sold":
            temp_data.rename(
                columns={f"{sold_column}": "sold_date"},
                inplace=True,
            )

        temp_data.insert(0, "source_id", int(source_data["source_id"]))

        temp_data.rename(
            columns={
                "ListingKey": "ListingKey",
                f"{Timestamp_column}": "modification_timestamp",
                "PhotosChangeTimestamp": "media_modification_timestamp",
            },
            inplace=True,
        )

        table_creation_and_loading(
            temp_data,
            "temp_table",
            source_data["source_id"],
            source_data["source_name"],
            cursor_rds,
            rds_connection,
            "Property",
        )

        return True

    return False


def api_call_and_load_tables(data_dict, auth):

    loginurl = auth["loginUrl"]
    batch_creation_date = data_dict["batch_creation_date"]
    source_id = data_dict["source_id"]
    source_name = data_dict["source_name"]
    source_type = data_dict["source_type"]
    batch_id = data_dict["batch_id"]
    cursor = data_dict["rds_cursor"]
    connection = data_dict["rds_connection"]
    token = auth["password"]

    classes_query = """select class_name from dev.class_metadata where source_id = {} and download_flag = 't' order by id """.format(
        source_id
    )
    cursor.execute(classes_query)
    classes = cursor.fetchall()
    classes = [k[0] for k in classes]

    chunk_size = 20
    chunks = []
    office_chunks = []
    office_mls_ids_chunks = []
    member_chunks = []
    member_mls_ids_chunks = []
    property_index = classes.index("Property")
    classes.insert(0, classes.pop(property_index))

    loginurl = loginurl.replace("$metadata", "")
    data_dict["headers"] = {"Authorization": f"Bearer {token}"}

    flow_type = data_dict["flow_type"]
    bl_flag = data_dict["bl_flag"]
    limit = data_dict["limit"]

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

    cursor.execute(query)
    temp_table_data = cursor.fetchall()
    listing_keys_list = [val[0] for val in temp_table_data]
    chunks.extend(
        [listing_keys_list[i : i + 50] for i in range(0, len(listing_keys_list), 50)]
    )

    for class_name in classes:

        data_dict["api_endpoint"] = loginurl + class_name
        data_dict["class_name"] = class_name
        target_table = f"ps_bridge_{class_name.lower()}_{source_id}"
        data_dict["values_list"] = chunks
        data_dict["column_name"] = "ListingKey"
        data_dict["target_table"] = target_table

        downloaded_listing_data = []

        if class_name == "Property":

            media = []

            downloaded_listing_data = chunks_download_from_source(data_dict)

            if downloaded_listing_data is False:
                return False

            for i in downloaded_listing_data:
                m = i.get("Media") or []
                media.extend(m)

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "Class": "Media",
                "Downloaded_Count": len(media),
            }

            logger.info(log_msg)

            media_df = pd.DataFrame(media)
            read_load_df = adding_extra_columns(
                media_df, batch_creation_date, source_id, batch_id
            )

            Upload_data_into_S3_DataLake(
                read_load_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                "Media",
            )

            table_creation_and_loading(
                read_load_df,
                "ps_bridge_media",
                source_id,
                source_name,
                cursor,
                connection,
                class_name,
            )

            if source_id == 767:
                """Expending Room from Property only for 767 for Attribute"""
                rooms = []
                for i in downloaded_listing_data:
                    for room in i.get("Rooms", []):
                        room["ListingKey"] = i.get("ListingKey")
                        rooms.append(room)

                log_msg = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "Class": "Rooms",
                    "Downloaded_Count": len(rooms),  # type: ignore
                }

                logger.info(log_msg)

                rooms_df = pd.DataFrame(rooms)
                delete_query = f"DELETE FROM idx_stage.{target_table} WHERE source_id = {source_id};"
                cursor.execute(delete_query)
                connection.commit()
                read_load_df = adding_extra_columns(
                    rooms_df, batch_creation_date, source_id, batch_id
                )

                Upload_data_into_S3_DataLake(
                    read_load_df,
                    source_id,
                    source_type,
                    source_name,
                    batch_id,
                    class_name,
                )

                table_creation_and_loading(
                    read_load_df,
                    f"ps_bridge_rooms_{source_id}",
                    source_id,
                    source_name,
                    cursor,
                    connection,
                    class_name,
                )

            df = pd.DataFrame(downloaded_listing_data)
            listing_keys = list(df["ListingKey"])
            chunks.extend(
                [
                    listing_keys[i : i + chunk_size]
                    for i in range(0, len(listing_keys), chunk_size)
                ]
            )

            if "ListAgentMlsId" in df.columns:
                member_chunks = list(df["ListAgentMlsId"].dropna())

            if "CoListAgentMlsId" in df.columns:
                CoListAgentMlsId = list(df["CoListAgentMlsId"].dropna())
                member_chunks.extend(CoListAgentMlsId)

            if "BuyerAgentMlsId" in df.columns:
                BuyerAgentMlsId = list(df["BuyerAgentMlsId"].dropna())
                member_chunks.extend(BuyerAgentMlsId)

            if "CoBuyerAgentMlsId" in df.columns:
                CoBuyerAgentMlsId = list(df["CoBuyerAgentMlsId"].dropna())
                member_chunks.extend(CoBuyerAgentMlsId)

            if "ListOfficeMlsId" in df.columns:
                office_chunks = list(df["ListOfficeMlsId"])

            if "CoListOfficeMlsId" in df.columns:
                CoListOfficeMlsId = list(df["CoListOfficeMlsId"].dropna())
                office_chunks.extend(CoListOfficeMlsId)

            if "BuyerOfficeMlsId" in df.columns:
                BuyerOfficeMlsId = list(df["BuyerOfficeMlsId"].dropna())
                office_chunks.extend(BuyerOfficeMlsId)

            if "CoBuyerOfficeMlsId" in df.columns:
                CoBuyerOfficeMlsId = list(df["CoBuyerOfficeMlsId"].dropna())
                office_chunks.extend(CoBuyerOfficeMlsId)

            # Remove duplicates from the list
            officemlsid = list(set(office_chunks))
            office_mls_ids_chunks.extend(
                [
                    officemlsid[i : i + chunk_size]
                    for i in range(0, len(officemlsid), chunk_size)
                ]
            )

            # Remove duplicates from the list
            membermlsid = list(set(member_chunks))
            member_mls_ids_chunks.extend(
                [
                    membermlsid[i : i + chunk_size]
                    for i in range(0, len(membermlsid), chunk_size)
                ]
            )

        elif class_name == "Office":

            data_dict["values_list"] = office_mls_ids_chunks
            data_dict["column_name"] = "OfficeMlsId"
            downloaded_listing_data = chunks_download_from_source(data_dict)
            if downloaded_listing_data is False:
                return False

        elif class_name == "Member":

            data_dict["values_list"] = member_mls_ids_chunks
            data_dict["column_name"] = "MemberMlsId"
            downloaded_listing_data = chunks_download_from_source(data_dict)
            if downloaded_listing_data is False:
                return False

        elif class_name == "OpenHouse" and "Bridge" in source_type:

            """Download Data for OpenHouse"""
            # Get current timestamp and subtract one day
            current_timestamp = datetime.now(timezone.utc)
            one_day_ago = current_timestamp - pd.Timedelta(days=1)
            one_day_ago_formatted = one_day_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            params = {
                "$filter": f"OpenHouseStartTime ge {one_day_ago_formatted}",
                "$orderby": "OpenHouseStartTime asc",
                "$count": "true",
                "$top": 200,
                "$skip": 0,
            }
            downloaded_listing_data = lmd_download_from_source(data_dict, params)
            if downloaded_listing_data is False:
                return False

        else:

            """Download Data for OpenHouse, Rooms, UnitTypes"""
            downloaded_listing_data = chunks_download_from_source(data_dict)
            if downloaded_listing_data is False:
                return False

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "Class": class_name,
            "Downloaded_Count": len(downloaded_listing_data),  # type: ignore
        }

        logger.info(log_msg)

        df = pd.DataFrame(downloaded_listing_data)
        read_load_property = adding_extra_columns(
            df, batch_creation_date, source_id, batch_id
        )

        table_creation_and_loading(
            read_load_property,
            target_table,
            source_id,
            source_name,
            cursor,
            connection,
            class_name,
        )

    return True


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, class_Name, Skip=None
):
    # Construct filename and folder path
    if Skip is not None:
        filename = f"{source_name}_{class_Name}_{Skip}.parquet"
    else:
        filename = f"{source_name}_{class_Name}.parquet"

    folder_path = f"{source_type}/{source_id}_{source_name}/{class_Name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload = df_upload.copy()  # avoid mutating caller's DataFrame
    df_upload.columns = df_upload.columns.map(lambda x: str(x).replace(".", "_"))
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")

    ensure_folder_structure(s3, bucket_name, folder_path)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())


def ensure_folder_structure(s3, bucket, path):
    parts = path.strip("/").split("/")
    cumulative_path = ""
    for part in parts:
        cumulative_path += part + "/"
        s3.put_object(Bucket=bucket, Key=cumulative_path)  # Creates "folder"


def identifing_flow_type(event, pentaho_cursor):
    """This function will identify the flow type and return the parameters for the API call"""

    # Adding 1 seconds Interval to the last modification date to avoid Missing records
    last_modification_date = event["last_modification_date"]
    auth = event["auth"]
    Timestampe_column = (
        "APIModificationTimestamp"
        if "navicamls" in auth["loginUrl"]
        else "BridgeModificationTimestamp"
    )
    flow = event["flow_type"]
    source_info = event["source_info"]
    params = None
    if flow == "sold":
        sold_date = event["sold_date"]
        sold_column = source_info["sold_column"]
        status_column = source_info["status_column"]
        sold_status = source_info["sold_status"]
        params = {
            "$filter": f"({sold_column} ge {sold_date} and {Timestampe_column} ge {last_modification_date}) and {status_column} in ({sold_status}) and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'",
            "$count": "true",
            "$orderby": f"{sold_column} asc, {Timestampe_column} asc",
        }

    elif flow in ["full_load", "respecs"]:
        status_column = source_info["status_column"]
        active_status = source_info["active_status"]
        params = {
            "$filter": f"{Timestampe_column}  ge {last_modification_date} and {status_column} in ({active_status}) and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'",
            "$count": "true",
            "$orderby": f"{Timestampe_column} asc",
        }

    else:
        params = {
            "$filter": f"{Timestampe_column}  ge {last_modification_date} and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'",
            "$count": "true",
            "$orderby": f"{Timestampe_column} asc",
        }
    return params


def lambda_handler(event, context):

    logger.info(event)

    # DATABASE CONNECTIONS SETUP
    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    db_secret_dev = fetch_secrets(rdsDatabase)
    db_secret_stage = fetch_secrets(listingDatabase)
    rds_connection = setup_db_connection(db_secret_dev, sqlExecLimit)
    homelisting_connection = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_rds = rds_connection.cursor()
    cursor_homelisting = homelisting_connection.cursor()

    # FETCHING KEYWORDS FROM EVENT
    source_data = event
    try:

        # LAST MODIFIED DATE FOR SOURCE_ID EXECUTION TO DOWNLOAD DATA FROM THAT DATE
        if source_data["download_flag"] is True:

            response = data_download_func(
                rds_connection,
                source_data,
            )

        else:

            response = validation_func(
                rds_connection,
                cursor_rds,
                cursor_homelisting,
                source_data,
            )

        return response  # type: ignore

    except Exception as e:
        # LOGGING AN ERROR MESSAGE
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data

    finally:

        if cursor_homelisting:  # type: ignore
            cursor_homelisting.close()
            homelisting_connection.close()
        if cursor_rds:  # type: ignore
            cursor_rds.close()
            rds_connection.close()
