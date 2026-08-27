# Rapattoni active data download lambda

import requests
import psycopg2
import pandas as pd
from psycopg2 import extras
import json
import boto3
import os
import traceback
import logging
from datetime import datetime, timedelta as delta
import time
import io

logger = logging.getLogger("Rapattoni-Active-Lambda")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        log_msg = {"Error": e}
        raise Exception(log_msg)


# make connection with PostgreSQL
def setup_db_connection(db_secret, sqlExecLimit):
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
            options=f"-c statement_timeout={sqlExecLimit}",
        )
        return connection
    except Exception as e:
        log_msg = {"Error": e}
        raise Exception(log_msg)


# max lmd date
def get_lmd(source_id, cursor_rds, flow_type, rolling_window_offset):

    if flow_type == "lmd":
        query_lmd = f"""SELECT last_modified_date::timestamp FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "rolling_window":
        query_lmd = f"""SELECT last_modified_date::timestamp - INTERVAL '{rolling_window_offset} hours' 
            FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "backlog":
        query_lmd = f"""SELECT bl_start_date::timestamp FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "respecs":
        query_lmd = f""" SELECT respecs_start_date::timestamp FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "full_load":
        query_lmd = f"""SELECT full_load_date::timestamp FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""

    elif flow_type == "sold":
        query_lmd = f"""SELECT last_modified_date::timestamp, sold_date::date FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""
        cursor_rds.execute(query_lmd)
        result = cursor_rds.fetchone()
        max_timestamp = result[0]
        sold_date = result[1]
        formatted_timestamp = formatted_date(max_timestamp)

        return formatted_timestamp, str(sold_date)

    cursor_rds.execute(query_lmd)
    max_timestamp = cursor_rds.fetchone()[0]
    formatted_timestamp = formatted_date(max_timestamp)

    return formatted_timestamp, None


# specific format date
def formatted_date(date):
    if not date:
        date = "1990-01-01 00:00:00.000"
    if "." not in str(date):
        date = str(date) + ".000"
    original_datetime_aware = datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S.%f")
    formatted_time = original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return formatted_time


# clean none values from data frames
def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


# insertion into prestage tables
def prestage_tables_insertion(
    resource_df,
    source_id,
    source_name,
    resource_name,
    table_name,
    cursor_rds,
    rds_connection,
):

    if len(resource_df) == 0:
        return

    resource_df.drop(
        columns=[
            "request_url",
            "request_params",
            "@odata.id",
        ],
        inplace=True,
        errors="ignore",
    )

    resource_df = resource_df.apply(lambda col: col.map(remove_characters))
    resource_df = resource_df.apply(lambda col: col.map(clean_value))
    resource_df.fillna(pd.NaT)
    resource_df.fillna("")
    resource_df.drop_duplicates(inplace=True)
    resource_df.columns = resource_df.columns.str.lower()

    column_names = """SELECT lower(column_name) FROM information_schema.columns WHERE table_name ~* '{}' and column_name not in ('id','pid')""".format(
        table_name
    )
    cursor_rds.execute(column_names)
    table_column_names = [column[0] for column in cursor_rds.fetchall()]

    df_cols = list(resource_df.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for col in extra_cols:  # IF NOT EXISTS
            alter_query = f"""ALTER TABLE idx_stage.{table_name} ADD COLUMN IF NOT EXISTS {col} TEXT"""

            cursor_rds.execute(alter_query)

            # Sync metadata for new columns
            insert_query = f""" INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name)
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{col}', '{col}');
                """
            cursor_rds.execute(insert_query)
            rds_connection.commit()

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "alter_query": alter_query,
                "insert_query": insert_query,
            }
            logger.info(log_msg)

    del df_cols, table_column_names, extra_cols, new_columns

    cols = ",".join(list(resource_df.columns))
    data_values = [tuple(row) for row in resource_df.values]

    insert_query = """
    INSERT INTO idx_stage.{0} ({1}) VALUES %s
    """.format(table_name, cols)

    if "media" in table_name.lower():
        insert_query = (
            insert_query.replace(",order,", ',"order",')
            .replace(",order)", ',"order")')
            .replace("(order,", '("order",')
        )

    extras.execute_values(cursor_rds, insert_query, data_values)
    rds_connection.commit()

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "table_name": table_name,
        "rows_inserted": len(data_values),
    }
    logger.info(log_msg)


# remove special characters from data to avoid conflicts in insertion
def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


# add new columns in prestage tables
def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    """Adding extra columns to the DataFrame Which are required for the data processing"""
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    meta_df = pd.DataFrame(
        {
            "source_id": [int(source_id)],
            "batch_id": [int(batch_id)],
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


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, resource_name, skip=None
):
    filename = f"{source_name}_{resource_name}.parquet"
    if skip is not None:
        filename = f"{source_name}_{resource_name}_{skip}.parquet"

    folder_path = f"{source_type}/{source_id}_{source_name}/{resource_name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload = df_upload.copy()
    df_upload.columns = df_upload.columns.map(lambda x: str(x).replace(".", "_"))
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")

    def ensure_folder_structure(s3, bucket, path):
        parts = path.strip("/").split("/")
        cumulative_path = ""
        for part in parts:
            cumulative_path += part + "/"
            s3.put_object(Bucket=bucket, Key=cumulative_path)

    ensure_folder_structure(s3, bucket_name, folder_path)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())


# creating chunks' lists for keys/mlsids
def chunks_creation(df, key_column_names, chunk_size):
    value_keys = []
    # key_chunks = []

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

    return key_chunks


# run-time token generation
def create_token(auth):
    """Create a new token using client credentials"""

    tokenUrl = auth["tokenUrl"]
    client_id = auth["client_id"]
    client_secret = auth["client_secret"]

    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url=tokenUrl, headers=headers, data=data, timeout=60)

    if response.status_code == 200:
        response = json.loads(response.content)
        return response

    else:
        ret = {"statusCode": response.status_code, "body": "Token Generation Failed"}
        logger.error(ret)
        raise Exception("Token generation failed")


# fetch token from auth or generate new token
def get_token(source_id, auth, cursor_homelisting, homelisting_connection):
    """Generate or refresh token as needed"""

    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    access_token = auth["access_token"]
    expires_in = auth.get("expires_in", "1990-01-01 00:00:00")

    if expires_in < current_time:  # new token generation

        response = create_token(auth)
        expires_in_seconds = response["expires_in"]
        access_token = response["access_token"]

        # Calculate new expiration date
        future_date = now + delta(seconds=expires_in_seconds)
        future_date_str = future_date.strftime("%Y-%m-%d %H:%M:%S")

        # Update the database with new token and expiration date
        query = """UPDATE source 
                  SET auth = auth || %s::jsonb
                  WHERE id = %s"""

        auth_json_update = {
            "expires_in": future_date_str,
            "access_token": access_token,
        }

        cursor_homelisting.execute(query, (json.dumps(auth_json_update), source_id))
        homelisting_connection.commit()

    return access_token


def request_source(url, params, headers):

    data_list = []

    try:
        response = requests.get(url=url, params=params, headers=headers)
        response.raise_for_status()

    except:
        try:
            time.sleep(10)  # wait and request again
            response = requests.get(url=url, params=params, headers=headers)
            response.raise_for_status()

        except Exception as e:
            log_msg = {
                "error_message": str(e),
                "server_response": response,
            }
            raise Exception(log_msg)

    data = json.loads(response.text)

    data = json.loads(response.text)
    total_count = data.get("@odata.count", 0)

    data_list.extend(
        [
            {**record, "request_url": url, "request_params": params}
            for record in data["value"]
        ]
    )
    return pd.DataFrame(data_list), total_count


def download_data_for_chunks(
    class_name,
    request_url,
    headers,
    key_column,
    key_chunks,
    chunk_size,
    expand_classes=None,
):

    data_df = pd.DataFrame()

    if len(key_chunks) > 0:

        for each_chunk in key_chunks:

            filter_value = " or ".join(
                [f"{key_column} eq '{key}'" for key in each_chunk]
            )

            params = {
                "$filter": filter_value,
                "$top": chunk_size,
            }

            if class_name:
                params["Class"] = class_name

            if expand_classes:
                params["$expand"] = expand_classes

            temp_df, _ = request_source(request_url, params, headers)

            data_df = pd.concat([data_df, temp_df], ignore_index=True)

    return data_df


def request_and_load_temp_table(
    source_data,
    flow_type,
    cursor_rds,
    rds_connection,
    cursor_homelisting,
    homelisting_connection,
):
    """request_and_load_temp_table"""

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    source_type = source_info["source_type"]
    status_column = source_info["status_column"]
    modification_column = source_info["modification_column"]
    last_modified_date = source_data["last_modification_date"]

    auth = source_data["auth"]
    token = auth["password"]
    url = auth["loginUrl"]

    if auth.get("token_generation", False):
        token = get_token(source_id, auth, cursor_homelisting, homelisting_connection)

    lmd_date = (
        str(last_modified_date).split(".", 1)[0].replace(":", "").replace("-", "")
    )
    lmd_date = f"{lmd_date}_temp"

    temp_table_df = pd.DataFrame()
    top = 200
    skip = 0
    total_count = 1000  # initial dummy value

    headers = {"Authorization": f"Bearer {token}"}

    filter_value = f"{modification_column} ge {last_modified_date}"

    params = {
        "$count": "true",
        "$top": top,
        "$orderby": f"{modification_column} asc",
        "$select": f"ListingKey,{modification_column},PhotosChangeTimestamp",
    }

    if flow_type in ["full_load", "respecs"]:
        active_status = source_info["active_status"]

        active_status = active_status.split(",")  # make list of active_status
        active_status_values = " or ".join(
            [f"{status_column} eq {status}" for status in active_status]
        )

        filter_value = filter_value + f" and ({active_status_values})"

    if flow_type == "sold":
        sold_status = source_info["sold_status"]
        sold_column = source_info["sold_column"]
        sold_date = source_data["sold_date"]

        params["$select"] = params["$select"] + f",{sold_column}"
        params["$orderby"] = f"{sold_column} asc"

        filter_value = (
            f"{status_column} eq {sold_status} and {sold_column} ge {sold_date}"
        )

    params["$filter"] = (
        filter_value
        + " and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"
    )

    query = f"SELECT resource_name, class_name FROM dev.class_metadata WHERE source_id = {source_id} AND download_flag = 't' and resource_name = 'Property'"
    cursor_rds.execute(query)
    resources = cursor_rds.fetchall()
    resource_name = resources[0][0]
    class_name = resources[0][1]

    if class_name:
        params["Class"] = class_name

    url = url.replace("$metadata", resource_name)

    while (skip < total_count) and (skip <= 10000):

        params["$skip"] = skip
        # GET data
        temp_df, total_count = request_source(url, params, headers)
        temp_table_df = pd.concat([temp_table_df, temp_df], ignore_index=True)

        if skip == 0:
            request_df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_name": source_name,
                        "total_count": total_count,
                        "request_url": url,
                        "request_params": params,
                    }
                ]
            )
            Upload_data_into_S3_DataLake(
                request_df,
                source_id,
                source_type,
                source_name,
                lmd_date,
                "Property",
                "Request",
            )
            del request_df

            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "message": f"total count from source = {total_count} after timestamp: {last_modified_date}",
                "params": params,
            }
            logger.info(log_msg)

        skip += top  # update skip for next request in loop

    if len(temp_table_df) != 0:
        if flow_type == "respecs":
            temp_table_df.insert(0, "respecs_flag", True)
        elif flow_type == "sold":
            temp_table_df.rename(
                columns={f"{sold_column}": "sold_date"},
                inplace=True,
            )

        temp_table_df.insert(0, "source_id", source_id)

        temp_table_df.rename(
            columns={
                "ListingKey": "ListingKey",
                f"{modification_column}": "modification_timestamp",
                f"PhotosChangeTimestamp": "media_modification_timestamp",
            },
            inplace=True,
        )

        temp_table_df = temp_table_df[
            [
                col
                for col in [
                    "ListingKey",
                    "modification_timestamp",
                    "media_modification_timestamp",
                    "sold_date",
                    "source_id",
                    "respecs_flag",
                ]
                if col in temp_table_df.columns
            ]
        ]  # keep only required columns for insertion in temp_table; drop the rest.

        Upload_data_into_S3_DataLake(
            temp_table_df,
            source_id,
            source_type,
            source_name,
            lmd_date,
            "Property",
        )

        prestage_tables_insertion(
            temp_table_df,
            source_id,
            source_name,
            "Property",
            "temp_table",
            cursor_rds,
            rds_connection,
        )


# Aggregations on temp_table
def temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_rds):
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
    source_data, cursor_rds, rds_connection, cursor_homelisting, homelisting_connection
):
    """Validation Function"""

    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        runtime_count = source_data["runtime_count"]
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params["bl_flag"]
        itr_value = batch_execution_params["itr_value"]
        respecs_flag = batch_execution_params["respecs_flag"]
        flow_type = source_info["flow_type"]
        rolling_window_batch = source_info["rolling_window_batch"]
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

        total_count, last_modified_date, latest_listing_date = (
            temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_rds)
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
            last_modified_date  # min date from temp_table; when temp_table is not empty
        )

        if source_data["temp_table_status"]:

            last_modified_date, sold_date = get_lmd(
                source_id,
                cursor_rds,
                flow_type,
                rolling_window_offset,
            )

            source_data["last_modification_date"] = last_modified_date
            source_data["sold_date"] = sold_date

            # downloading temp_table
            request_and_load_temp_table(
                source_data,
                flow_type,
                cursor_rds,
                rds_connection,
                cursor_homelisting,
                homelisting_connection,
            )

            total_count, last_modified_date, latest_listing_date = (
                temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_rds)
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
                if latest_listing_date and latest_listing_date > last_modified_date
                else last_modified_date
            )

        source_data["latest_listing_date"] = str(latest_listing_date)
        source_data["flow_type"] = flow_type
        source_data["row_count"] = total_count
        source_data["download_flag"] = True

        return source_data

    except Exception as e:
        log_msg = {"Error": str(e)}
        logger.error(log_msg)
        raise


# call download
def download_func(
    source_data, cursor_rds, rds_connection, cursor_homelisting, homelisting_connection
):

    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        batch_id = source_data["batch_id"]
        batch_creation_date = source_data["batch_creation_date"]
        auth = source_data["auth"]
        loginurl = auth["loginUrl"]
        token = auth["password"]
        limit = source_info.get("limit", 1000)
        source_type = source_info["source_type"]
        expand_classes = source_info["expand_classes"]
        flow_type = source_data["flow_type"]
        bl_flag = source_data["batch_execution_params"]["bl_flag"]

        if auth.get("token_generation", False):
            token = get_token(
                source_id, auth, cursor_homelisting, homelisting_connection
            )

        headers = {"Authorization": f"Bearer {token}"}

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
        listing_keys = cursor_rds.fetchall()
        listing_keys = [row[0] for row in listing_keys]

        listing_chunks = []
        chunk_size = 100

        listing_chunks.extend(
            [
                listing_keys[i : i + chunk_size]
                for i in range(0, len(listing_keys), chunk_size)
            ]
        )

        del listing_keys  # releasing memory

        query = f"SELECT resource_name, class_name FROM dev.class_metadata WHERE source_id = {source_id} AND download_flag = 't' ORDER BY case when resource_name = 'Property' then 0 else 1 end"
        cursor_rds.execute(query)
        resources = cursor_rds.fetchall()

        for res in resources:
            resource_name = res[0]
            class_name = res[1]

            data_df = pd.DataFrame()
            request_url = loginurl.replace("$metadata", resource_name)

            if resource_name == "Property":

                property_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "ListingKey",
                    listing_chunks,
                    chunk_size,
                    expand_classes,
                )

                if len(property_df) != 0:
                    # extracting media data-frame from property data-frame

                    if not expand_classes:
                        expand_classes = "Media"

                    expand_classes = expand_classes.split(
                        ","
                    )  # make list of expand_classes

                    for expanded_class in expand_classes:

                        if not expanded_class in property_df.columns:
                            continue

                        table_name = f"ps_rapattoni_{expanded_class}_{source_id}"

                        if (
                            "media" in expanded_class.lower()
                            or "picture" in expanded_class.lower()
                        ):
                            table_name = f"ps_rapattoni_media"

                        expanded_class_df = pd.json_normalize(
                            property_df[expanded_class].dropna().sum()
                        )

                        # drop expanded columns from property_df
                        property_df.drop(
                            columns=[
                                expanded_class,
                            ],
                            inplace=True,
                            errors="ignore",
                        )

                        if len(expanded_class_df) != 0:

                            expanded_class_df = adding_extra_columns(
                                expanded_class_df,
                                batch_creation_date,
                                source_id,
                                batch_id,
                            )
                            Upload_data_into_S3_DataLake(
                                expanded_class_df,
                                source_id,
                                source_type,
                                source_name,
                                batch_id,
                                expanded_class,
                            )
                            prestage_tables_insertion(
                                expanded_class_df,
                                source_id,
                                source_name,
                                resource_name,
                                table_name,
                                cursor_rds,
                                rds_connection,
                            )

                            del expanded_class_df

                # office/agent key/mlsid chunks creation from Property's columns
                office_mlsid_chunks = chunks_creation(
                    property_df,
                    [
                        "ListOfficeMlsId",
                        "CoListOfficeMlsId",
                        "BuyerOfficeMlsId",
                        "CoBuyerOfficeMlsId",
                    ],
                    chunk_size,
                )
                office_key_chunks = chunks_creation(
                    property_df,
                    [
                        "ListOfficeKey",
                        "CoListOfficeKey",
                        "BuyerOfficeKey",
                        "CoBuyerOfficeKey",
                    ],
                    chunk_size,
                )
                agent_key_chunks = chunks_creation(
                    property_df,
                    [
                        "ListAgentKey",
                        "CoListAgentKey",
                        "BuyerAgentKey",
                        "CoBuyerAgentKey",
                    ],
                    chunk_size,
                )
                agent_mlsid_chunks = chunks_creation(
                    property_df,
                    [
                        "ListAgentMlsId",
                        "CoListAgentMlsId",
                        "BuyerAgentMlsId",
                        "CoBuyerAgentMlsId",
                    ],
                    chunk_size,
                )

                data_df = property_df
                del property_df

            elif resource_name == "PropertyUnitTypes":

                property_unit_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "ListingKey",
                    listing_chunks,
                    chunk_size,
                )

                data_df = property_unit_df
                del property_unit_df, listing_chunks

            elif resource_name == "Office":

                office_mlsid_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "OfficeMlsId",
                    office_mlsid_chunks,
                    chunk_size,
                )

                office_key_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "OfficeKey",
                    office_key_chunks,
                    chunk_size,
                )

                data_df = pd.concat([office_mlsid_df, office_key_df], ignore_index=True)
                del (
                    office_mlsid_df,
                    office_key_df,
                    office_mlsid_chunks,
                    office_key_chunks,
                )

            elif resource_name == "Member" or resource_name == "ActiveAgents":

                agent_mlsid_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "MemberMlsId",
                    agent_mlsid_chunks,
                    chunk_size,
                )

                agent_key_df = download_data_for_chunks(
                    class_name,
                    request_url,
                    headers,
                    "MemberKey",
                    agent_key_chunks,
                    chunk_size,
                )

                data_df = pd.concat([agent_mlsid_df, agent_key_df], ignore_index=True)
                del agent_mlsid_df, agent_key_df, agent_mlsid_chunks, agent_key_chunks

            elif resource_name == "OpenHouse":
                current_time = datetime.now()
                current_time = current_time - delta(days=1)  # last 1 day data
                current_time = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                total_count = 1000  # initial dummy value
                top = 200
                skip = 0
                params = {
                    "$filter": f"OpenHouseStartTime ge {current_time}",
                    "$top": top,
                    "$skip": skip,
                    "$count": "true",
                }

                if class_name:
                    params["Class"] = class_name

                while skip < total_count:

                    params["$skip"] = skip

                    temp_df, total_count = request_source(request_url, params, headers)
                    data_df = pd.concat([data_df, temp_df], ignore_index=True)

                    skip += top

                del temp_df

            # preparing resource's dataframe for insertion
            if len(data_df) != 0:

                data_df = adding_extra_columns(
                    data_df, batch_creation_date, source_id, batch_id
                )
                Upload_data_into_S3_DataLake(
                    data_df,
                    source_id,
                    source_type,
                    source_name,
                    batch_id,
                    resource_name,
                )
                prestage_tables_insertion(
                    data_df,
                    source_id,
                    source_name,
                    resource_name,
                    f"ps_rapattoni_{resource_name}_{source_id}",
                    cursor_rds,
                    rds_connection,
                )

    except Exception as e:
        log_msg = {"Error": str(e)}
        logger.error(log_msg)
        raise

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "source_type": source_type,
        "flow_type": flow_type,
        "limit": limit,
        "batch_id": batch_id,
        "batch_creation_date": batch_creation_date,
        "mls_board": source_info.get("mls_board"),
        "bl_flag": bl_flag,
        "temp_table_status": source_data["temp_table_status"],
        "last_refresh_date": source_data["last_modification_date"],
        "run_host": source_data["run_host"],
        "status": True,
        "success": False,
    }
    return final_response


# start lambda execution
def lambda_handler(event, context):

    source_data = event

    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    db_secret_dev = fetch_secrets(rdsDatabase)
    db_secret_stage = fetch_secrets(listingDatabase)
    rds_connection = setup_db_connection(db_secret_dev, sqlExecLimit)
    homelisting_connection = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_rds = rds_connection.cursor()
    cursor_homelisting = homelisting_connection.cursor()

    try:

        if source_data["download_flag"]:
            final_response = download_func(
                source_data,
                cursor_rds,
                rds_connection,
                cursor_homelisting,
                homelisting_connection,
            )

        else:
            final_response = validation_func(
                source_data,
                cursor_rds,
                rds_connection,
                cursor_homelisting,
                homelisting_connection,
            )

        return final_response

    except Exception as e:
        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At Line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data

    finally:
        if cursor_rds:
            cursor_rds.close()
            rds_connection.close()
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()
