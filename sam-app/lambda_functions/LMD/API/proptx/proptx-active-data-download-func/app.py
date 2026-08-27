"""PropTX Validation and Data Download with Temp-Table"""

import requests
import pandas as pd
import boto3
import psycopg2
from psycopg2 import extras
import logging
from urllib.parse import urlencode
import traceback
import os
import json
from datetime import datetime, timezone
import time
import io

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("PropTX-Active-Lambda")
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)

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
        logger.info("Connection established successfully")
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)

def formatted_date(date):
    """Convert a date (string, tuple, or datetime) to ISO‑8601 Z format."""
    dummy_date = "1990-01-01 00:00:00"
    naive = datetime.strptime(dummy_date, "%Y-%m-%d %H:%M:%S")
    default_utc = naive.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if date is None:
        return default_utc
    if isinstance(date, str):
        try:
            dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%SZ")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return default_utc
    if isinstance(date, tuple):
        return formatted_date(date[0])
    if isinstance(date, datetime):
        date = date.replace(tzinfo=timezone.utc)
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    return default_utc

def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value

def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")

def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    """Add standard metadata columns."""
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    generic_df.insert(0, "source_creation_date", formatted_datetime)
    generic_df.insert(0, "y_last_update_date", batch_creation_date)
    generic_df.insert(0, "y_creation_date", batch_creation_date)
    generic_df.insert(0, "source_last_update_date", formatted_datetime)
    generic_df.insert(0, "batch_id", int(batch_id))
    generic_df.insert(0, "source_id", int(source_id))
    return generic_df

def ensure_folder_structure(s3, bucket, path):
    parts = path.strip("/").split("/")
    cumulative_path = ""
    for part in parts:
        cumulative_path += part + "/"
        s3.put_object(Bucket=bucket, Key=cumulative_path)

def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, class_Name, Skip=None
):
    if Skip is not None:
        filename = f"{source_name}_{class_Name}_{Skip}.parquet"
    else:
        filename = f"{source_name}_{class_Name}.parquet"

    folder_path = f"{source_type}/{source_id}_{source_name}/{class_Name}_{source_id}/{batch_id}/"
    s3_key = folder_path + filename

    df_upload = df_upload.copy()
    df_upload.columns = df_upload.columns.map(lambda x: str(x).replace(".", "_"))
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")
    ensure_folder_structure(s3, bucket_name, folder_path)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

def table_creation_and_loading(df_instance,table_name,source_id,cursor_serverless,serverless_db_con,flow_type,source_name,resource_name,):
    """Insert DataFrame into idx_stage.<table_name>, creating missing columns."""
    df_instance = df_instance.fillna(pd.NaT).fillna("")
    df_instance_filtered = df_instance.apply(lambda col: col.map(clean_value))
    df_instance_filtered = df_instance_filtered.drop_duplicates()

    column_names = """SELECT column_name FROM information_schema.columns
                      WHERE table_name = '{}' and column_name not in ('id')""".format(
        table_name
    )
    cursor_serverless.execute(column_names)
    table_column_names = [col[0] for col in cursor_serverless.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()

    if flow_type == "sold" and "lotsizedimensions" in df_instance_filtered.columns:
        df_instance_filtered["lotsizedimensions"] = None

    df_cols = list(df_instance_filtered.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN {1} TEXT""".format(
                table_name, n
            )
            cursor_serverless.execute(alter_query)
            serverless_db_con.commit()
            insert_query = """INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name)
                VALUES ({0}, '{1}', '{2}', '{2}', '{3}', '{3}')""".format(
                source_id, source_name, resource_name, n
            )
            cursor_serverless.execute(insert_query)
            serverless_db_con.commit()

    cols = ",".join(list(df_instance_filtered.columns))
    data_values = [tuple(row) for row in df_instance_filtered.values]

    insert_query = """
    INSERT INTO idx_stage.{0} ({1}) VALUES %s
    """.format(
        table_name, cols
    )

    if table_name in (
        f"ps_proptx_media_{source_id}",
        f"ps_proptx_propertyrooms_{source_id}",
    ):
        insert_query = insert_query.replace("order", '"order"')

    extras.execute_values(cursor_serverless, insert_query, data_values)
    logger.info(
        {
            "source_id": source_id,
            "table": f"idx_stage.{table_name}",
            "insert count": len(data_values),
        }
    )
    serverless_db_con.commit()

def request_and_load_temp_table(source_data, flow_type, cursor_serverless, rds_connection):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    status_column = source_info["status_column"] 
    last_modification_date = source_data["last_modification_date"]
    token = source_data["auth"]["password"]
    loginurl = source_data["auth"]["loginUrl"]
    source_type = source_info["source_type"]
    property_url = loginurl.replace("$metadata", "Property")
    headers = {"Authorization": f"Bearer {token}"}
    sold_date_column = source_info["sold_date_column"]


    cdc_filter = (
            f"ModificationTimestamp ge {last_modification_date} "
        )
    property_type_filter = (
            f"and PropertyType ne 'Residential Lease' "
            f"and PropertyType ne 'Commercial Lease' "
            f"and PropertyType ne 'Land Lease'"
    )
    select_fields = "ListingKey,ModificationTimestamp,PhotosChangeTimestamp" if flow_type in ["full_load", "respecs"] else f"ListingKey,ModificationTimestamp,PhotosChangeTimestamp,{sold_date_column}"
    orderby = f"{sold_date_column} asc,ModificationTimestamp asc" if flow_type == "sold" else "ModificationTimestamp asc"

    if flow_type == "sold":
        sold_status_value = source_info["sold_status_value"]
        sold_date = source_data.get("sold_date") 
        sold_filter = (
            f"({sold_date_column} ge {sold_date} and {cdc_filter}) "
            f"and {status_column} in ({sold_status_value}) "
            f"and TransactionType eq 'For Sale' "
        ) 
        filter_val =  sold_filter + property_type_filter

    elif flow_type in ["full_load", "respecs"]:
        active_statuses = source_info["active_status_values"]
        filter_val = cdc_filter + f"and {status_column} in ({active_statuses}) " + property_type_filter

    else:
        filter_val = cdc_filter + property_type_filter

    params = {
        "$filter": filter_val,
        "$orderby": orderby,
        "$select": select_fields,
        "$top": 5000,
        "$count": "true",
    }

    temp_table_df = pd.DataFrame()
    top = 5000
    skip = 0
    total_count = 1  # dummy to enter loop

    lmd_date = (
        str(last_modification_date).split(".", 1)[0].replace(":", "").replace("-", "")
    )
    lmd_date = f"{lmd_date}_temp"

    while skip < total_count and skip <= 50000:
        params["$skip"] = skip
        query_string = urlencode(params, safe=":").replace("+", "%20")
        url_endpoint = f"{property_url}?{query_string}"

        try:
            response = requests.get(url_endpoint, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
        except requests.exceptions.HTTPError as http_err:
            # If 400, check if it's a "no data" error
            if response.status_code == 400:
                try:
                    error_body = response.json()
                except:
                    error_body = response.text
                # Log the error for debugging
                logger.warning(f"400 on page skip={skip}, body: {error_body}")
                
                # You need to know the exact message the API sends when data is empty.
                # Common patterns: "No records found", "Empty result", "Resource not found"
                # Adjust this condition based on your API’s actual response.
                if ("no records" in str(error_body).lower() or
                    "empty" in str(error_body).lower() or
                    "not found" in str(error_body).lower()):
                    # Treat as normal end of data
                    logger.info(f"Empty result set (400) at skip={skip}, stopping pagination")
                    break
                else:
                    # Real error, raise again
                    raise
            else:
                # Other HTTP errors, retry after sleep
                time.sleep(5)
                response = requests.get(url_endpoint, headers=headers)
                response.raise_for_status()
                data = json.loads(response.text)
        except Exception:
            # Non-HTTP errors (timeout, connection, etc.) - retry once
            time.sleep(5)
            response = requests.get(url_endpoint, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)

        total_count = data.get("@odata.count", 0)
        page_df = pd.DataFrame(data["value"])
        temp_table_df = pd.concat([temp_table_df, page_df], ignore_index=True)

        skip += top

    if len(temp_table_df) == 0:
        logger.info(f"No records found for temp_table, source {source_id}")
        return

    Upload_data_into_S3_DataLake(
        temp_table_df, source_id, source_type, source_name, lmd_date, "Property"
    )

    # Rename columns and add source metadata
    temp_table_df.rename(
        columns={
            "ListingKey": "listingkey",
            "ModificationTimestamp": "modification_timestamp",
            "PhotosChangeTimestamp": "media_modification_timestamp",
        },
        inplace=True,
    )
    temp_table_df["source_id"] = source_id

    if flow_type == "respecs":
        temp_table_df["respecs_flag"] = "t"
    if flow_type not in ["full_load", "respecs"]:
        temp_table_df.rename(
            columns={source_info["sold_date_column"]: "sold_date"}, inplace=True
        )

    # Insert into idx_stage.temp_table (reuse table_creation_and_loading)
    table_creation_and_loading(
        temp_table_df,
        "temp_table",
        source_id,
        cursor_serverless,
        rds_connection,
        flow_type,
        source_name,
        "Property",
    )

def get_max_last_modified_date(pentaho_db_con,serverless_db_con,source_id,cursor_serverless,cursor_pentaho,flow_type,rolling_window_offset=None,):
    """Determine the starting modification date for the current flow."""
    if flow_type == "respecs":
        query = f"SELECT respecs_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "backlog":
        query = f"SELECT bl_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "full_load":
        query = f"""SELECT full_load_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"""
    elif flow_type == "sold":
        query = f"SELECT last_modified_date, batch_id, sold_date FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
        cursor_serverless.execute(query)
        data = cursor_serverless.fetchall()
        df = pd.DataFrame(data, columns=["last_modified_date", "batch_id", "sold_date"])
        current_lmd = df["last_modified_date"][0]
        prev_batch = df["batch_id"][0]
        sold_date = df["sold_date"][0]

        query = f"select max(modification_timestamp)::timestamp from listing WHERE source_id = {source_id} and batch_id = {prev_batch}"
        cursor_pentaho.execute(query)
        prev_lmd = cursor_pentaho.fetchone()[0]

        if current_lmd == prev_lmd:
            mod_ts = "1990-01-01 00:00:00"
        else:
            query = f"select min(sold_date), max(sold_date) from listing_p_sold where source_id = {source_id} and batch_id = {prev_batch}"
            cursor_pentaho.execute(query)
            min_sd, max_sd = cursor_pentaho.fetchone()
            if min_sd == max_sd:
                query = f"select max(modification_timestamp)::timestamp from listing_p_sold where source_id = {source_id} and batch_id = {prev_batch}"
                cursor_pentaho.execute(query)
                mod_ts = cursor_pentaho.fetchone()[0]
            else:
                mod_ts = "1990-01-01 00:00:00"
        return formatted_date(mod_ts), str(sold_date)
    else:
        query = f"SELECT last_modified_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
        if rolling_window_offset:
            query = f"SELECT last_modified_date::timestamp(0) - interval '{rolling_window_offset} hours' FROM stage.serverless_idx_loads WHERE source_id = {source_id}"

    cursor_serverless.execute(query)
    max_date = cursor_serverless.fetchone()
    return formatted_date(max_date)

def temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_serverless):
    query = f""" select count(distinct listingkey), min(modification_timestamp),  max(modification_timestamp) 
        from idx_stage.temp_table 
        where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """
    cursor_serverless.execute(query)
    result, max_last_modified_date, latest_listing_date = cursor_serverless.fetchone()
    return (
        int(result),
        formatted_date(max_last_modified_date),
        formatted_date(latest_listing_date),
    )

def validation_func(source_data, serverless_db_con, cursor_serverless, pentaho_db_con, cursor_homelisting):
    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        flow_type = source_info["flow_type"]
        rolling_window_batch = source_info["rolling_window_batch"]
        runtime_count = source_data["runtime_count"]
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params["bl_flag"]
        itr_value = batch_execution_params["itr_value"]
        respecs_flag = batch_execution_params["respecs_flag"]

        temp_respecs_flag = "f"
        rolling_window_offset = None
        sold_date = None

        # 1. Determine effective flow type and get LMD
        if flow_type not in ["sold", "full_load"]:
            if runtime_count % rolling_window_batch == 0:
                rolling_window_offset = source_info.get("rolling_window_offset")
                flow_type = "rolling_window"
            if respecs_flag and (runtime_count % itr_value != 0):
                flow_type = "respecs"
                temp_respecs_flag = "t"
            elif bl_flag and (runtime_count % itr_value != 0):
                flow_type = "backlog"

        del_query = f""" DELETE FROM idx_stage.temp_table where source_id = {source_id} and download_flag = 'f' """
        cursor_serverless.execute(del_query)
        serverless_db_con.commit()

        log_message = {
            "source_id": source_id,
            "source_name": source_name,
            "deleted_count": cursor_serverless.rowcount,
            "Query": del_query,
        }
        logger.info(log_message)

        total_count, last_modified_date, latest_listing_date = (
                temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_serverless)
            )

        # source_data["last_modification_date"] = max_last_modified_date
        # source_data["flow_type"] = flow_type

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


        # 3. Load new temp_table records
        if source_data["temp_table_status"]:

            if flow_type == "sold":
                last_modified_date, sold_date = get_max_last_modified_date(
                    pentaho_db_con, serverless_db_con, source_id,
                    cursor_serverless, cursor_homelisting, flow_type
                )
                source_data["sold_date"] = sold_date
            else:
                last_modified_date = get_max_last_modified_date(
                    pentaho_db_con, serverless_db_con, source_id,
                    cursor_serverless, cursor_homelisting, flow_type,
                    rolling_window_offset
                )

            source_data["last_modification_date"] = (
                    last_modified_date  # from serverless_idx_loads when temp_table is empty; will download in temp_table after that date
                )

            request_and_load_temp_table(source_data, flow_type, cursor_serverless, serverless_db_con)
            
            total_count, last_modified_date, latest_listing_date = (
                    temp_table_aggregation_stats(source_id, temp_respecs_flag, cursor_serverless)
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

        logger.info({
            "source_id": source_id,
            "source_name": source_name,
            "flow_type": flow_type,
            "total_count": total_count,
            "latest_listing_date": latest_listing_date,
            "download_flag": source_data["download_flag"],
        })

        return source_data

    except Exception as e:
        log_msg = {"Error": str(e)}
        logger.error(log_msg)
        raise

def api_call_and_load_tables(batch_creation_date,loginurl,token,source_id,source_name,batch_id,cursor,connection,flow_type,source_type,listingkeys_chunks,officekeys_chunks,memberkeys_chunks,):
   
    classes_query = """select class_name from dev.class_metadata
                       where source_id = {} and download_flag = 'true' order by id""".format(source_id)
    cursor.execute(classes_query)
    classes = [k[0] for k in cursor.fetchall()]

    # Ensure Property comes first
    property_index = classes.index("Property")
    classes.insert(0, classes.pop(property_index))

    chunk_size = 5
    url_base = loginurl.replace("$metadata", "")
    headers = {"Authorization": f"Bearer {token}"}

    # We'll fill these while processing Property
    office_chunks_from_prop = []
    member_chunks_from_prop = []

    for proptx_class in classes:
        resource_url = url_base + proptx_class
        if proptx_class == "Property":
            property_data = []
            # Use the pre-supplied listing keys chunks
            for chunk in listingkeys_chunks:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = f"ListingKey in ({keys_str})"
                params = {"$filter": filter_val, "$top": len(chunk)}
                query_string = urlencode(params, safe=":")
                query_string = query_string.replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"

                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)

                property_data.extend(data["value"])

                # Upload request info on first chunk (similar to original)
                if chunk == listingkeys_chunks[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(
                        req_df, source_id, source_type, source_name, batch_id, proptx_class, "Request"
                    )

            df = pd.DataFrame(property_data)
            # Derive office and member keys from this property dataframe
            office_keys = []
            for col in ["ListOfficeKey","CoListOfficeKey","BuyerOfficeKey","CoBuyerOfficeKey","MainOfficeKey"]:
                if col in df.columns:
                    office_keys.extend(df[col].dropna().tolist())
            unique_office = list(dict.fromkeys(office_keys))
            office_chunks_from_prop = [unique_office[i:i+chunk_size] for i in range(0, len(unique_office), chunk_size)]

            member_keys = []
            for col in ["ListAgentKey","CoListAgentKey","BuyerAgentKey","CoBuyerAgentKey"]:
                if col in df.columns:
                    member_keys.extend(df[col].dropna().tolist())
            unique_member = list(dict.fromkeys(member_keys))
            member_chunks_from_prop = [unique_member[i:i+chunk_size] for i in range(0, len(unique_member), chunk_size)]

            # Clean and load property
            df_flatten = df.apply(lambda col: col.map(remove_characters))
            df_cleaned = df_flatten.apply(lambda col: col.map(clean_value))
            df_cleaned = df_cleaned.drop(columns=["@odata.id"], errors="ignore")

            # Single-file archival of the full Property resource for this batch
            Upload_data_into_S3_DataLake(
                df_cleaned, source_id, source_type, source_name, batch_id, proptx_class
            )

            prop_table = f"ps_proptx_property_{source_id}"
            read_load = adding_extra_columns(df_cleaned, batch_creation_date, source_id, batch_id)
            table_creation_and_loading(read_load, prop_table, source_id, cursor, connection,
                                       flow_type, source_name, proptx_class)

        elif proptx_class == "OpenHouse":
            openhouse_data = []
            for chunk in listingkeys_chunks:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = f"ListingKey in ({keys_str})"
                params = {"$filter": filter_val, "$orderby": "ListingKey", "$top": "200"}
                query_string = urlencode(params, safe=":").replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"
                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                openhouse_data.extend(data["value"])
                # Upload request info (first chunk only)
                if chunk == listingkeys_chunks[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(req_df, source_id, source_type, source_name,
                                                 batch_id, proptx_class, "Request")
            df_oh = pd.DataFrame(openhouse_data)
            if not df_oh.empty:
                df_oh = df_oh.apply(lambda col: col.map(remove_characters)).apply(lambda col: col.map(clean_value))
                df_oh = df_oh.drop(columns=["@odata.id"], errors="ignore")

                Upload_data_into_S3_DataLake(
                    df_oh, source_id, source_type, source_name, batch_id, proptx_class
                )

                oh_table = f"ps_proptx_openhouse_{source_id}"
                read_load_oh = adding_extra_columns(df_oh, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(read_load_oh, oh_table, source_id, cursor, connection,
                                           flow_type, source_name, proptx_class)

        elif proptx_class == "Office":
            office_data = []
            for chunk in office_chunks_from_prop:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = f"OfficeKey in ({keys_str})"
                params = {"$filter": filter_val}
                query_string = urlencode(params, safe=":").replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"
                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                office_data.extend(data["value"])
                # Request info for first chunk
                if chunk == office_chunks_from_prop[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(req_df, source_id, source_type, source_name,
                                                 batch_id, proptx_class, "Request")
            df_off = pd.DataFrame(office_data)
            if not df_off.empty:
                df_off = df_off.apply(lambda col: col.map(remove_characters)).apply(lambda col: col.map(clean_value))
                df_off = df_off.drop(columns=["@odata.id"], errors="ignore")

                # Single-file archival of the full Office resource for this batch
                Upload_data_into_S3_DataLake(
                    df_off, source_id, source_type, source_name, batch_id, proptx_class
                )

                off_table = f"ps_proptx_office_{source_id}"
                read_load_off = adding_extra_columns(df_off, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(read_load_off, off_table, source_id, cursor, connection,
                                           flow_type, source_name, proptx_class)

        elif proptx_class == "Member":
            member_data = []
            for chunk in member_chunks_from_prop:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = f"MemberKey in ({keys_str})"
                params = {"$filter": filter_val}
                query_string = urlencode(params, safe=":").replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"
                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                member_data.extend(data["value"])
                if chunk == member_chunks_from_prop[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(req_df, source_id, source_type, source_name,
                                                 batch_id, proptx_class, "Request")
            df_mem = pd.DataFrame(member_data)
            if not df_mem.empty:
                df_mem = df_mem.apply(lambda col: col.map(remove_characters)).apply(lambda col: col.map(clean_value))
                df_mem = df_mem.drop(columns=["@odata.id"], errors="ignore")

                # Single-file archival of the full Member resource for this batch
                Upload_data_into_S3_DataLake(
                    df_mem, source_id, source_type, source_name, batch_id, proptx_class
                )

                mem_table = f"ps_proptx_member_{source_id}"
                read_load_mem = adding_extra_columns(df_mem, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(read_load_mem, mem_table, source_id, cursor, connection,
                                           flow_type, source_name, proptx_class)

        elif proptx_class == "PropertyRooms":
            rooms_data = []
            for chunk in listingkeys_chunks:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = f"ListingKey in ({keys_str})"
                params = {"$filter": filter_val, "$orderby": "ListingKey", "$top": "1000"}
                query_string = urlencode(params, safe=":").replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"
                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                rooms_data.extend(data["value"])
                if chunk == listingkeys_chunks[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(req_df, source_id, source_type, source_name,
                                                 batch_id, proptx_class, "Request")
            df_rooms = pd.DataFrame(rooms_data)
            if not df_rooms.empty:
                df_rooms = df_rooms.apply(lambda col: col.map(remove_characters)).apply(lambda col: col.map(clean_value))
                df_rooms = df_rooms.drop(columns=["@odata.id"], errors="ignore")

                # Single-file archival of the full PropertyRooms resource for this batch
                Upload_data_into_S3_DataLake(
                    df_rooms, source_id, source_type, source_name, batch_id, proptx_class
                )

                rooms_table = f"ps_proptx_propertyrooms_{source_id}"
                read_load_rooms = adding_extra_columns(df_rooms, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(read_load_rooms, rooms_table, source_id, cursor, connection,
                                           flow_type, source_name, proptx_class)

        elif proptx_class == "Media":
            media_data = []
            for chunk in listingkeys_chunks:
                keys_str = ",".join(f"'{k}'" for k in chunk)
                filter_val = (
                    f"ResourceRecordKey in ({keys_str}) and MediaStatus eq 'Active' "
                    f"and MediaCategory eq 'Photo' and ImageSizeDescription eq 'Large'"
                )
                params = {
                    "$filter": filter_val,
                    "$orderby": "ResourceRecordKey,Order",
                    "$top": "5000"
                }
                query_string = urlencode(params, safe=":").replace("+", "%20")
                url_endpoint = f"{resource_url}?{query_string}"
                try:
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                except Exception:
                    time.sleep(5)
                    response = requests.get(url_endpoint, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                media_data.extend(data["value"])
                if chunk == listingkeys_chunks[0]:
                    req_df = pd.DataFrame([{**params, "Request_url": url_endpoint}])
                    req_df = adding_extra_columns(req_df, batch_creation_date, source_id, batch_id)
                    Upload_data_into_S3_DataLake(req_df, source_id, source_type, source_name,
                                                 batch_id, proptx_class, "Request")
            df_media = pd.DataFrame(media_data)
            if not df_media.empty:
                df_media = df_media.apply(lambda col: col.map(remove_characters)).apply(lambda col: col.map(clean_value))
                df_media = df_media.drop(columns=["@odata.id"], errors="ignore")

                # Single-file archival of the full Media resource for this batch
                Upload_data_into_S3_DataLake(
                    df_media, source_id, source_type, source_name, batch_id, proptx_class
                )

                media_table = f"ps_proptx_media_{source_id}"
                read_load_media = adding_extra_columns(df_media, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(read_load_media, media_table, source_id, cursor, connection,
                                           flow_type, source_name, proptx_class)

    return True

def download_func(source_data, cursor_serverless, serverless_db_con):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    source_type = source_info["source_type"]
    mls_board = source_info["mls_board"]
    auth = source_data["auth"]
    loginurl = auth["loginUrl"]
    token = auth["password"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    flow_type = source_data["flow_type"]
    run_host = source_data["run_host"]
    limit = source_info.get("limit", 1000)
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params.get("bl_flag", False)

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

    cursor_serverless.execute(query)
    listing_keys = cursor_serverless.fetchall()
    listing_keys = [row[0] for row in listing_keys]

    chunk_size = 20
    listingkeys_chunks = [
        listing_keys[i:i+chunk_size] for i in range(0, len(listing_keys), chunk_size)
    ]

    try:
        status_flag = api_call_and_load_tables(
            batch_creation_date,
            loginurl,
            token,
            source_id,
            source_name,
            batch_id,
            cursor_serverless,
            serverless_db_con,
            flow_type,
            source_type,
            listingkeys_chunks,
            [],   # officekeys_chunks (will be derived inside)
            [],   # memberkeys_chunks (will be derived inside)
        )

        final_response = {
            "source_id": source_id,
            "source_name": source_name,
            "batch_id": batch_id,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_creation_date": batch_creation_date,
            "last_refresh_date": source_data["last_modification_date"],
            "temp_table_status": source_data["temp_table_status"],
            "status": status_flag,
            "run_host": run_host,
            "flow_type": flow_type,
            "limit":limit,
            "bl_flag": bl_flag,
        }
        logger.info(final_response)
        return final_response

    except Exception as e:
        final_response = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "source_type": source_type,
            "batch_id": batch_id,
            "batch_creation_date": batch_creation_date,
            "last_refresh_date": source_data["last_modification_date"],
            "temp_table_status": source_data["temp_table_status"],
            "status": False,
            "run_host": run_host,
            "flow_type": flow_type,
            "limit":limit,
            "bl_flag": bl_flag,
        }
        log_msg = {"Error": e, "Error At line": traceback.format_exc(), "Payload": final_response}
        logger.error(log_msg)
        return final_response

def lambda_handler(event, context):
    logger.info(event)
    source_data = event
    download_flag = source_data["download_flag"]
    sqlExecLimit = context.get_remaining_time_in_millis()
    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    db_secret_dev = fetch_secrets(rdsDatabase)
    db_secret_stage = fetch_secrets(listingDatabase)
    serverless_db_con = setup_db_connection(db_secret_dev, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    pentaho_db_con = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_pentaho = pentaho_db_con.cursor()

    try:
        if download_flag:
            final_response = download_func(source_data, cursor_serverless, serverless_db_con)
        else:
            final_response = validation_func(
                source_data, serverless_db_con, cursor_serverless,
                pentaho_db_con, cursor_pentaho
            )
        final_response["success"] = source_data.get("success", False)
        logger.info(final_response)
        return final_response
    except Exception as e:
        log_msg = {"Error": str(e), "Error At Line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data
    finally:
        cursor_serverless.close()
        cursor_pentaho.close()
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()