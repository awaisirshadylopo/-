"""
Trestle Active Validation and Download Function -- (with temp_table logic)
"""

import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import json
import os
import traceback
import logging
from datetime import datetime, timezone
import io
import time

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("TrestleAPI-Active-Lambda")
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


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


def create_token(source_id, source_name, client_id, client_secret):
    url = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
    }
    auth = (str(client_id), str(client_secret))
    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        return json.loads(response.content)["access_token"]
    else:
        logs = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "Token Generation Failed",
            "Status Code": response.status_code,
        }
        logger.error(logs)
        raise Exception("Token generation failed")


def formatted_date(date):
    # Default dummy date if input is None
    dummy = "1990-01-01 00:00:00"
    naive_dummy = datetime.strptime(dummy, "%Y-%m-%d %H:%M:%S")
    utc_dummy = naive_dummy.replace(tzinfo=timezone.utc)
    default_formatted = utc_dummy.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if date is None:
        return default_formatted

    # datetime object
    if isinstance(date, datetime):
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # string
    if isinstance(date, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(date, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                continue
        return default_formatted

    # tuple
    if isinstance(date, tuple):
        return formatted_date(date[0])

    return default_formatted


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value


def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    """Adding extra columns to the DataFrame Which are required for the data processing"""

    if len(generic_df) == 0:
        return generic_df

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

    folder_path = (
        f"{source_type}/{source_id}_{source_name}/{class_Name}_{source_id}/{batch_id}/"
    )
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


def table_creation_and_loading(
    df_instance, table_name, source_id, source_name, cursor, connection
):
    df_instance.fillna(pd.NaT)
    df_instance.fillna("")
    df_instance_filtered = df_instance.apply(lambda col: col.map(clean_value))
    df_instance_filtered = df_instance_filtered.drop_duplicates()

    column_names = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}' and column_name not in ('id')""".format(
        table_name
    )
    cursor.execute(column_names)
    table_column_names = [column[0] for column in cursor.fetchall()]
    df_instance_filtered.columns = df_instance_filtered.columns.str.lower()
    df_cols = list(df_instance_filtered.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)

    if extra_cols:
        for n in extra_cols:
            alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN {1} TEXT""".format(
                table_name, n
            )
            cursor.execute(alter_query)
            connection.commit()

    cols = ",".join(list(df_instance_filtered.columns))
    data_values = [tuple(row) for row in df_instance_filtered.values]
    insert_query = """INSERT INTO idx_stage.{0} ({1}) VALUES %s""".format(
        table_name, cols
    )
    if table_name == "ps_trestle_media":
        insert_query = insert_query.replace("order", '"order"')
    extras.execute_values(cursor, insert_query, data_values)
    connection.commit()

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "table_name": f"idx_stage.{table_name}",
        "rows_inserted": len(data_values),
    }
    logger.info(log_msg)


def get_lmd(
    source_id,
    cursor_serverless,
    cursor_homelisting,
    flow_type,
    rolling_window_offset=None,
):
    """
    Retrieve the last modification date from serverless_idx_loads.
    """
    if flow_type == "backlog":
        query = "SELECT bl_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {}".format(
            source_id
        )
    elif flow_type == "respecs":
        query = "SELECT respecs_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {}".format(
            source_id
        )
    elif flow_type == "full_load":
        query = "SELECT full_load_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {}".format(
            source_id
        )
    elif flow_type == "rolling_window":
        query = f"SELECT last_modified_date::timestamp(0) - interval '{rolling_window_offset} hours' FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "sold":
        query = f"SELECT last_modified_date, sold_date FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
        cursor_serverless.execute(query)
        data = cursor_serverless.fetchall()
        lmd = data[0][0]
        sold_date = data[0][1]
        return formatted_date(lmd), str(sold_date)
    else:
        query = f"SELECT last_modified_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"

    cursor_serverless.execute(query)
    result = cursor_serverless.fetchone()
    if result is None:
        return formatted_date("1990-01-01 00:00:00")
    return formatted_date(result[0])


def get_total_count_from_temp(source_id, temp_respecs_flag, cursor_serverless):
    query = f"""
        SELECT count(distinct listingkey),
               min(modification_timestamp),
               max(modification_timestamp)
        FROM idx_stage.temp_table
        WHERE source_id = {source_id}
          AND download_flag = 't'
          AND respecs_flag = '{temp_respecs_flag}'
    """
    cursor_serverless.execute(query)
    cnt, max_last_modified_date, latest_listing_date = cursor_serverless.fetchone()
    return (
        int(cnt),
        formatted_date(max_last_modified_date),
        formatted_date(latest_listing_date),
    )


def request_and_load_temp_table(
    source_data, flow_type, cursor_serverless, serverless_db_con
):
    """
    Download ListingKey, ModificationTimestamp and (if available) PhotosChangeTimestamp
    from the Trestle Property endpoint into idx_stage.temp_table.
    """
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    originating_system_name = source_info["originating_system_name"]
    status_column = source_info["status_column"]
    active_statuses = source_info["active_status"]
    sold_status = source_info["sold_status"]
    sold_column = source_info["sold_column"]
    last_modification_date = source_data["last_modification_date"]
    token = source_data["auth"]["token"]  # token is passed in source_data
    base_url = source_data["auth"]["request_url"]

    headers = {"Authorization": f"Bearer {token}"}
    top = 200
    skip = 0

    filter_str = (
        f"OriginatingSystemName eq {originating_system_name} "
        f"and ModificationTimestamp ge {last_modification_date} "
        f"and PropertyType ne 'ResidentialLease' and PropertyType ne 'CommercialLease'"
    )
    select_fields = (
        "ListingKey,ModificationTimestamp,PhotosChangeTimestamp"
        if flow_type in ["full_load", "respecs"]
        else f"ListingKey,ModificationTimestamp,PhotosChangeTimestamp,{sold_column}"
    )
    orderby = (
        f"{sold_column} asc, ModificationTimestamp asc"
        if flow_type == "sold"
        else "ModificationTimestamp asc"
    )

    # Build filter based on flow_type
    if flow_type == "sold":
        sold_date = source_data.get("sold_date")
        filter_str = (
            f"{filter_str}"
            f" and {sold_column} ge {sold_date} "
            f" and {status_column} eq {sold_status} "
        )

    elif flow_type in ["full_load", "respecs"]:
        filter_str = f"{filter_str}" f" and {status_column} in ({active_statuses}) "

    params = {
        "$filter": filter_str,
        "$orderby": orderby,
        "$count": "true",
        "$top": top,
        "$select": select_fields,
    }

    data_list = []
    temp_upload_frames = []
    while True:
        params["$skip"] = skip
        try:
            resp = requests.get(base_url, headers=headers, params=params)
            resp.raise_for_status()
        except Exception:
            time.sleep(10)
            resp = requests.get(base_url, headers=headers, params=params)
            resp.raise_for_status()
        data = resp.json()
        total_count = data.get("@odata.count", 0)
        data_list.extend(data["value"])
        # optional: upload request metadata to S3
        if skip == 0:
            req_df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_name": source_name,
                        "creation_date": datetime.now(timezone.utc),
                        "source_count": total_count,
                        "request_url_endpoint": base_url,
                        "params": params,
                    }
                ]
            )
            Upload_data_into_S3_DataLake(
                req_df,
                source_id,
                source_info["source_type"],
                source_name,
                f"{last_modification_date[:10]}_temp",
                "Property",
                "Request",
            )
        # df_page = pd.DataFrame(data["value"])
        # Upload_data_into_S3_DataLake(
        #     df_page,
        #     source_id,
        #     source_info["source_type"],
        #     source_name,
        #     f"{last_modification_date[:10]}_temp",
        #     "Property",
        #     skip,
        # )
        temp_upload_frames.extend(data["value"])
        if skip + top >= total_count or skip >= 10000:
            break
        skip += top

    if temp_upload_frames:
        df_temp = pd.DataFrame(temp_upload_frames)
        Upload_data_into_S3_DataLake(
            df_temp,
            source_id,
            source_info["source_type"],
            source_name,
            f"{last_modification_date[:10]}_temp",
            "Property",
        )

    if not data_list:
        return False

    temp_df = pd.DataFrame(data_list)

    temp_df.insert(0, "source_id", source_id)
    temp_df.insert(1, "respecs_flag", "t" if flow_type == "respecs" else "f")

    if flow_type in ["full_load", "respecs"]:
        rename_map = {
            "ListingKey": "listingkey",
            "ModificationTimestamp": "modification_timestamp",
            "PhotosChangeTimestamp": "media_modification_timestamp",
        }
    else:
        rename_map = {
            "ListingKey": "listingkey",
            "ModificationTimestamp": "modification_timestamp",
            "PhotosChangeTimestamp": "media_modification_timestamp",
            f"{sold_column}": "sold_date",
        }

    temp_df.rename(columns=rename_map, inplace=True)

    table_creation_and_loading(
        temp_df,
        "temp_table",
        source_id,
        source_name,
        cursor_serverless,
        serverless_db_con,
    )
    return True


def validation_func(source_data, token, cursor_serverless, cursor_homelisting):

    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        rolling_window_offset = source_info.get("rolling_window_offset")
        rolling_window_batch = source_info.get("rolling_window_batch")
        flow_type = source_info["flow_type"]
        runtime_count = source_data["runtime_count"]
        source_data["auth"]["token"] = token
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params.get("bl_flag", False)
        itr_value = batch_execution_params.get("itr_value")
        respecs_flag = batch_execution_params.get("respecs_flag", False)
        temp_respecs_flag = "f"

        # determine actual flow_type considering runtime flags
        if flow_type not in ["sold", "full_load"]:
            if respecs_flag and (runtime_count % itr_value != 0):
                flow_type = "respecs"
                temp_respecs_flag = "t"
            elif bl_flag and (runtime_count % itr_value != 0):
                flow_type = "backlog"
            elif runtime_count % rolling_window_batch == 0:
                flow_type = "rolling_window"

        # Clean up old records in temp_table
        delete_query = f"DELETE FROM idx_stage.temp_table WHERE source_id = {source_id} AND download_flag = 'f'"
        cursor_serverless.execute(delete_query)
        serverless_db_con = cursor_serverless.connection  # get connection from cursor
        serverless_db_con.commit()

        log_message = {
            "source_id": source_id,
            "source_name": source_name,
            "deleted_count": cursor_serverless.rowcount,
            "Query": delete_query,
        }
        logger.info(log_message)

        cnt, last_modified_date, latest_listing_date = get_total_count_from_temp(
            source_id, temp_respecs_flag, cursor_serverless
        )

        source_data["temp_table_status"] = (cnt == 0) or (
            bl_flag and flow_type in ["lmd", "rolling_window"]
        )

        source_data["last_modification_date"] = (
            last_modified_date  # min date from temp_table, when temp_table is not empty
        )

        if source_data["temp_table_status"]:
            if flow_type == "sold":
                last_modified_date, sold_date = get_lmd(
                    source_id,
                    cursor_serverless,
                    cursor_homelisting,
                    flow_type,
                    rolling_window_offset,
                )
                source_data["sold_date"] = sold_date
            else:
                last_modified_date = get_lmd(
                    source_id,
                    cursor_serverless,
                    cursor_homelisting,
                    flow_type,
                    rolling_window_offset,
                )
            source_data["last_modification_date"] = (
                last_modified_date  # from serverless_idx_loads when temp_table is empty, will download in temp_table after that date
            )
            request_and_load_temp_table(
                source_data, flow_type, cursor_serverless, serverless_db_con
            )
            cnt, last_modified_date, latest_listing_date = get_total_count_from_temp(
                source_id, temp_respecs_flag, cursor_serverless
            )

        if source_data["temp_table_status"] and temp_respecs_flag == "t":
            # get latest_listing_date for respecs_finish_date  (only for API sources)
            query = f""" select max(modification_timestamp::timestamp) from listing_p_active where source_id = {source_id}; """
            cursor_homelisting.execute(query)
            latest_listing_date = cursor_homelisting.fetchone()[0]
        else:
            latest_listing_date = (
                latest_listing_date
                if latest_listing_date and latest_listing_date > last_modified_date
                else last_modified_date
            )

        source_data["row_count"] = cnt
        source_data["latest_listing_date"] = str(latest_listing_date)
        source_data["download_flag"] = True
        source_data["flow_type"] = flow_type

        return source_data
    except Exception as e:
        log_msg = {"Error": str(e)}
        logger.error(log_msg)
        raise


def chunks_download_from_source(data_dict):
    """
    Downloads full Property data for a chunk of ListingKeys.
    """
    download_data = []
    source_name = data_dict["source_name"]
    source_id = data_dict["source_id"]
    headers = data_dict["headers"]
    chunk = data_dict["values_list"]
    class_name = data_dict["class_name"]
    url_endpoint = data_dict["api_endpoint"]
    batch_id = data_dict["batch_id"]
    batch_creation_date = data_dict["batch_creation_date"]
    source_type = data_dict["source_type"]
    source_info = data_dict["source_info"]
    expand_classes = data_dict["expand_classes"]

    keys = ", ".join(f"'{k}'" for k in chunk)
    params = {
        "$filter": f"ListingKey in ({keys})",
        "$expand": expand_classes,
        "$count": "true",
        "$top": 200,
    }

    try:
        resp = requests.get(url_endpoint, headers=headers, params=params)
        resp.raise_for_status()
    except Exception:
        time.sleep(10)
        resp = requests.get(url_endpoint, headers=headers, params=params)
        resp.raise_for_status()

    data = resp.json()
    values = data.get("value", [])
    download_data.extend(values)

    df_page = pd.DataFrame(values)
    df_page = adding_extra_columns(df_page, batch_creation_date, source_id, batch_id)
    # Upload_data_into_S3_DataLake(
    #     df_page, source_id, source_type, source_name, batch_id, class_name, chunk[0][:8]
    # )

    return download_data


def db_loader(
    trestle_list_data,
    classes,
    batch_creation_date,
    batch_id,
    source_id,
    source_type,
    source_name,
    cursor,
    connection,
):
    empty_list = {}
    office_frames = []
    member_frames = []

    for class_name in classes:
        empty_list[class_name] = []

    for val in trestle_list_data:
        # Merge CustomProperty/CustomFields
        if "CustomProperty" in val and val["CustomProperty"] is not None:
            for cp in val["CustomProperty"]:
                if "CustomFields" in cp and cp["CustomFields"] is not None:
                    custom_fields = cp.pop("CustomFields")
                    if isinstance(custom_fields, dict):
                        cp.update(custom_fields)
                    elif isinstance(custom_fields, str):
                        try:
                            parsed = json.loads(custom_fields)
                            if isinstance(parsed, dict):
                                cp.update(parsed)
                        except json.JSONDecodeError:
                            pass
                    else:
                        cp.update({})

        filtered_dict = {k: v for k, v in val.items() if k not in classes[1:]}
        if "CustomProperty" in val and val["CustomProperty"] is not None:
            filtered_dict["CustomProperty"] = val["CustomProperty"]
        empty_list[classes[0]].append(filtered_dict)

        for i in range(1, len(classes)):
            if val.get(classes[i]) is not None:
                for record in val[classes[i]]:
                    empty_list[classes[i]].append(record)

    for dfo in classes:
        df_instance = pd.DataFrame(empty_list[dfo])
        if df_instance.empty:
            continue
        df_instance.columns = df_instance.columns.str.lower()
        df_instance = df_instance.astype(str)
        df_instance = df_instance[sorted(df_instance.columns)]

        if dfo.lower() == "media":
            df_instance = df_instance.drop(
                "originatingsystemresourcerecordkey", axis=1, errors="ignore"
            )
            Upload_data_into_S3_DataLake(
                df_instance, source_id, source_type, source_name, batch_id, dfo
            )
        if dfo.lower() == "property":
            df_instance = df_instance.drop("unittypes", axis=1, errors="ignore")

        df_instance = adding_extra_columns(
            df_instance, batch_creation_date, source_id, batch_id
        )

        if dfo in ["ListOffice", "CoListOffice", "BuyerOffice", "CoBuyerOffice"]:
            office_frames.append(df_instance)
            table_creation_and_loading(
                df_instance,
                "ps_trestle_office",
                source_id,
                source_name,
                cursor,
                connection,
            )
        elif dfo in ["ListAgent", "CoListAgent", "BuyerAgent", "CoBuyerAgent"]:
            member_frames.append(df_instance)
            table_creation_and_loading(
                df_instance,
                "ps_trestle_member",
                source_id,
                source_name,
                cursor,
                connection,
            )
        elif dfo == "Rooms":
            Upload_data_into_S3_DataLake(
                df_instance,
                source_id,
                source_type,
                source_name,
                batch_id,
                "PropertyRooms",
            )
            table_creation_and_loading(
                df_instance,
                "ps_trestle_propertyrooms",
                source_id,
                source_name,
                cursor,
                connection,
            )
        elif dfo == "UnitTypes":
            Upload_data_into_S3_DataLake(
                df_instance,
                source_id,
                source_type,
                source_name,
                batch_id,
                "PropertyUnitTypes",
            )
            table_creation_and_loading(
                df_instance,
                "ps_trestle_propertyunittypes",
                source_id,
                source_name,
                cursor,
                connection,
            )
        elif dfo == "CustomProperty":
            Upload_data_into_S3_DataLake(
                df_instance,
                source_id,
                source_type,
                source_name,
                batch_id,
                "CustomProperty",
            )
            table_creation_and_loading(
                df_instance,
                "ps_trestle_customproperty",
                source_id,
                source_name,
                cursor,
                connection,
            )
        else:
            table_creation_and_loading(
                df_instance,
                f"ps_trestle_{dfo.lower()}",
                source_id,
                source_name,
                cursor,
                connection,
            )
    if office_frames:
        office_df = pd.concat(office_frames, ignore_index=True)
        Upload_data_into_S3_DataLake(
            office_df, source_id, source_type, source_name, batch_id, "Office"
        )
    if member_frames:
        member_df = pd.concat(member_frames, ignore_index=True)
        Upload_data_into_S3_DataLake(
            member_df, source_id, source_type, source_name, batch_id, "Member"
        )


def api_call_and_load_tables(source_data, token, connection, cursor):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    flow_type = source_data["flow_type"]
    last_modification_date = source_data["last_modification_date"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_type = source_info["source_type"]
    originating_system_name = source_info["originating_system_name"]
    limit = source_info["limit"]
    run_host = source_data["run_host"]
    mls_board = source_info["mls_board"]
    expand_classes = source_info["expand_classes"]
    download_classes = source_info["download_classes"]
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params.get("bl_flag", False)
    base_url = source_data["auth"]["request_url"]

    classes = [c.strip() for c in download_classes.split(",")]
    property_index = classes.index("Property")

    logger.info(f"Classes before insert: {classes} (type: {type(classes)})")
    classes.insert(0, classes.pop(property_index))

    headers = {"Authorization": f"Bearer {token}"}

    # Fetch listing keys from temp_table
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
        distinct on ({orderby_column}, listingkey)
        listingkey as listingkey
        from idx_stage.temp_table
        where source_id ={source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}'
        order by {orderby_column}::timestamp {orderby_type}, listingkey
        Limit {limit};
        """
    cursor.execute(query)
    keys = [row[0] for row in cursor.fetchall()]
    if not keys:
        logger.warning(f"No pending listing keys in temp_table for source {source_id}")
        return True

    # Break into chunks of 50
    chunk_size = 50
    chunks = [keys[i : i + chunk_size] for i in range(0, len(keys), chunk_size)]

    req_metadata = pd.DataFrame(
        [
            {
                # "source_id": source_id,
                "source_name": source_name,
                "creation_date": datetime.now(timezone.utc),
                "source_count": len(keys),
                "request_url_endpoint": base_url,
                "params": {"chunk_size": 50, "limit": limit, "flow_type": flow_type},
            }
        ]
    )
    req_metadata = adding_extra_columns(
        req_metadata, batch_creation_date, source_id, batch_id
    )
    Upload_data_into_S3_DataLake(
        req_metadata,
        source_id,
        source_type,
        source_name,
        batch_id,
        "Property",
        "Request",
    )

    # Download Property data with $expand for each chunk
    trestle_list_data = []
    for chunk in chunks:
        data_dict = {
            "headers": headers,
            "values_list": chunk,
            "class_name": "Property",
            "api_endpoint": base_url,
            "source_id": source_id,
            "source_name": source_name,
            "source_type": source_type,
            "batch_id": batch_id,
            "batch_creation_date": batch_creation_date,
            "expand_classes": expand_classes,
            "source_info": source_info,
        }
        chunk_data = chunks_download_from_source(data_dict)
        trestle_list_data.extend(chunk_data)

    if trestle_list_data:
        df_upload = pd.DataFrame(trestle_list_data)  # already flattened records
        df_upload = adding_extra_columns(
            df_upload, batch_creation_date, source_id, batch_id
        )
        Upload_data_into_S3_DataLake(
            df_upload, source_id, source_type, source_name, batch_id, "Property"
        )

    if not trestle_list_data:
        return True

    db_loader(
        trestle_list_data,
        classes,
        batch_creation_date,
        batch_id,
        source_id,
        source_type,
        source_name,
        cursor,
        connection,
    )

    # Download OpenHouse
    openhouse_url = base_url.replace("Property", "OpenHouse")
    current_date = datetime.now().strftime("%Y-%m-%d")
    openhouse_data = []
    skip = 0
    top = 200
    o_params = {
        "$filter": f"OriginatingSystemName eq {originating_system_name} and OpenHouseDate ge {current_date}",
        "$count": "true",
        "$top": top,
        "$orderby": "OpenHouseDate asc",
    }
    openhouse_upload_frames = []
    while skip < limit:
        o_params["$skip"] = skip
        try:
            resp = requests.get(openhouse_url, headers=headers, params=o_params)
            resp.raise_for_status()
        except Exception:
            time.sleep(10)
            resp = requests.get(openhouse_url, headers=headers, params=o_params)
            resp.raise_for_status()
        data = resp.json()
        total_count = data["@odata.count"]
        openhouse_data.extend(data["value"])

        # df_oh = pd.DataFrame(data)
        # df_oh = adding_extra_columns(df_oh, batch_creation_date, source_id, batch_id)
        # Upload_data_into_S3_DataLake(
        #     df_oh, source_id, source_type, source_name, batch_id, "OpenHouse", skip
        # )
        openhouse_upload_frames.extend(
            [
                {**record, "url_endpoint": openhouse_url, **o_params}
                for record in data["value"]
            ]
        )

        if skip + top >= total_count:
            break
        skip += top

    if openhouse_upload_frames:
        df_oh = pd.DataFrame(openhouse_upload_frames)
        df_oh = adding_extra_columns(df_oh, batch_creation_date, source_id, batch_id)
        Upload_data_into_S3_DataLake(
            df_oh, source_id, source_type, source_name, batch_id, "OpenHouse"
        )

    if openhouse_data:
        openhouse_df = pd.DataFrame(openhouse_data)
        openhouse_df = adding_extra_columns(
            openhouse_df, batch_creation_date, source_id, batch_id
        )
        table_creation_and_loading(
            openhouse_df,
            "ps_trestle_openhouse",
            source_id,
            source_name,
            cursor,
            connection,
        )

    return True


def download_func(source_data, token, connection, cursor):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    flow_type = source_data["flow_type"]
    last_modification_date = source_data["last_modification_date"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_type = source_info["source_type"]
    originating_system_name = source_info["originating_system_name"]
    limit = source_info["limit"]
    run_host = source_data["run_host"]
    mls_board = source_info["mls_board"]
    expand_classes = source_info["expand_classes"]
    download_classes = source_info["download_classes"]
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params.get("bl_flag", False)

    try:
        status = api_call_and_load_tables(source_data, token, connection, cursor)
        response = {
            "source_id": source_data["source_id"],
            "source_name": source_data["source_name"],
            "batch_id": source_data["batch_id"],
            "mls_board": source_info["mls_board"],
            "source_type": source_info["source_type"],
            "batch_creation_date": source_data["batch_creation_date"],
            "last_refresh_date": source_data["last_modification_date"],
            "flow_type": source_data["flow_type"],
            "run_host": source_data["run_host"],
            "limit": source_info["limit"],
            "temp_table_status": source_data["temp_table_status"],
            "bl_flag": source_data["batch_execution_params"]["bl_flag"],
            "status": status,
            "success": False,
        }
        return response
    except Exception as e:
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data


def lambda_handler(event, context):
    logger.info(event)

    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    sqlExecLimit = os.environ.get("sqlExecLimit")
    db_secret_dev = fetch_secrets(rdsDatabase)
    db_secret_stage = fetch_secrets(listingDatabase)
    serverless_db_con = setup_db_connection(db_secret_dev, sqlExecLimit)
    homelisting_db_con = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    cursor_homelisting = homelisting_db_con.cursor()

    source_data = event
    final_response = {}
    try:
        download_flag = source_data["download_flag"]
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        client_id = source_data["auth"]["user"]
        client_secret = source_data["auth"]["password"]
        token = create_token(source_id, source_name, client_id, client_secret)

        if download_flag:
            final_response = download_func(
                source_data, token, serverless_db_con, cursor_serverless
            )
        else:
            final_response = validation_func(
                source_data, token, cursor_serverless, cursor_homelisting
            )

    except Exception as e:
        log_msg = {"Error": str(e), "Error At Line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data

    finally:
        cursor_serverless.close()
        cursor_homelisting.close()
        if serverless_db_con:
            serverless_db_con.close()
        if homelisting_db_con:
            homelisting_db_con.close()

    if not final_response.get("download_flag", True):
        raise Exception(final_response)
    return final_response
