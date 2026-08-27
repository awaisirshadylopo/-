# Paragon API Active Data Download Lambda (temp_table)

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

logger = logging.getLogger("Paragon-Active-Lambda")
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def setup_db_connection(db_secret, sqlExecLimit):
    db_username = db_secret.get("username")
    db_password = db_secret.get("password")
    db_host = db_secret.get("host")
    db_name = db_secret.get("dbname")
    db_port = db_secret.get("port")
    conn = psycopg2.connect(
        database=db_name,
        user=db_username,
        password=db_password,
        host=db_host,
        port=db_port,
        options=f"-c statement_timeout={sqlExecLimit}",
    )
    return conn


def create_token(url, client_id, client_secret):
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "OData",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(url, headers=headers, data=data)
    if resp.status_code == 200:
        return resp.json().get("access_token")
    else:
        logger.error({"message": "token generation failed", "response": resp.text})
        raise Exception("Token generation failed")


def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


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


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value


def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    new_cols = pd.DataFrame(
        {
            "source_id": int(source_id),
            "batch_id": int(batch_id),
            "source_last_update_date": formatted_datetime,
            "y_creation_date": batch_creation_date,
            "y_last_update_date": batch_creation_date,
            "source_creation_date": formatted_datetime,
        },
        index=generic_df.index,
    )

    return pd.concat([new_cols, generic_df], axis=1)


def get_max_last_modified_date(
    source_id,
    cursor_serverless,
    cursor_homelisting,
    flow_type,
    rolling_window_offset=None,
):

    if flow_type == "respecs":
        query = f"SELECT respecs_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "backlog":
        query = f"SELECT bl_start_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "full_load":
        query = f"SELECT full_load_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "rolling_window":
        query = f"SELECT last_modified_date::timestamp(0) - interval '{rolling_window_offset} hours' FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
    elif flow_type == "sold":
        query = f"SELECT last_modified_date, batch_id, sold_date FROM stage.serverless_idx_loads WHERE source_id = {source_id}"
        cursor_serverless.execute(query)
        data = cursor_serverless.fetchall()
        df = pd.DataFrame(data, columns=["last_modified_date", "batch_id", "sold_date"])
        currect_lmd = df["last_modified_date"][0]
        previous_batch_id = df["batch_id"][0]
        sold_date = df["sold_date"][0]

        query = f"SELECT max(modification_timestamp)::timestamp FROM listing WHERE source_id = {source_id} AND batch_id = {previous_batch_id}"
        cursor_homelisting.execute(query)
        data = cursor_homelisting.fetchall()
        previous_lmd = pd.DataFrame(data, columns=["max_modification_timestamp"])[
            "max_modification_timestamp"
        ][0]

        if currect_lmd == previous_lmd:
            modification_timestamp = "1990-01-01 00:00:00"
        else:
            query = f"SELECT min(sold_date), max(sold_date) FROM listing_p_sold WHERE source_id = {source_id} AND batch_id = {previous_batch_id}"
            cursor_homelisting.execute(query)
            data = cursor_homelisting.fetchall()
            df2 = pd.DataFrame(data, columns=["min_sold_date", "max_sold_date"])
            if df2["min_sold_date"][0] == df2["max_sold_date"][0]:
                query = f"SELECT max(modification_timestamp)::timestamp FROM listing_p_sold WHERE source_id = {source_id} AND batch_id = {previous_batch_id}"
                cursor_homelisting.execute(query)
                modification_timestamp = cursor_homelisting.fetchone()[0]
            else:
                modification_timestamp = "1990-01-01 00:00:00"
        return formatted_date(modification_timestamp), str(sold_date)
    else:
        query = f"SELECT last_modified_date::timestamp(0) FROM stage.serverless_idx_loads WHERE source_id = {source_id}"

    cursor_serverless.execute(query)
    result = cursor_serverless.fetchone()
    return (
        formatted_date(result[0]) if result else formatted_date("1990-01-01 00:00:00")
    )


def chunks_creation(df, key_column_names, chunk_size):

    value_keys = []
    key_column_names = key_column_names.strip("[]").replace("'", "").split(",")
    for column in key_column_names:
        if column in df.columns:
            value_keys.extend(df[column].dropna().tolist())
    filtered_keys = [key for key in value_keys if key != ""]
    unique_keys = pd.Series(filtered_keys, dtype=object).unique().tolist()
    return [
        unique_keys[i : i + chunk_size] for i in range(0, len(unique_keys), chunk_size)
    ]


def table_creation_and_loading(
    list_data,
    source_id,
    table_name,
    source_name,
    batch_id,
    batch_creation_date,
    cursor,
    connection,
    resource_name,
):

    resource_df = pd.DataFrame(list_data)
    resource_df = resource_df.drop(
        columns=["@odata.id", "Media", "PropertyRooms"], errors="ignore"
    )
    resource_df = adding_extra_columns(
        resource_df, batch_creation_date, source_id, batch_id
    )
    resource_df = resource_df.apply(lambda col: col.map(remove_characters))
    resource_df = resource_df.apply(lambda col: col.map(clean_value))
    resource_df.drop_duplicates(inplace=True)

    resource_df.columns = resource_df.columns.str.lower()

    metadata_query = f"""
        SELECT DISTINCT LOWER(long_name) as column_name
        FROM dev.field_metadata
        WHERE source_id = {source_id}
          AND resource_name = '{resource_name}'
          AND download_flag = 't'
    """
    cursor.execute(metadata_query)
    downloadable_columns = {row[0] for row in cursor.fetchall()}

    system_columns = {
        "source_id",
        "batch_id",
        "source_creation_date",
        "y_last_update_date",
        "y_creation_date",
        "source_last_update_date",
    }
    downloadable_columns.update(system_columns)

    if downloadable_columns:
        existing_downloadable = downloadable_columns.intersection(
            set(resource_df.columns)
        )

        if existing_downloadable.issubset(resource_df.columns):
            resource_df = resource_df[list(existing_downloadable)]
        else:

            missing = existing_downloadable - set(resource_df.columns)
            if missing:
                logger.warning(
                    {
                        "source_id": source_id,
                        "resource_name": resource_name,
                        "message": f"Some downloadable columns not in API response: {missing}",
                    }
                )

    column_names = f"""SELECT column_name FROM information_schema.columns
                       WHERE table_name = '{table_name}' AND column_name NOT IN ('pid')"""
    full_table = f"idx_stage.{table_name}"
    cursor.execute(column_names)
    table_column_names = [col[0] for col in cursor.fetchall()]

    df_cols = list(resource_df.columns)
    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)

    if extra_cols:
        for n in extra_cols:
            alter_query = f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS {n} TEXT"
            cursor.execute(alter_query)
            insert_query = f"""INSERT INTO dev.field_metadata
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name, download_flag)
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}', 't')"""
            cursor.execute(insert_query)
            logger.info(
                {
                    "source_id": source_id,
                    "table_name": full_table,
                    "class_name": resource_name,
                    "message": f"Added new column {n} with download_flag=true",
                }
            )
            connection.commit()

    cols = ",".join(df_cols)
    data_values = [tuple(row) for row in resource_df.values]
    insert_query = f"""INSERT INTO {full_table} ({cols}) VALUES %s"""
    insert_query = insert_query.replace("order", '"order"')
    extras.execute_values(cursor, insert_query, data_values)
    connection.commit()

    logger.info(
        {
            "source_id": source_id,
            "table_name": full_table,
            "rows_inserted": len(data_values),
        }
    )


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, resource_name, skip=None
):

    filename = f"{source_name}_{resource_name}.parquet"
    if skip is not None:
        filename = f"{source_name}_{resource_name}_{skip}.parquet"
    # folder_path = f"{source_type}/{source_id}_{source_name}/{batch_id}/{resource_name}/" --old path
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

    logger.info(
        {
            "source_id": source_id,
            "resource_name": resource_name,
            "s3_key": s3_key,
            "rows_uploaded": len(df_upload),
        }
    )


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
    cnt, min_ts, max_ts = cursor_serverless.fetchone()
    return int(cnt), formatted_date(min_ts), formatted_date(max_ts)


def request_and_load_temp_table(source_data, flow_type, cursor_serverless, serverless_db_con):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    auth = source_data["auth"]
    loginurl = auth["loginUrl"]
    token = source_data["auth"]["token"]
    cdc_column = source_info["cdc_column"]
    status_column = source_info["status_column"]
    active_status = source_info["active_status"]
    sold_status = source_info["sold_status"]
    sold_column = source_info["sold_column"]
    last_modification_date = source_data["last_modification_date"]
    source_type = source_info["source_type"]
    is_rels = "paragonrels" in loginurl.lower()
    sold_column_filter_flag = source_info.get("sold_column_filter_flag", True)
    reference_key = source_info.get("reference_key", "ListingKey")

    photo_col = "PhotosChangeTimestamp" if not is_rels else "PhotoTimestamp"
    headers = {"Authorization": f"Bearer {token}"}
    base_url = loginurl.replace("$metadata", "Property")
    top = 200
    skip = 0

    filter_str = f"{cdc_column} ge {last_modification_date} "

    select_fields = (
        f"{reference_key},{cdc_column},{photo_col}"
        if flow_type in ["full_load", "respecs"] or (not sold_column_filter_flag)
        else f"{reference_key},{cdc_column},{photo_col},{sold_column}"
    )
    orderby = (
        f"{sold_column} asc, {cdc_column} asc"
        if flow_type == "sold" and sold_column_filter_flag
        else f"{cdc_column} asc"
    )

    if flow_type == "sold":
        sold_date = source_data.get("sold_date")
        if is_rels:
            status_list = [s.strip().strip("'") for s in sold_status.split(",")]
            status_filter = " or ".join(
                f"{status_column} eq '{s}'" for s in status_list
            )
            status_filter = f"and ({status_filter})"
        else:
            status_filter = f"and {status_column} eq {sold_status} "

        if sold_column_filter_flag:
            filter_str = (
                f"{filter_str}" f"{status_filter}" f"and {sold_column} ge {sold_date}"
            )
        else:
            filter_str = f"{filter_str}" f"{status_filter}"
    elif flow_type in ["full_load", "respecs"]:
        if is_rels:
            status_list = [s.strip().strip("'") for s in active_status.split(",")]
            status_filter = " or ".join(
                f"{status_column} eq '{s}'" for s in status_list
            )
            status_filter = f"and ({status_filter})"
        else:
            status_filter = f"and {status_column} in ({active_status})"
        filter_str = f"{filter_str}" f"{status_filter}"
    else:
        filter_str = f"({filter_str}or {photo_col} ge {last_modification_date}) "

    params = {
        "$filter": (
            filter_str + " and Sale_Or_Rent ne 'R'"
            if is_rels
            else filter_str
            + " and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"
        ),
        "$orderby": orderby,
        "$count": "true",
        "$top": top,
        "$select": select_fields,
    }

    data_list = []
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
                source_type,
                source_name,
                f"{last_modification_date[:10]}_temp",
                "Property",
                "Request",
            )

        if skip + top >= total_count or skip >= 10000:
            break
        skip += top

    if not data_list:
        return False

    temp_df = pd.DataFrame(data_list)
    Upload_data_into_S3_DataLake(
            temp_df,
            source_id,
            source_type,
            source_name,
            f"{last_modification_date[:10]}_temp",
            "Property",
        )


    if flow_type in ["full_load", "respecs"] or (not sold_column_filter_flag):
        rename_map = {
            reference_key: "listingkey",
            cdc_column: "modification_timestamp",
            photo_col: "media_modification_timestamp",
        }
        cols_to_keep = [reference_key, cdc_column, photo_col]
    else:
        rename_map = {
            reference_key: "listingkey",
            cdc_column: "modification_timestamp",
            photo_col: "media_modification_timestamp",
            sold_column: "sold_date",
        }
        cols_to_keep = [reference_key, cdc_column, photo_col, sold_column]

    temp_df = temp_df[cols_to_keep]
    temp_df.rename(columns=rename_map, inplace=True)

    temp_df["source_id"] = source_id
    temp_df["respecs_flag"] = "t" if flow_type == "respecs" else "f"

    col_check = """SELECT column_name FROM information_schema.columns 
                   WHERE table_name = 'temp_table'"""
    cursor_serverless.execute(col_check)
    existing_cols = {row[0] for row in cursor_serverless.fetchall()}

    temp_df.columns = temp_df.columns.str.lower()
    needed_cols = list(temp_df.columns)

    for col in needed_cols:
        if col not in existing_cols:
            alter_query = (
                f"ALTER TABLE idx_stage.temp_table ADD COLUMN IF NOT EXISTS {col} TEXT"
            )
            cursor_serverless.execute(alter_query)
            serverless_db_con.commit()

    cols = ",".join(needed_cols)
    data_values = [tuple(row) for row in temp_df.values]
    insert_query = f"INSERT INTO idx_stage.temp_table ({cols}) VALUES %s"
    extras.execute_values(cursor_serverless, insert_query, data_values)
    serverless_db_con.commit()

    logger.info(
        {
            "source_id": source_id,
            "table": "idx_stage.temp_table",
            "rows_inserted": len(data_values),
        }
    )

    return True


def _fetch_chunk(resource, key_column, keys, headers, loginurl):
    """Fetch a single resource for a list of key values."""
    url = loginurl.replace("$metadata", resource)
    key_str = ",".join(f"'{k}'" for k in keys)
    params = {"$filter": f"{key_column} in ({key_str})", "$top": 200}
    try:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json().get("value", [])
    except Exception as e:
        logger.error({"resource": resource, "chunk_fetch_error": str(e)})
        return []


def _upload_and_load_class(
    class_name,
    data_list,
    source_data,
    batch_creation_date,
    source_type,
    connection,
    cursor,
):

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    batch_id = source_data["batch_id"]

    table_name = (
        f"ps_paragon_{class_name.lower()}_{source_id}"
        if class_name not in ("Media",)
        else "ps_paragon_media"
    )
    table_creation_and_loading(
        data_list,
        source_id,
        table_name,
        source_name,
        batch_id,
        batch_creation_date,
        cursor,
        connection,
        class_name,
    )
    # S3 upload
    df = pd.DataFrame(data_list)
    df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
    Upload_data_into_S3_DataLake(
        df, source_id, source_type, source_name, batch_id, class_name
    )


def _build_s3_df(
    data_list,
    s3_url_list,
    s3_filter_list,
    batch_creation_date,
    source_id,
    batch_id,
    drop_cols=None,
):
    df_s3 = pd.DataFrame(data_list).copy()
    if drop_cols:
        df_s3 = df_s3.drop(columns=drop_cols, errors="ignore")
    for col in df_s3.columns:
        df_s3[col] = df_s3[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )
    df_s3 = adding_extra_columns(df_s3, batch_creation_date, source_id, batch_id)
    df_s3 = df_s3.reset_index(drop=True)
    df_s3["request_url_endpoint"] = s3_url_list
    df_s3["request_filter"] = s3_filter_list
    context_cols = ["request_url_endpoint", "request_filter"]
    business_cols = [c for c in df_s3.columns if c not in context_cols]
    df_s3.drop_duplicates(subset=business_cols, inplace=True)
    return df_s3


def _paragon_load(
    source_data,
    property_list,
    headers,
    loginurl,
    batch_creation_date,
    source_type,
    connection,
    cursor,
):

    source_id = source_data["source_id"]
    source_info = source_data["source_info"]
    reference_key = source_info.get("reference_key", "ListingKey")
    office_mlsid_columns = source_info.get("office_mlsid_columns")
    agent_mlsid_columns = source_info.get("agent_mlsid_columns")
    office_key_columns = source_info.get("office_key_columns")
    agent_key_columns = source_info.get("agent_key_columns")
    chunk_size = 10

    property_df = pd.DataFrame(property_list)

    # ── Media ──
    media_list = []
    for p in property_list:
        if p.get("Media"):
            media_list.extend(p["Media"])
    if media_list:
        _upload_and_load_class(
            "Media",
            media_list,
            source_data,
            batch_creation_date,
            source_type,
            connection,
            cursor,
        )

    # ── Office ──
    office_mlsid_chunks = chunks_creation(property_df, office_mlsid_columns, chunk_size)
    office_key_chunks = chunks_creation(property_df, office_key_columns, chunk_size)
    office_list = []
    for chunk in office_key_chunks:
        office_list.extend(
            _fetch_chunk("Office", "OfficeKey", chunk, headers, loginurl)
        )
    for chunk in office_mlsid_chunks:
        office_list.extend(
            _fetch_chunk("Office", "OfficeMlsId", chunk, headers, loginurl)
        )
    if office_list:
        _upload_and_load_class(
            "Office",
            office_list,
            source_data,
            batch_creation_date,
            source_type,
            connection,
            cursor,
        )

    # ── Member ──
    agent_key_chunks = chunks_creation(property_df, agent_key_columns, chunk_size)
    agent_mlsid_chunks = chunks_creation(property_df, agent_mlsid_columns, chunk_size)
    member_list = []
    for chunk in agent_key_chunks:
        member_list.extend(
            _fetch_chunk("Member", "MemberKey", chunk, headers, loginurl)
        )
    for chunk in agent_mlsid_chunks:
        member_list.extend(
            _fetch_chunk("Member", "MemberMlsId", chunk, headers, loginurl)
        )
    if member_list:
        _upload_and_load_class(
            "Member",
            member_list,
            source_data,
            batch_creation_date,
            source_type,
            connection,
            cursor,
        )

    # ── OpenHouse ──
    openhouse_key_chunks = chunks_creation(property_df, reference_key, chunk_size)
    openhouse_list = []
    for chunk in openhouse_key_chunks:
        openhouse_list.extend(
            _fetch_chunk("OpenHouse", reference_key, chunk, headers, loginurl)
        )
    if openhouse_list:
        _upload_and_load_class(
            "OpenHouse",
            openhouse_list,
            source_data,
            batch_creation_date,
            source_type,
            connection,
            cursor,
        )

    return True


def _paragonrels_load(
    source_data,
    property_list,
    headers,
    loginurl,
    batch_creation_date,
    source_type,
    connection,
    cursor,
):
    """Given an already-downloaded property_list, load Media/OpenHouse/Office/Agent."""
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    batch_id = source_data["batch_id"]
    reference_key = source_info.get("reference_key", "ListingKey")
    office_identifier_columns = source_info.get("office_identifier_columns")
    agent_identifier_columns = source_info.get("agent_identifier_columns")
    chunk_size = 10
    top = 200

    if not property_list:
        return True

    property_df = pd.DataFrame(property_list)
    media_openhouse_key_chunks = chunks_creation(property_df, reference_key, chunk_size)
    office_identifier_chunks = chunks_creation(
        property_df, office_identifier_columns, chunk_size
    )
    agent_identifier_chunks = chunks_creation(
        property_df, agent_identifier_columns, chunk_size
    )

    # Media
    media_list = []
    media_s3_url = []
    media_s3_filter = []
    for chunk in media_openhouse_key_chunks:
        int_keys = [int(k) for k in chunk]
        value = " or ".join([f"{reference_key} eq {k}" for k in int_keys])
        params = {"$filter": value, "$top": top}
        cur_filter = str(value)
        try:
            resp = requests.get(
                loginurl.replace("$metadata", "Media"), headers=headers, params=params
            )
            resp.raise_for_status()
            for item in resp.json().get("value", []):
                media_list.append(item)
                media_s3_url.append(loginurl.replace("$metadata", "Media"))
                media_s3_filter.append(cur_filter)
        except Exception as e:
            logger.error({"source_id": source_id, "resource": "Media", "error": str(e)})
            return False

    if media_list:
        try:
            m_s3 = _build_s3_df(
                media_list,
                media_s3_url,
                media_s3_filter,
                batch_creation_date,
                source_id,
                batch_id,
                drop_cols=["@odata.id", "Media", "PropertyRooms"],
            )
            Upload_data_into_S3_DataLake(
                m_s3, source_id, source_type, source_name, batch_id, "Media"
            )
            del m_s3
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "Media", "s3_error": str(e)}
            )
        table_creation_and_loading(
            media_list,
            source_id,
            "ps_paragon_media",
            source_name,
            batch_id,
            batch_creation_date,
            cursor,
            connection,
            "Media",
        )

    # OpenHouse
    openhouse_list = []
    openhouse_s3_url = []
    openhouse_s3_filter = []
    for chunk in media_openhouse_key_chunks:
        int_keys = [int(k) for k in chunk]
        value = " or ".join([f"{reference_key} eq {k}" for k in int_keys])
        params = {"$filter": value, "$top": top}
        cur_filter = str(value)
        try:
            resp = requests.get(
                loginurl.replace("$metadata", "OpenHouse"),
                headers=headers,
                params=params,
            )
            resp.raise_for_status()
            for item in resp.json().get("value", []):
                openhouse_list.append(item)
                openhouse_s3_url.append(loginurl.replace("$metadata", "OpenHouse"))
                openhouse_s3_filter.append(cur_filter)
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "OpenHouse", "error": str(e)}
            )
            return False

    if openhouse_list:
        try:
            oh_s3 = _build_s3_df(
                openhouse_list,
                openhouse_s3_url,
                openhouse_s3_filter,
                batch_creation_date,
                source_id,
                batch_id,
                drop_cols=["@odata.id", "Media", "PropertyRooms"],
            )
            Upload_data_into_S3_DataLake(
                oh_s3, source_id, source_type, source_name, batch_id, "OpenHouse"
            )
            del oh_s3
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "OpenHouse", "s3_error": str(e)}
            )
        table_creation_and_loading(
            openhouse_list,
            source_id,
            f"ps_paragon_openhouse_{source_id}",
            source_name,
            batch_id,
            batch_creation_date,
            cursor,
            connection,
            "OpenHouse",
        )

    # Office
    office_list = []
    office_s3_url = []
    office_s3_filter = []
    for chunk in office_identifier_chunks:
        int_keys = [int(k) for k in chunk]
        value = " or ".join([f"Office_Identifier eq {k}" for k in int_keys])
        params = {"$filter": value, "$top": top}
        cur_filter = str(value)
        try:
            resp = requests.get(
                loginurl.replace("$metadata", "Office"), headers=headers, params=params
            )
            resp.raise_for_status()
            for item in resp.json().get("value", []):
                office_list.append(item)
                office_s3_url.append(loginurl.replace("$metadata", "Office"))
                office_s3_filter.append(cur_filter)
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "Office", "error": str(e)}
            )
            return False

    if office_list:
        try:
            o_s3 = _build_s3_df(
                office_list,
                office_s3_url,
                office_s3_filter,
                batch_creation_date,
                source_id,
                batch_id,
                drop_cols=["@odata.id", "Media", "PropertyRooms"],
            )
            Upload_data_into_S3_DataLake(
                o_s3, source_id, source_type, source_name, batch_id, "Office"
            )
            del o_s3
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "Office", "s3_error": str(e)}
            )
        table_creation_and_loading(
            office_list,
            source_id,
            f"ps_paragon_office_{source_id}",
            source_name,
            batch_id,
            batch_creation_date,
            cursor,
            connection,
            "Office",
        )

    # Agent
    agent_list = []
    agent_s3_url = []
    agent_s3_filter = []
    for chunk in agent_identifier_chunks:
        int_keys = [int(k) for k in chunk]
        value = " or ".join([f"Agent_Identifier eq {k}" for k in int_keys])
        params = {"$filter": value, "$top": top}
        cur_filter = str(value)
        try:
            resp = requests.get(
                loginurl.replace("$metadata", "Agent"), headers=headers, params=params
            )
            resp.raise_for_status()
            for item in resp.json().get("value", []):
                agent_list.append(item)
                agent_s3_url.append(loginurl.replace("$metadata", "Agent"))
                agent_s3_filter.append(cur_filter)
        except Exception as e:
            logger.error({"source_id": source_id, "resource": "Agent", "error": str(e)})
            return False

    if agent_list:
        try:
            a_s3 = _build_s3_df(
                agent_list,
                agent_s3_url,
                agent_s3_filter,
                batch_creation_date,
                source_id,
                batch_id,
                drop_cols=["@odata.id", "Media", "PropertyRooms"],
            )
            Upload_data_into_S3_DataLake(
                a_s3, source_id, source_type, source_name, batch_id, "Agent"
            )
            del a_s3
        except Exception as e:
            logger.error(
                {"source_id": source_id, "resource": "Agent", "s3_error": str(e)}
            )
        table_creation_and_loading(
            agent_list,
            source_id,
            f"ps_paragon_agent_{source_id}",
            source_name,
            batch_id,
            batch_creation_date,
            cursor,
            connection,
            "Agent",
        )
    return True


def validation_func(source_data, cursor_serverless, cursor_homelisting):
    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        auth = source_data["auth"]
        loginurl = auth["loginUrl"]
        runtime_count = source_data["runtime_count"]
        flow_type = source_info["flow_type"]
        rolling_window_batch = source_info.get("rolling_window_batch", 10)
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params["bl_flag"]
        itr_value = batch_execution_params["itr_value"]
        respecs_flag = batch_execution_params["respecs_flag"]
        rolling_window_offset = source_info.get("rolling_window_offset", 1)
        temp_respecs_flag = "f"

        if "paragonrels" in loginurl.lower():
            client_id = auth["user"]
            client_secret = auth["password"]
            token_url = auth["tokenUrl"]
            token = create_token(token_url, client_id, client_secret)
        else:
            token = auth["password"]
        source_data["auth"]["token"] = token

        if flow_type not in ["sold", "full_load"]:
            if respecs_flag and (runtime_count % itr_value != 0):
                flow_type = "respecs"
                temp_respecs_flag = "t"
            elif bl_flag and (runtime_count % itr_value != 0):
                flow_type = "backlog"
            elif runtime_count % rolling_window_batch == 0:
                flow_type = "rolling_window"

        delete_query = f"DELETE FROM idx_stage.temp_table WHERE source_id = {source_id} AND download_flag = 'f'"
        cursor_serverless.execute(delete_query)
        serverless_db_con = cursor_serverless.connection
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
        temp_table_status = (cnt == 0) or (
            bl_flag and flow_type in ["lmd", "rolling_window"]
        )
        source_data["temp_table_status"] = temp_table_status
        source_data["last_modification_date"] = last_modified_date

        if temp_table_status:
            if flow_type == "sold":
                last_modified_date, sold_date = get_max_last_modified_date(
                    source_id,
                    cursor_serverless,
                    cursor_homelisting,
                    flow_type,
                    rolling_window_offset,
                )
                source_data["sold_date"] = sold_date
            else:
                last_modified_date = get_max_last_modified_date(
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

        if temp_table_status and temp_respecs_flag == "t":
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

        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "flow_type": flow_type,
            "last_modification_date": last_modified_date,
            "latest_listing_date": latest_listing_date,
            "row_count": cnt,
            "download_flag": source_data["download_flag"],
            "temp_table_status": temp_table_status,
        }
        logger.info(log_msg)

        return source_data
    except Exception as e:
        log_msg = {"Error": str(e)}
        logger.error(log_msg)
        raise


def download_func(source_data, cursor, connection):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    auth = source_data["auth"]
    flow_type = source_info["flow_type"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_type = source_info["source_type"]
    limit = source_info["limit"]
    run_host = source_data["run_host"]
    mls_board = source_info["mls_board"]
    reference_key = source_info.get("reference_key", "ListingKey")
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params.get("bl_flag", False)
    temp_table_status = source_data["temp_table_status"]
    is_rels = False

    loginurl = auth["loginUrl"]
    if "paragonrels" in loginurl.lower():
        client_id = auth["user"]
        client_secret = auth["password"]
        token_url = auth["tokenUrl"]
        token = create_token(token_url, client_id, client_secret)
        is_rels = True
    else:
        token = auth["password"]

    headers = {"Authorization": f"Bearer {token}"}

    try:
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
        raw_keys = [row[0] for row in cursor.fetchall()]

        # ── Ensure unique keys while preserving ORDER BY ──
        seen = set()
        keys = []
        for k in raw_keys:
            if k not in seen:
                seen.add(k)
                keys.append(k)

        pending_keys = set(keys)

        if not keys:
            logger.warning(f"No pending listing keys for source {source_id}")
            return {
                "source_id": source_id,
                "source_name": source_name,
                "mls_board": mls_board,
                "flow_type": flow_type,
                "source_type": source_type,
                "batch_creation_date": batch_creation_date,
                "batch_id": batch_id,
                "last_refresh_date": source_data["last_modification_date"],
                "status": True,
                "success": False,
                "run_host": run_host,
            }

        chunk_size = 20
        chunks = [keys[i : i + chunk_size] for i in range(0, len(keys), chunk_size)]
        chunk_idx = 0
        property_url = loginurl.replace("$metadata", "Property")
        property_list = []

        while chunk_idx < len(chunks):
            chunk = chunks[chunk_idx]
            if is_rels:
                key_filter = " or ".join(f"{reference_key} eq {k}" for k in chunk)
            else:
                key_filter = f"{reference_key} in ({','.join(f"'{k}'" for k in chunk)})"
            params = {"$filter": key_filter, "$top": 200}

            try:
                resp = requests.get(property_url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                property_list.extend(data.get("value", []))
                for item in data.get("value", []):
                    k = item.get(reference_key)
                    if k and k in pending_keys:
                        pending_keys.discard(k)
                if not pending_keys:
                    logger.info(f"All pending keys found for source {source_id}")
                    break
                chunk_idx += 1
                chunk_size = min(20, chunk_size * 2)
            except requests.exceptions.HTTPError as he:
                if resp is not None and resp.status_code == 500:
                    if chunk_size > 1:
                        chunk_size = max(1, chunk_size // 2)
                        logger.warning(
                            f"500 error – reducing chunk size to {chunk_size}"
                        )
                        remaining_keys = [k for k in keys if k in pending_keys]
                        chunks = [
                            remaining_keys[i : i + chunk_size]
                            for i in range(0, len(remaining_keys), chunk_size)
                        ]
                        chunk_idx = 0
                    else:
                        logger.error(f"Skipping problematic single key: {chunk[0]}")
                        chunk_idx += 1
                else:
                    logger.error(
                        {"source_id": source_id, "chunk_download_error": str(he)}
                    )
                    return {
                        "source_id": source_id,
                        "status": False,
                        "success": False,
                        "error": str(he),
                    }
            except Exception as e:
                logger.error({"source_id": source_id, "chunk_download_error": str(e)})
                return {
                    "source_id": source_id,
                    "status": False,
                    "success": False,
                    "error": str(e),
                }

        if not property_list:
            logger.warning(f"No property data fetched for source {source_id}")
            status = True
        else:
            # ── Load Property directly for ALL sources (like RELS) ──
            prop_s3_url = [property_url] * len(property_list)
            prop_s3_filter = [str(params.get("$filter", ""))] * len(property_list)
            try:
                prop_s3_df = _build_s3_df(
                    property_list,
                    prop_s3_url,
                    prop_s3_filter,
                    batch_creation_date,
                    source_id,
                    batch_id,
                    drop_cols=["@odata.id", "Media", "PropertyRooms"],
                )
                Upload_data_into_S3_DataLake(
                    prop_s3_df,
                    source_id,
                    source_type,
                    source_name,
                    batch_id,
                    "Property",
                )
                del prop_s3_df
            except Exception as e:
                logger.error({"source_id": source_id, "Property S3 error": str(e)})

            table_name = f"ps_paragon_property_{source_id}"
            table_creation_and_loading(
                property_list,
                source_id,
                table_name,
                source_name,
                batch_id,
                batch_creation_date,
                cursor,
                connection,
                "Property",
            )

            # ── Sub‑resources ──
            if is_rels:
                status = _paragonrels_load(
                    source_data,
                    property_list,
                    headers,
                    loginurl,
                    batch_creation_date,
                    source_type,
                    connection,
                    cursor,
                )
            else:
                # Classic Paragon: _paragon_load handles Media, Office, Member, OpenHouse
                status = _paragon_load(
                    source_data,
                    property_list,
                    headers,
                    loginurl,
                    batch_creation_date,
                    source_type,
                    connection,
                    cursor,
                )

            if not status:
                return {
                    "source_id": source_id,
                    "status": False,
                    "success": False,
                    "error": "Sub-resource loading failed",
                }

        last_refresh = source_data["last_modification_date"]
        if flow_type == "sold" and "1990-01-01" in last_refresh:
            last_refresh = source_data.get("sold_date")

        response = {
            "source_id": source_id,
            "source_name": source_name,
            "mls_board": mls_board,
            "flow_type": flow_type,
            "source_type": source_type,
            "batch_creation_date": batch_creation_date,
            "temp_table_status": temp_table_status,
            "batch_id": batch_id,
            "last_refresh_date": last_refresh,
            "status": status,
            "success": False,
            "run_host": run_host,
            "limit": limit,
            "bl_flag": bl_flag,
        }
        return response

    except Exception as e:
        source_data["status"] = False
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data


def lambda_handler(event, context):
    source_data = event
    download_flag = source_data["download_flag"]

    sqlExecLimit = context.get_remaining_time_in_millis()
    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    db_secret_dev = fetch_secrets(rdsDatabase)
    db_secret_stage = fetch_secrets(listingDatabase)
    serverless_db_con = setup_db_connection(db_secret_dev, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    homelisting_db_con = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_homelisting = homelisting_db_con.cursor()

    try:
        if download_flag:
            final_response = download_func(
                source_data, cursor_serverless, serverless_db_con
            )
        else:
            final_response = validation_func(
                source_data, cursor_serverless, cursor_homelisting
            )
        logger.info(final_response)
        return final_response
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
