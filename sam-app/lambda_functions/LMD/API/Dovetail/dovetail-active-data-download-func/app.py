"""Dovetail Validation and Data Download Lambda Function (with temp_table integration)"""

import json
import os
import io
from datetime import datetime, timezone
import requests
import logging
import random
import time
import boto3
import pandas as pd
import psycopg2
from psycopg2 import extras
import traceback

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):
        session = boto3.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response_db = client.get_secret_value(SecretId=secret_name)
            secret_db = get_secret_value_response_db["SecretString"]
            secret_dict_db = json.loads(secret_db)
            return secret_dict_db
        except Exception as e:
            raise e


def api_call_and_get_count(token, loginurl, params, max_last_modified_date):
    headers = {"Authorization": f"Bearer {token}"}
    response = None
    try:
        random_number = random.randint(1, 3)
        time.sleep(random_number)
        response = requests.get(
            url=loginurl, headers=headers, params=params, timeout=30
        )
        response.raise_for_status()
        data = json.loads(response.text)
        total_count = data.get("@odata.count", 0)
        latest_listing_date = max_last_modified_date
        if total_count > 0:
            latest_listing_date = data["value"][0]["ModificationTimestamp"]
        return total_count, latest_listing_date
    except Exception as e:
        log_msg = {
            "server_response": response.text,
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        raise Exception("Error in API call: {}".format(log_msg))


def db_conn(db_secret, sqlExecLimit):
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
        response_dict_success = {"status": "Success"}
        logger.info(
            "Database connection established successfully",
            extra={"event": response_dict_success},
        )
        return connection
    except Exception as e:
        log_msg = {
            "Level": "Error",
            "Function": "db_conn()",
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        logger.error("Error while establishing connection: %s", log_msg)
        return False


def execute_query(connection, query, cursor, query_mode=None):
    log_msg = {"Executed Query": query}
    cursor.execute(query)
    logger.info("Query executed successfully", extra={"event": log_msg})
    if query_mode is None:
        data = cursor.fetchone()
    else:
        try:
            generated_id = cursor.fetchone()
            connection.commit()
            return generated_id
        except psycopg2.ProgrammingError:
            connection.commit()
            return None
    return data


def get_max_last_modified_date(
    serverless_db_con, source_id, cursor_serverless, flow, rolling_window_offset=None
):
    query_for_last_modified_date_serverless = """
        SELECT last_modified_date::timestamp(0)
        FROM stage.serverless_idx_loads
        WHERE source_id = {}
        limit 1
    """.format(source_id)

    if flow == "sold":
        query_for_last_modified_date_serverless = """
            SELECT sold_date::timestamp(0)
            FROM stage.serverless_idx_loads
            WHERE source_id = {}
            limit 1
        """.format(source_id)
    elif flow == "respecs":
        query_for_last_modified_date_serverless = """
            SELECT respecs_start_date::timestamp(0)
            FROM stage.serverless_idx_loads
            WHERE source_id = {}
        """.format(source_id)
    elif flow == "full_load":
        query_for_last_modified_date_serverless = """
            SELECT full_load_date::timestamp(0)
            FROM stage.serverless_idx_loads
            WHERE source_id = {}
        """.format(source_id)
    elif flow == "rolling_window":
        query_for_last_modified_date_serverless = """
            SELECT last_modified_date::timestamp(0) - interval '{1} hours'
            FROM stage.serverless_idx_loads
            WHERE source_id = {0}
        """.format(source_id, rolling_window_offset)
    elif flow == "backlog":
        query_for_last_modified_date_serverless = """
            SELECT bl_start_date::timestamp(0)
            FROM stage.serverless_idx_loads
            WHERE source_id = {}
        """.format(source_id)

    max_date_serverless = execute_query(
        serverless_db_con, query_for_last_modified_date_serverless, cursor_serverless
    )
    max_date_serverless = (
        None if "None" in str(max_date_serverless) else max_date_serverless
    )
    formatted_date_serverless = formatted_date(max_date_serverless)
    return formatted_date_serverless


def formatted_date(date):
    dumy_date = "1990-01-01 00:00:00"
    naive_datetime = datetime.strptime(dumy_date, "%Y-%m-%d %H:%M:%S")
    original_datetime_utc = naive_datetime.replace(tzinfo=timezone.utc)
    formatted_datetime_utc = original_datetime_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if date is None:
        return formatted_datetime_utc

    if isinstance(date, str):
        try:
            original_datetime_aware = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            original_datetime_aware = original_datetime_aware.replace(
                tzinfo=timezone.utc
            )
            return original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return formatted_datetime_utc

    if isinstance(date, tuple):
        return formatted_date(date[0])

    if isinstance(date, datetime):
        date = date.replace(tzinfo=timezone.utc)
        return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return formatted_datetime_utc


def etl_source_info(source_id, cursor_pentaho):
    query = f"""
        select source_info->>'flow_type' as flow_type, source_info->>'sold_status' as sold_status,
               source_info->>'active_status' as active_status, source_info->>'status_column' as status_column,
               source_info->>'sold_column' as sold_column
        from source
        where id = {source_id}
    """
    cursor_pentaho.execute(query)
    result = cursor_pentaho.fetchone()
    return {
        "flow_type": str(result[0]).lower(),
        "sold_status": result[1],
        "active_status": result[2],
        "status_column": result[3],
        "sold_column": result[4],
    }


def request_source_data(url, params, headers):
    """Generic function to call Dovetail API with retry and return JSON."""
    try:
        time.sleep(random.randint(1, 3))
        response = requests.get(url=url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return json.loads(response.text)
    except Exception as e:
        time.sleep(5)
        response = requests.get(url=url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return json.loads(response.text)


def request_and_load_temp_table(
    source_data, flow_type, cursor_serverless, serverless_db_con
):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    auth = source_data["auth"]
    token = source_data["token"]
    loginurl = auth["loginUrl"]
    sold_column = source_data["source_info"]["sold_column"]
    final_url = loginurl.replace("$metadata", "Property")
    key_column = source_data["source_info"].get("key_column", "ListingKey")
    cdc_column = source_data["source_info"].get("cdc_column", "ModificationTimestamp")
    media_cdc_column = source_data["source_info"].get(
        "media_cdc_column", "PhotosChangeTimestamp"
    )
    top = int(source_data["source_info"].get("top", 1000))

    headers = {"Authorization": f"Bearer {token}"}
    last_modification_date = source_data["last_modification_date"]

    lmd_date = (
        str(last_modification_date).split(".", 1)[0].replace(":", "").replace("-", "")
    )
    lmd_date = f"{lmd_date}_temp"

    filter_parts = [f"{cdc_column} ge {last_modification_date}"]
    select_fields = (
        f"{key_column},{cdc_column},{media_cdc_column}"
        if flow_type in ["full_load", "respecs"]
        else f"{key_column},{cdc_column},{media_cdc_column},{sold_column}"
    )
    if flow_type == "sold":
        sold_status = source_data.get("sold_status")
        filter_parts.append(sold_status)
    elif flow_type in ["full_load", "respecs"]:
        active_status = source_data.get("active_status")
        filter_parts.append(active_status)
    # orderby = f"{sold_column} asc, {cdc_column} asc" if flow_type == "sold" else f"{cdc_column} asc"
    orderby = f"{cdc_column} asc"
    params = {
        "$filter": " and ".join(filter_parts),
        "$count": "true",
        "$top": top,
        "$select": select_fields,
        "$orderby": orderby,
    }

    total_count = 1
    skip = 0
    temp_df = []
    respecs_flag = flow_type == "respecs"

    while skip < total_count:
        params["$skip"] = skip
        data = request_source_data(final_url, params, headers)
        total_count = data.get("@odata.count", 0)
        value = data.get("value", [])

        if not value:
            break

        for item in value:
            listingkey = item.get(key_column)
            mod_ts = item.get(cdc_column)
            media_mod_ts = item.get(media_cdc_column)
            sold_date = (
                item.get(sold_column)
                if flow_type not in ["full_load", "respecs"]
                else None
            )
            temp_df.append(
                (
                    source_id,
                    listingkey,
                    mod_ts,
                    media_mod_ts,
                    sold_date,
                    respecs_flag,
                    True,
                )
            )

        # Upload page to S3 for audit
        df_page = pd.DataFrame(data)
        batch_id = source_data.get("batch_id", lmd_date)
        source_type = source_data.get("source_info", {}).get("source_type", "Dovetail")
        Upload_data_into_S3_DataLake(
            df_page,
            source_id,
            source_type,
            source_name,
            batch_id,
            "Property_Temp",
            skip,
        )

        if skip + params["$top"] >= total_count:
            break
        skip += params["$top"]

    if temp_df:

        insert_query = """
            INSERT INTO idx_stage.temp_table
                (source_id, listingkey, modification_timestamp, media_modification_timestamp, sold_date, respecs_flag, download_flag)
            VALUES %s
        """
        extras.execute_values(cursor_serverless, insert_query, temp_df)
        serverless_db_con.commit()
        logger.info(
            f"Inserted {len(temp_df)} records into temp_table for source_id {source_id}"
        )
    else:
        logger.info(f"No records to insert into temp_table for source_id {source_id}")


def aggregate_temp_table(source_id, temp_respecs_flag, cursor_serverless):
    """Get count, min modification timestamp, and max modification timestamp from temp_table for this source."""
    query = f""" select count(distinct listingkey), min(modification_timestamp),  max(modification_timestamp) 
        from idx_stage.temp_table 
        where source_id = {source_id} and download_flag = 't' and respecs_flag = '{temp_respecs_flag}' """
    cursor_serverless.execute(query)
    result = cursor_serverless.fetchone()
    total_count = result[0] if result[0] else 0
    min_ts = formatted_date(result[1]) if result[1] else None
    max_ts = formatted_date(result[2]) if result[2] else None
    return total_count, min_ts, max_ts


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value


def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    generic_df.insert(0, "source_creation_date", current_datetime)
    generic_df.insert(0, "y_last_update_date", batch_creation_date)
    generic_df.insert(0, "y_creation_date", batch_creation_date)
    generic_df.insert(0, "source_last_update_date", current_datetime)
    generic_df.insert(0, "batch_id", int(batch_id))
    generic_df.insert(0, "source_id", int(source_id))
    return generic_df


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, class_Name, Skip
):
    filename = f"{source_name}_{class_Name}_{Skip}.parquet"
    folder_path = f"{source_type}/{source_id}_{source_name}/{batch_id}/{class_Name}/"
    s3_key = folder_path + filename

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


def table_creation_and_loading(
    df,
    table_name,
    source_id,
    cursor_serverless,
    serverless_db_con,
    source_name,
    resource_name,
):
    df = df.drop(
        columns=["Media", "Rooms", "UnitTypes", "BodyOfWater", "@odata.id"],
        errors="ignore",
    )
    df.columns = df.columns.str.lower()
    df_instance = df.loc[:, ~df.columns.duplicated()]

    max_column_name_length = 63
    columns_to_rename = {
        col: col[:max_column_name_length]
        for col in list(df_instance.columns)
        if len(col) > max_column_name_length
    }
    df_instance.rename(columns=columns_to_rename, inplace=True)

    df_instance.fillna(pd.NaT)
    df_instance.fillna("")
    df_instance = df_instance.apply(lambda x: x.map(remove_characters))
    df_instance_filtered = df_instance.apply(lambda x: x.map(clean_value))

    if table_name == "ps_dovetail_Open_House_1014":
        column_names = """SELECT column_name FROM information_schema.columns WHERE table_name ~* '{}'""".format(
            table_name
        )
    else:
        column_names = """SELECT column_name FROM information_schema.columns WHERE table_name ~* '{}' and column_name not in ('pid', 'id')""".format(
            table_name
        )

    cursor_serverless.execute(column_names)
    table_column_names = [column[0] for column in cursor_serverless.fetchall()]
    df_cols = list(df_instance_filtered.columns)
    extra_cols = set(df_cols) - set(table_column_names)

    for n in extra_cols:
        alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN {1} TEXT""".format(
            table_name, n
        )
        cursor_serverless.execute(alter_query)
        insert_query = f""" INSERT INTO dev.field_metadata 
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name) 
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}'); 
        """
        cursor_serverless.execute(insert_query)
        serverless_db_con.commit()
        logger.info(f"Added new column {n} to {table_name}")

    cols = ",".join(list(df_instance_filtered.columns))
    data_values = [tuple(row) for row in df_instance_filtered.values]
    insert_query = "INSERT INTO idx_stage.{0} ({1}) VALUES %s".format(table_name, cols)

    if table_name == "ps_dovetail_media":
        insert_query = insert_query.replace("order", '"order"')

    extras.execute_values(cursor_serverless, insert_query, data_values)
    serverless_db_con.commit()
    logger.info(f"Inserted {len(data_values)} rows into {table_name}")


def download_from_source(url_endpoint, params, headers):
    try:
        time.sleep(random.randint(1, 5))
        response = requests.get(
            url=url_endpoint, params=params, headers=headers, timeout=30
        )
        response.raise_for_status()
    except Exception:
        time.sleep(5)
        response = requests.get(
            url=url_endpoint, params=params, headers=headers, timeout=30
        )
        response.raise_for_status()
    data = json.loads(response.text)
    data["url_endpoint"] = url_endpoint
    data.update(params)
    return data


def chunks_creation_from_list(key_list, chunk_size):
    return [key_list[i : i + chunk_size] for i in range(0, len(key_list), chunk_size)]


def download_data_for_chunks(
    request_url, headers, key_column, key_chunks, chunk_size, expand_classes=None
):
    data_frames = []
    for chunk in key_chunks:
        if not chunk:
            continue
        keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
        params = {
            "$filter": f"{key_column} in ({keys_str})",
            "$top": chunk_size,
        }
        if expand_classes:
            params["$expand"] = expand_classes

        try:
            data = request_source_data(request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                temp_df["request_url"] = request_url
                temp_df["request_params"] = json.dumps(params)
                data_frames.append(temp_df)
        except Exception as e:
            logger.error(f"Error downloading chunk for {key_column}: {e}")
            raise
    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    return pd.DataFrame()


def api_call_and_load_tables(
    source_data, cursor_serverless, serverless_db_con, listing_chunks
):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    auth = source_data["auth"]
    token = source_data["token"]
    loginurl = auth["loginUrl"]
    expand_classes = source_info.get("expand_classes")
    top = int(source_info.get("top", 1000))
    chunk_size = 300
    batch_creation_date = source_data["batch_creation_date"]
    batch_id = source_data["batch_id"]
    source_type = source_info.get("source_type", "Dovetail")

    headers = {"Authorization": f"Bearer {token}"}
    property_request_url = loginurl.replace("$metadata", "Property")
    office_request_url = loginurl.replace("$metadata", "Office")
    member_request_url = loginurl.replace("$metadata", "Member")
    media_request_url = loginurl.replace("$metadata", "Media")
    openhouse_request_url = loginurl.replace("$metadata", "OpenHouse")

    property_key_col = source_info.get("key_column", "ListingKey")
    office_key_col = source_info.get("office_key_col", "OfficeKey")
    member_key_col = source_info.get("member_key_col", "MemberKey")

    office_parent_cols_str = (
        source_info.get("office_parent_cols").replace("'", "").replace('"', "")
    )
    member_parent_cols_str = (
        source_info.get("member_parent_cols").replace("'", "").replace('"', "")
    )

    office_parent_cols = [
        x.strip() for x in office_parent_cols_str.split(",") if x.strip()
    ]
    member_parent_cols = [
        x.strip() for x in member_parent_cols_str.split(",") if x.strip()
    ]

    all_office_ids = []
    all_member_ids = []

    # 1. Process Property
    property_df = pd.DataFrame()
    if listing_chunks:
        prop_expand = expand_classes if expand_classes else None
        for chunk in listing_chunks:
            keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
            params = {
                "$filter": f"{property_key_col} in ({keys_str})",
                "$top": top,
            }
            if prop_expand:
                params["$expand"] = prop_expand
            data = download_from_source(property_request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                property_df = pd.concat([property_df, temp_df], ignore_index=True)

        if not property_df.empty:
            prop_df = adding_extra_columns(
                property_df, batch_creation_date, source_id, batch_id
            )
            Upload_data_into_S3_DataLake(
                prop_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                "Property",
                "full",
            )
            table_creation_and_loading(
                prop_df,
                f"ps_dovetail_Property_{source_id}",
                source_id,
                cursor_serverless,
                serverless_db_con,
                source_name,
                "Property",
            )

            if expand_classes:
                expanded_list = expand_classes.split(",")
                for exp_class in expanded_list:
                    if exp_class not in property_df.columns:
                        continue
                    temp_exp_df = (
                        property_df[["ListingKey", exp_class]]
                        .explode(exp_class)
                        .dropna(subset=[exp_class])
                    )
                    if temp_exp_df.empty:
                        continue
                    nested_df = (
                        temp_exp_df[exp_class].apply(pd.Series).reset_index(drop=True)
                    )
                    if "ListingKey" not in nested_df.columns:
                        nested_df = pd.concat(
                            [
                                temp_exp_df[["ListingKey"]].reset_index(drop=True),
                                nested_df,
                            ],
                            axis=1,
                        )
                    if exp_class == "Media":
                        nested_df.rename(
                            columns={"ListingKey": "PropertyListingKey"}, inplace=True
                        )
                    if "media" in exp_class.lower():
                        resource_name = "Media"
                        table_name = "ps_dovetail_media"
                    elif "room" in exp_class.lower():
                        resource_name = "PropertyRooms"
                        table_name = f"ps_dovetail_propertyrooms_{source_id}"
                    elif "unit" in exp_class.lower():
                        resource_name = "PropertyUnitTypes"
                        table_name = f"ps_dovetail_propertyunittypes_{source_id}"
                    elif "bodyofwater" in exp_class.lower():
                        resource_name = "PropertyBodyOfWater"
                        table_name = f"ps_dovetail_propertyBodyOfWater_{source_id}"
                    else:
                        continue

                    nested_df = adding_extra_columns(
                        nested_df, batch_creation_date, source_id, batch_id
                    )
                    if not nested_df.empty:
                        Upload_data_into_S3_DataLake(
                            nested_df,
                            source_id,
                            source_type,
                            source_name,
                            batch_id,
                            resource_name,
                            "expanded",
                        )
                        table_creation_and_loading(
                            nested_df,
                            table_name,
                            source_id,
                            cursor_serverless,
                            serverless_db_con,
                            source_name,
                            resource_name,
                        )
                    property_df.drop(columns=[exp_class], inplace=True)

            for col in office_parent_cols:
                if col in property_df.columns:
                    all_office_ids.extend(property_df[col].dropna().tolist())
            for col in member_parent_cols:
                if col in property_df.columns:
                    all_member_ids.extend(property_df[col].dropna().tolist())

            all_office_ids = list(set([x for x in all_office_ids if x]))
            all_member_ids = list(set([x for x in all_member_ids if x]))

    # 2. Process Media
    if not property_df.empty:
        listing_keys = property_df["ListingKey"].dropna().unique().tolist()
        listing_chunks_media = chunks_creation_from_list(listing_keys, 5)
        media_df = pd.DataFrame()
        for chunk in listing_chunks_media:
            keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
            params = {"$filter": f"ResourceRecordKey in ({keys_str})", "$top": 200}
            data = download_from_source(media_request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                media_df = pd.concat([media_df, temp_df], ignore_index=True)
        if not media_df.empty:
            media_df = adding_extra_columns(
                media_df, batch_creation_date, source_id, batch_id
            )
            Upload_data_into_S3_DataLake(
                media_df, source_id, source_type, source_name, batch_id, "Media", "full"
            )
            table_creation_and_loading(
                media_df,
                "ps_dovetail_media",
                source_id,
                cursor_serverless,
                serverless_db_con,
                source_name,
                "Media",
            )

    # 3. Process Office
    if all_office_ids:
        office_chunks = chunks_creation_from_list(all_office_ids, chunk_size)
        office_df = pd.DataFrame()
        for chunk in office_chunks:
            keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
            params = {"$filter": f"{office_key_col} in ({keys_str})", "$top": top}
            data = download_from_source(office_request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                office_df = pd.concat([office_df, temp_df], ignore_index=True)
        if not office_df.empty:
            office_df = adding_extra_columns(
                office_df, batch_creation_date, source_id, batch_id
            )
            Upload_data_into_S3_DataLake(
                office_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                "Office",
                "full",
            )
            table_creation_and_loading(
                office_df,
                f"ps_dovetail_Office_{source_id}",
                source_id,
                cursor_serverless,
                serverless_db_con,
                source_name,
                "Office",
            )

    # 4. Process Member
    if all_member_ids:
        member_chunks = chunks_creation_from_list(all_member_ids, chunk_size)
        member_df = pd.DataFrame()
        for chunk in member_chunks:
            keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
            params = {"$filter": f"{member_key_col} in ({keys_str})", "$top": top}
            data = download_from_source(member_request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                member_df = pd.concat([member_df, temp_df], ignore_index=True)
        if not member_df.empty:
            member_df = adding_extra_columns(
                member_df, batch_creation_date, source_id, batch_id
            )
            Upload_data_into_S3_DataLake(
                member_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                "Member",
                "full",
            )
            table_creation_and_loading(
                member_df,
                f"ps_dovetail_Member_{source_id}",
                source_id,
                cursor_serverless,
                serverless_db_con,
                source_name,
                "Member",
            )

    # 5. Process OpenHouse using listing keys from property_df
    if not property_df.empty:
        listing_keys = property_df["ListingKey"].dropna().unique().tolist()
        openhouse_chunks = chunks_creation_from_list(listing_keys, chunk_size)
        openhouse_df = pd.DataFrame()
        for chunk in openhouse_chunks:
            keys_str = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
            params = {
                "$filter": f"ListingId in ({keys_str})",
                "$top": top,
                "$count": "true",
            }
            data = download_from_source(openhouse_request_url, params, headers)
            if data.get("value"):
                temp_df = pd.DataFrame(data["value"])
                openhouse_df = pd.concat([openhouse_df, temp_df], ignore_index=True)

        if not openhouse_df.empty:
            openhouse_df = adding_extra_columns(
                openhouse_df, batch_creation_date, source_id, batch_id
            )
            Upload_data_into_S3_DataLake(
                openhouse_df,
                source_id,
                source_type,
                source_name,
                batch_id,
                "OpenHouse",
                "full",
            )
            table_name = "ps_dovetail_Open_House_1014"
            table_creation_and_loading(
                openhouse_df,
                table_name,
                source_id,
                cursor_serverless,
                serverless_db_con,
                source_name,
                "OpenHouse",
            )

    return True


def validation_func(
    pentaho_db_con, serverless_db_con, cursor_serverless, cursor_pentaho, source_data
):
    try:
        source_id = source_data["source_id"]
        source_name = source_data["source_name"]
        source_info = source_data["source_info"]
        batch_execution_params = source_data["batch_execution_params"]
        bl_flag = batch_execution_params["bl_flag"]
        itr_value = batch_execution_params["itr_value"]
        respecs_flag = batch_execution_params["respecs_flag"]
        runtime_count = source_data["runtime_count"]
        rolling_window_batch = source_info["rolling_window_batch"]
        rolling_window_offset = None
        temp_respecs_flag = "f"

        etl_data = etl_source_info(source_id, cursor_pentaho)
        flow_type = etl_data["flow_type"]

        if flow_type not in ["sold", "full_load"]:
            if respecs_flag is True and (runtime_count % itr_value != 0):
                flow_type = "respecs"
                temp_respecs_flag = "t"
            elif bl_flag is True and (runtime_count % itr_value != 0):
                flow_type = "backlog"
            elif runtime_count % rolling_window_batch == 0:
                rolling_window_offset = source_info["rolling_window_offset"]
                flow_type = "rolling_window"

        source_data["flow_type"] = flow_type
        source_data["sold_status"] = etl_data["sold_status"]
        source_data["active_status"] = etl_data["active_status"]

        cleanup_query = "DELETE FROM idx_stage.temp_table WHERE source_id = %s AND download_flag = 'f'"
        cursor_serverless.execute(cleanup_query, (source_id,))
        serverless_db_con.commit()

        log_message = {
            "source_id": source_id,
            "source_name": source_name,
            "deleted_count": cursor_serverless.rowcount,
            "Query": cleanup_query,
        }
        logger.info(log_message)

        total_count, last_modified_date, latest_listing_date = aggregate_temp_table(
            source_id, temp_respecs_flag, cursor_serverless
        )

        source_data["temp_table_status"] = (
            True
            if (
                total_count == 0
                or (bl_flag is True and flow_type in ["lmd", "rolling_window"])
            )
            else False
        )

        source_data["last_modification_date"] = last_modified_date

        if source_data["temp_table_status"]:
            last_modified_date = get_max_last_modified_date(
                serverless_db_con,
                source_id,
                cursor_serverless,
                flow_type,
                rolling_window_offset,
            )
            source_data["last_modification_date"] = last_modified_date

            request_and_load_temp_table(
                source_data, flow_type, cursor_serverless, serverless_db_con
            )

            total_count, last_modified_date, latest_listing_date = aggregate_temp_table(
                source_id, temp_respecs_flag, cursor_serverless
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


def data_download_func(
    pentaho_db_con, cursor_pentaho, cursor_serverless, serverless_db_con, source_data
):
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    run_host = source_data["run_host"]
    source_info = source_data["source_info"]
    flow_type = source_data["flow_type"]
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    last_modification_date = source_data["last_modification_date"]
    mls_board = source_info["mls_board"]
    source_type = source_info["source_type"]
    limit = source_info.get("limit", 1000)
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

    cursor_serverless.execute(query)
    listing_keys = cursor_serverless.fetchall()
    listing_keys = [row[0] for row in listing_keys]

    if not listing_keys:
        logger.info(f"No listing keys to download for source {source_id}")
        return {
            "source_id": source_id,
            "source_name": source_name,
            "batch_id": batch_id,
            "mls_board": mls_board,
            "flow_type": flow_type,
            "limit": limit,
            "source_type": source_type,
            "run_host": run_host,
            "status": True,
            "last_refresh_date": last_modification_date,
            "batch_creation_date": batch_creation_date,
            "success": False,
            "message": "No new listings to download",
        }

    chunk_size = 300
    listing_chunks = chunks_creation_from_list(listing_keys, chunk_size)

    try:
        status = api_call_and_load_tables(
            source_data, cursor_serverless, serverless_db_con, listing_chunks
        )
    except Exception as e:
        logger.error(f"Download failed for source {source_id}: {e}")
        raise

    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "mls_board": mls_board,
        "flow_type": flow_type,
        "limit": limit,
        "source_type": source_type,
        "run_host": run_host,
        "status": status,
        "last_refresh_date": last_modification_date,
        "batch_creation_date": batch_creation_date,
        "temp_table_status": source_data["temp_table_status"],
        "bl_flag": source_data["batch_execution_params"]["bl_flag"],
        "success": False,
    }
    logger.info(final_response)
    return final_response


def create_token(source_id, source_name, client_id, client_secret):
    url = "https://reso.dovetaildata.com/odata/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
        "client_secret": str(client_secret),
    }
    auth = (str(client_id), str(client_secret))
    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        logs = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "Token Generation Failed",
            "Status Code": response.status_code,
        }
        logger.error(logs)
        raise Exception(f"Token generation failed for source {source_id}")


def lambda_handler(event, context):
    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    db_secret_dev = SecretManagerHelper.get_secret(rdsDatabase, "us-west-2")
    db_secret_stage = SecretManagerHelper.get_secret(listingDatabase, "us-west-2")
    serverless_db_con = db_conn(db_secret_dev, sqlExecLimit)
    pentaho_db_con = db_conn(db_secret_stage, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    cursor_pentaho = pentaho_db_con.cursor()

    source_data = event
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    client_id = source_data["auth"]["user"]
    password = source_data["auth"]["password"]
    token_generation = source_data.get("source_info", {}).get("token_generation", False)

    if token_generation:
        token = create_token(source_id, source_name, client_id, password)
        source_data["token"] = token
    else:
        source_data["token"] = password

    try:
        if source_data.get("download_flag", False):
            source_data = data_download_func(
                pentaho_db_con,
                cursor_pentaho,
                cursor_serverless,
                serverless_db_con,
                source_data,
            )
        else:
            source_data = validation_func(
                pentaho_db_con,
                serverless_db_con,
                cursor_serverless,
                cursor_pentaho,
                source_data,
            )
        return source_data

    except Exception as e:
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        source_data.update(log_msg)
        logger.error(source_data)
        return source_data

    finally:
        if cursor_pentaho:
            cursor_pentaho.close()
        if cursor_serverless:
            cursor_serverless.close()
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()
