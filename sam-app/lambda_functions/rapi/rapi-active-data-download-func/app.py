"""R API Active Data Download Lambda"""

import io
import json
import os
import logging
import traceback
from datetime import datetime, timezone
import time
import boto3  # type: ignore
import pandas as pd
import requests
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
    except:
        raise


def create_token(data):
    client_id = data["user"]
    loginUrl = data["loginUrl"]
    client_secret = data["password"]

    headers = {"Content-Type": "application/json", "Accept": "*/*"}

    data = {
        "grant_type": "client_credentials",
        "client_id": str(client_id),
        "client_secret": str(client_secret),
        "audience": "rcapi.realcomp.com",
    }
    data = json.dumps(data)

    try:
        response = requests.post(url=loginUrl, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        response = json.loads(response.text)
        token = response["access_token"]

        return token
    except:
        log_msg = {
            "statusCode": response.status_code,
            "message": f"Token Generation Failed",
        }
        logger.error(log_msg)
        raise


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, resource_name, skip=None
):
    """Serialize a DataFrame to Parquet and upload it to the S3 data-lake."""
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

    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "resource_name": resource_name,
        "s3_key": s3_key,
        "rows_uploaded": len(df_upload),
    }
    logger.info(log_msg)


def _build_s3_df(
    df, s3_url_list, s3_filter_list, batch_creation_date, source_id, batch_id
):
    """
    Attach request-context columns (url + filter) and extra metadata columns
    to a copy of *df*, then deduplicate on business columns only.
    Mirrors the same helper used in SourceRE.
    """
    df_s3 = df.copy()

    # Serialise any list/dict cells so Parquet doesn't choke
    for col in df_s3.columns:
        df_s3[col] = df_s3[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    df_s3 = adding_extra_columns(df_s3, source_id, batch_creation_date, batch_id)
    df_s3 = df_s3.reset_index(drop=True)

    # Assign per-record context BEFORE dedup so the filter travels with each row
    df_s3["request_url_endpoint"] = s3_url_list
    df_s3["request_filter"] = s3_filter_list

    # Dedup on business columns only — context cols are excluded
    context_cols = ["request_url_endpoint", "request_filter"]
    business_cols = [c for c in df_s3.columns if c not in context_cols]
    df_s3.drop_duplicates(subset=business_cols, inplace=True)

    return df_s3


# API GET Request function
def request_source(url, token, chunks, key_column, expandable_classes=None):

    headers = {"User-Agent": "Ylopo", "Authorization": f"Bearer {token}"}
    params = {"$top": 100}

    if "property" in url.lower():
        params["$expand"] = expandable_classes

    data_list = []
    s3_url_list = []  # one entry per record
    s3_filter_list = []  # one entry per record

    for chunk in chunks:
        each_chunk = str(chunk).replace("[", "").replace("]", "").replace(" ", "")
        if "mlsid" not in key_column.lower():
            each_chunk = each_chunk.replace("'", "")

        current_filter = f"{key_column} in ({each_chunk})"
        params["$filter"] = current_filter

        try:
            response = requests.get(url=url, params=params, headers=headers)
            response.raise_for_status()

        except:
            time.sleep(60)
            response = requests.get(url=url, params=params, headers=headers)
            response.raise_for_status()

        data = json.loads(response.text)
        records = data["value"]
        data_list.extend(records)

        # Keep url + filter aligned with every record in this chunk
        for _ in records:
            s3_url_list.append(url)
            s3_filter_list.append(current_filter)

    return pd.DataFrame(data_list), s3_url_list, s3_filter_list


def formatted_date(date):
    if not date:
        date = "1990-01-01 00:00:00"
    date = str(date).split(".")[0]
    original_datetime_aware = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    formatted_time = original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_time


def get_lmd(
    source_id,
    cursor_rds,
    cursor_homelisting,
    flow_type,
    rolling_window_offset=None,
):
    """Get the maximum last modified date for a given source_id and flow type."""

    if flow_type == "sold":

        modification_timestamp = "1990-01-01 00:00:00"
        query = f"SELECT sold_date, batch_id from stage.serverless_idx_loads where source_id = {source_id} limit 1"
        cursor_rds.execute(query)
        sold_date, batch_id = cursor_rds.fetchone()  # batch_id of last executed batch

        # if previous batch has same sold_date in all listings; then get max(modification_timestamp) to avoid loop in API request
        query = f"""SELECT CASE WHEN min(sold_date) = max(sold_date) THEN max(modification_timestamp)::timestamp(0) ELSE NULL END AS max_modification_timestamp
        from listing_p_sold where source_id = {source_id} and batch_id = {batch_id} """
        cursor_homelisting.execute(query)
        max_modification_timestamp = cursor_homelisting.fetchone()[0]

        if max_modification_timestamp:
            modification_timestamp = max_modification_timestamp

        return formatted_date(modification_timestamp), str(sold_date)

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

    cursor_rds.execute(lmd_query)
    max_date_serverless = cursor_rds.fetchone()[0]
    last_modified_date = formatted_date(max_date_serverless)  # type: ignore

    return last_modified_date


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


def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value


def prestage_tables_insertion(
    data_df,
    table_name,
    source_id,
    source_name,
    resource_name,
    cursor_rds,
    rds_connection,
    batch_creation_date=None,
    batch_id=None,
):
    """Loading DataFrame to the table in serverless database"""

    data_df.fillna(pd.NaT)
    data_df.fillna("")
    data_df = data_df.apply(lambda col: col.map(remove_characters))
    data_df = data_df.apply(lambda col: col.map(clean_value))
    data_df.drop_duplicates(inplace=True)
    if table_name != "temp_table":
        data_df = adding_extra_columns(
            data_df,
            source_id,
            batch_creation_date,
            batch_id,
        )
    data_df.columns = data_df.columns.str.lower()

    column_names = f"""SELECT column_name FROM information_schema.columns
        WHERE table_name ~* '{table_name}' and column_name not in ('id')"""
    cursor_rds.execute(column_names)
    table_column_names = [column[0] for column in cursor_rds.fetchall()]
    df_cols = list(data_df.columns)

    new_columns = set(df_cols) - set(table_column_names)
    extra_cols = list(new_columns)
    table_column_names = table_column_names + extra_cols

    if extra_cols:
        for n in extra_cols:
            alter_query = f"""ALTER TABLE idx_stage.{table_name}
            ADD COLUMN  IF NOT EXISTS {n} TEXT"""
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
                "Alter Query": alter_query,
                "Insert Query": insert_query,
                "Message": f"Added new column {n} to {table_name}",
            }
            logger.info(log_msg)
            rds_connection.commit()

    cols = ",".join(list(data_df.columns))
    cols = (
        cols.replace(",order,", ',"order",')
        .replace(",table,", ',"table",')
        .replace(",group,", ',"group",')
    )
    data_values = [tuple(row) for row in data_df.values]

    insert_query = f"""
    INSERT INTO idx_stage.{table_name} ({cols}) VALUES %s
    """

    extras.execute_values(cursor_rds, insert_query, data_values)
    rds_connection.commit()

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "table_name": table_name,
        "download_count": len(data_df),
    }
    logger.info(log_msg)


def adding_extra_columns(generic_df, source_id, batch_creation_date, batch_id):
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
    meta_df = pd.concat([meta_df] * len(generic_df), ignore_index=True)
    generic_df = generic_df.reset_index(drop=True)

    generic_df = pd.concat([meta_df, generic_df], axis=1)
    return generic_df


def chunks_creation(df, key_column_names, chunk_size):
    value_keys = []

    for column in key_column_names:
        if column in df.columns:
            value_keys.extend(df[column].dropna().to_list())

    filtered_keys = [
        key for key in value_keys if key and key != ""
    ]  # remove empty string elements
    unique_value_keys = (
        pd.Series(filtered_keys, dtype=object).unique().tolist()
    )  # Remove duplicates

    key_chunks = [
        unique_value_keys[i : i + chunk_size]
        for i in range(0, len(unique_value_keys), chunk_size)
    ]  # Split into chunks of chunk_size

    return key_chunks  # type: ignore


def request_and_load_temp_table(source_data, flow_type, cursor_rds, rds_connection):
    """request_and_load_temp_table"""

    source_info = source_data["source_info"]
    status_column = source_info["status_column"]
    last_modified_date = source_data["last_modification_date"]
    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_type = source_info["source_type"]

    filter_value = f"ModificationTimestamp ge {last_modified_date}"

    params = {
        "$count": "true",
        "$orderby": "ModificationTimestamp asc",
        "$select": "ListingKeyNumeric,ModificationTimestamp,PhotosChangeTimestamp",
    }

    if flow_type == "sold":
        sold_column = source_info["sold_column"]
        sold_status = source_info["sold_status"]
        sold_date = source_data["sold_date"]

        filter_value = f"{sold_column} ge {sold_date} and {filter_value} and {status_column} in ({sold_status})"
        params["$orderby"] = f"{sold_column} asc, ModificationTimestamp asc"
        params["$select"] = params["$select"] + f",{sold_column}"

    elif flow_type in ["full_load", "respecs"]:
        active_status = source_info["active_status"]
        filter_value = filter_value + f" and {status_column} in ({active_status})"

    params["$filter"] = (
        filter_value
        + " and InternetEntireListingDisplayYN eq true and PropertyType ne 'ResidentialLease' and PropertyType ne 'CommercialLease' and PropertyType ne 'LandLease'"
    )

    auth = source_data["auth"]
    token = create_token(auth)
    url = auth["metadataUrl"]
    url = url.replace("$metadata", "Property")

    # Temp files go into {batch_id}/{lmd_date}_temp/ subfolder
    # batch_id = source_data["batch_id"]
    lmd_date = (
        str(last_modified_date).split(".", 1)[0].replace(":", "").replace("-", "")
    )
    temp_batch_id = f"{lmd_date}_temp"

    data_list = []
    top = 1000
    skip = 0
    params["$top"] = top
    headers = {"User-Agent": "Ylopo", "Authorization": f"Bearer {token}"}
    total_count = 1000  # initial dummy value

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

        try:
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
                    temp_batch_id,
                    "Property",
                    "Request",
                )
        except Exception as e:
            logger.error(
                {
                    "source_id": source_id,
                    "resource": "Property",
                    "temp_s3_request_error": str(e),
                    "Error At line": traceback.format_exc(),
                }
            )
        # ─────────────────────────────────────────────────────────────────────

        if skip + top >= total_count:
            break
        skip += top

    # ── S3: single combined upload of all pages
    try:
        df_all_pages = pd.DataFrame(data_list)
        if len(df_all_pages) > 0:
            Upload_data_into_S3_DataLake(
                df_all_pages,
                source_id,
                source_type,
                source_name,
                temp_batch_id,
                "Property",
                "0",
            )
    except Exception as e:
        logger.error(
            {
                "source_id": source_id,
                "resource": "Property",
                "temp_s3_data_error": str(e),
                "Error At line": traceback.format_exc(),
            }
        )
    # ─────────────────────────────────────────────────────────────────────────

    temp_data = pd.DataFrame(data_list)
    if len(temp_data) != 0:
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
                "ListingKeyNumeric": "ListingKey",
                "ModificationTimestamp": "modification_timestamp",
                "PhotosChangeTimestamp": "media_modification_timestamp",
            },
            inplace=True,
        )
        prestage_tables_insertion(
            temp_data,
            "temp_table",
            source_data["source_id"],
            source_data["source_name"],
            "Property",
            cursor_rds,
            rds_connection,
        )

        return True

    return False


def request_and_load_prestage_tables(source_data, cursor_rds, rds_connection):
    """request_and_load_prestage_tables"""

    source_id = source_data["source_id"]
    source_name = source_data["source_name"]
    source_info = source_data["source_info"]
    limit = source_info.get("limit", 1000)
    source_type = source_info.get("source_type")
    batch_id = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]

    bl_flag = source_data["batch_execution_params"]["bl_flag"]
    flow_type = source_data["flow_type"]

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

    cursor_rds.execute(query)
    temp_table_data = cursor_rds.fetchall()
    listing_keys_list = [val[0] for val in temp_table_data]

    listing_keys_chunks = [
        listing_keys_list[i : i + 50] for i in range(0, len(listing_keys_list), 50)
    ]
    del listing_keys_list

    query = f"""SELECT class_name FROM dev.class_metadata 
        WHERE source_id = {source_id} AND download_flag = 't' and active_flag = 't' ORDER BY id"""
    cursor_rds.execute(query)
    resources = cursor_rds.fetchall()
    resources = [r[0] for r in resources]
    resources.insert(
        0, resources.pop(resources.index("Property"))
    )  # set Property at first index

    auth = source_data["auth"]
    token = create_token(auth)
    url = auth["metadataUrl"]
    url = url.replace("$metadata", "")

    for resource_name in resources:
        endpoint_url = url + resource_name

        # ── PROPERTY & EXPANDED resources ────────────────────────────────────
        if resource_name == "Property":
            expandable_classes = [
                "Media",
                "PropertyRooms",
                "PropertyUnitTypes",
                "OpenHouse",
            ]

            # request_source now returns (df, url_list, filter_list)
            property_df, prop_s3_url, prop_s3_filter = request_source(
                endpoint_url,
                token,
                listing_keys_chunks,
                "ListingKeyNumeric",
                # "ListingId",
                expandable_classes,
            )

            if len(property_df) != 0:
                for expanded_class in expandable_classes:
                    # Extract the expanded child DataFrame
                    temp_df = (
                        property_df[expanded_class]
                        .explode()
                        .dropna()
                        .apply(pd.Series)
                        .reset_index(drop=True)
                    )

                    if len(temp_df) != 0:
                        # ── S3 archival for each expanded class ───────────────
                        # Build per-record url/filter aligned with the exploded
                        # rows.  Each child row inherits its parent property's
                        # url+filter;
                        try:
                            # Explode the per-property s3 context lists in the
                            # same way as the data was exploded above.
                            expanded_counts = property_df[expanded_class].apply(
                                lambda x: len(x) if isinstance(x, list) else 0
                            )
                            exp_url_list = []
                            exp_filter_list = []
                            for i, count in enumerate(expanded_counts):
                                if count > 0:
                                    parent_url = (
                                        prop_s3_url[i] if i < len(prop_s3_url) else ""
                                    )
                                    parent_filter = (
                                        prop_s3_filter[i]
                                        if i < len(prop_s3_filter)
                                        else ""
                                    )
                                    exp_url_list.extend([parent_url] * count)
                                    exp_filter_list.extend([parent_filter] * count)

                            # Pad/trim to match temp_df length
                            exp_url_list = (exp_url_list + [""] * len(temp_df))[
                                : len(temp_df)
                            ]
                            exp_filter_list = (exp_filter_list + [""] * len(temp_df))[
                                : len(temp_df)
                            ]

                            exp_s3_df = _build_s3_df(
                                temp_df.copy(),
                                exp_url_list,
                                exp_filter_list,
                                batch_creation_date,
                                source_id,
                                batch_id,
                            )
                            Upload_data_into_S3_DataLake(
                                exp_s3_df,
                                source_id,
                                source_type,
                                source_name,
                                batch_id,
                                expanded_class,
                            )
                            del exp_s3_df
                        except Exception as e:
                            logger.error(
                                {
                                    "source_id": source_id,
                                    "resource": expanded_class,
                                    "s3_error": str(e),
                                    "Error At line": traceback.format_exc(),
                                }
                            )

                        prestage_tables_insertion(
                            temp_df,
                            f"ps_rapi_{expanded_class}_{source_id}",
                            source_id,
                            source_name,
                            expanded_class,
                            cursor_rds,
                            rds_connection,
                            batch_creation_date,
                            batch_id,
                        )
                        property_df.drop(columns=[expanded_class], inplace=True)
                        del temp_df

                # ── S3 archival for Property ──────────────────────────────────
                try:
                    property_s3_df = _build_s3_df(
                        property_df.copy(),
                        prop_s3_url,
                        prop_s3_filter,
                        batch_creation_date,
                        source_id,
                        batch_id,
                    )
                    Upload_data_into_S3_DataLake(
                        property_s3_df,
                        source_id,
                        source_type,
                        source_name,
                        batch_id,
                        "Property",
                    )
                    del property_s3_df
                except Exception as e:
                    logger.error(
                        {
                            "source_id": source_id,
                            "resource": "Property",
                            "s3_error": str(e),
                            "Error At line": traceback.format_exc(),
                        }
                    )

                prestage_tables_insertion(
                    property_df,
                    f"ps_rapi_{resource_name}_{source_id}",
                    source_id,
                    source_name,
                    resource_name,
                    cursor_rds,
                    rds_connection,
                    batch_creation_date,
                    batch_id,
                )
                office_key_chunks = chunks_creation(
                    property_df,
                    [
                        "ListOfficeKey",
                        "CoListOfficeKey",
                        "BuyerOfficeKey",
                        "CoBuyerOfficeKey",
                    ],
                    50,
                )
                office_mlsid_chunks = chunks_creation(
                    property_df,
                    [
                        "ListOfficeMlsId",
                        "CoListOfficeMlsId",
                        "BuyerOfficeMlsId",
                        "CoBuyerOfficeMlsId",
                    ],
                    50,
                )
                agent_key_chunks = chunks_creation(
                    property_df,
                    [
                        "ListAgentKey",
                        "CoListAgentKey",
                        "BuyerAgentKey",
                        "CoBuyerAgentKey",
                    ],
                    50,
                )
                agent_mlsid_chunks = chunks_creation(
                    property_df,
                    [
                        "ListAgentMlsId",
                        "CoListAgentMlsId",
                        "BuyerAgentMlsId",
                        "CoBuyerAgentMlsId",
                    ],
                    50,
                )

            del property_df, listing_keys_chunks, expandable_classes
        # ── end PROPERTY & EXPANDED ───────────────────────────────────────────

        # ── MEMBER resource ───────────────────────────────────────────────────
        elif resource_name == "Member":
            agent_df = pd.DataFrame()
            member_s3_url = []
            member_s3_filter = []

            if len(agent_key_chunks) > 0:
                temp_df, agt_key_url, agt_key_filter = request_source(
                    endpoint_url, token, agent_key_chunks, "MemberKeyNumeric"
                )
                agent_df = pd.concat([agent_df, temp_df], ignore_index=True)
                member_s3_url += agt_key_url
                member_s3_filter += agt_key_filter

            if len(agent_mlsid_chunks) > 0:
                temp_df, agt_mlsid_url, agt_mlsid_filter = request_source(
                    endpoint_url, token, agent_mlsid_chunks, "MemberMlsId"
                )
                agent_df = pd.concat([agent_df, temp_df], ignore_index=True)
                member_s3_url += agt_mlsid_url
                member_s3_filter += agt_mlsid_filter

            if len(agent_df) != 0:
                # ── S3 archival for Member ────────────────────────────────────
                try:
                    mem_s3_df = _build_s3_df(
                        agent_df.copy(),
                        member_s3_url,
                        member_s3_filter,
                        batch_creation_date,
                        source_id,
                        batch_id,
                    )
                    Upload_data_into_S3_DataLake(
                        mem_s3_df,
                        source_id,
                        source_type,
                        source_name,
                        batch_id,
                        "Member",
                    )
                    del mem_s3_df
                except Exception as e:
                    logger.error(
                        {
                            "source_id": source_id,
                            "resource": "Member",
                            "s3_error": str(e),
                            "Error At line": traceback.format_exc(),
                        }
                    )

                prestage_tables_insertion(
                    agent_df,
                    f"ps_rapi_{resource_name}_{source_id}",
                    source_id,
                    source_name,
                    resource_name,
                    cursor_rds,
                    rds_connection,
                    batch_creation_date,
                    batch_id,
                )

            del agent_df, temp_df, agent_key_chunks, agent_mlsid_chunks
        # ── end MEMBER ────────────────────────────────────────────────────────

        # ── OFFICE resource ───────────────────────────────────────────────────
        elif resource_name == "Office":
            office_df = pd.DataFrame()
            office_s3_url = []
            office_s3_filter = []

            if len(office_key_chunks) > 0:
                temp_df, off_key_url, off_key_filter = request_source(
                    endpoint_url, token, office_key_chunks, "OfficeKeyNumeric"
                )
                office_df = pd.concat([office_df, temp_df], ignore_index=True)
                office_s3_url += off_key_url
                office_s3_filter += off_key_filter

            if len(office_mlsid_chunks) > 0:
                temp_df, off_mlsid_url, off_mlsid_filter = request_source(
                    endpoint_url, token, office_mlsid_chunks, "OfficeMlsId"
                )
                office_df = pd.concat([office_df, temp_df], ignore_index=True)
                office_s3_url += off_mlsid_url
                office_s3_filter += off_mlsid_filter

            if len(office_df) != 0:
                # ── S3 archival for Office ────────────────────────────────────
                try:
                    off_s3_df = _build_s3_df(
                        office_df.copy(),
                        office_s3_url,
                        office_s3_filter,
                        batch_creation_date,
                        source_id,
                        batch_id,
                    )
                    Upload_data_into_S3_DataLake(
                        off_s3_df,
                        source_id,
                        source_type,
                        source_name,
                        batch_id,
                        "Office",
                    )
                    del off_s3_df
                except Exception as e:
                    logger.error(
                        {
                            "source_id": source_id,
                            "resource": "Office",
                            "s3_error": str(e),
                            "Error At line": traceback.format_exc(),
                        }
                    )

                # DB insert (unchanged)
                prestage_tables_insertion(
                    office_df,
                    f"ps_rapi_{resource_name}_{source_id}",
                    source_id,
                    source_name,
                    resource_name,
                    cursor_rds,
                    rds_connection,
                    batch_creation_date,
                    batch_id,
                )

            del office_df, temp_df, office_key_chunks, office_mlsid_chunks
        # ── end OFFICE ────────────────────────────────────────────────────────

        else:
            pass

    return True


def validation_func(source_data, cursor_rds, rds_connection, cursor_homelisting):
    """Validation Function"""

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
    max_last_modified_date = None
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

    logger.info(
        {
            f"Source ID": {source_id},
            f"Source Name": {source_name},
            f"Total temp table Count": {total_count},
            f"Flow Type": {flow_type},
            f"Max Last Modified Date": {max_last_modified_date},
            f"Latest Listing Date": {latest_listing_date},
        }
    )
    if source_data["temp_table_status"]:
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
        # get latest_listing_date for respecs_finish_date  (only for API sources)
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

    return source_data


def data_download_func(source_data, cursor_rds, rds_connection):
    """Data Download Function"""
    source_id = source_data["source_id"]
    batch_id = source_data["batch_id"]
    run_host = source_data["run_host"]
    source_name = source_data["source_name"]
    flow = source_data["flow_type"]
    source_info = source_data["source_info"]
    limit = source_info.get("limit", 1000)
    source_type = source_info.get("source_type")
    mls_board = source_info.get("mls_board")
    batch_creation_date = source_data["batch_creation_date"]
    last_modified_date = source_data["last_modification_date"]
    source_data["status"] = False

    download_status = request_and_load_prestage_tables(
        source_data,
        cursor_rds,
        rds_connection,
    )

    success_response = {
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

    return success_response


def lambda_handler(event, context):
    """Main Lambda Handler Function"""
    logger.info(event)

    listing_database = os.environ.get("listingDatabase")
    rds_database = os.environ.get("rdsDatabase")
    sql_exec_limit = context.get_remaining_time_in_millis()
    db_secret_rds = fetch_secrets(rds_database)
    db_secret_listing = fetch_secrets(listing_database)
    rds_connection = db_conn(db_secret_rds, sql_exec_limit)
    homelisting_connection = db_conn(db_secret_listing, sql_exec_limit)
    cursor_rds = rds_connection.cursor()  # type: ignore
    cursor_homelisting = homelisting_connection.cursor()  # type: ignore

    final_response = None
    try:

        if event["download_flag"]:
            final_response = data_download_func(event, cursor_rds, rds_connection)

        else:
            final_response = validation_func(
                event, cursor_rds, rds_connection, cursor_homelisting
            )

    except Exception as e:
        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(log_msg)

        logger.error(event)
        final_response = event

    finally:
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()
        if cursor_rds:
            cursor_rds.close()
            rds_connection.close()

    return final_response
