import json
import boto3
import pandas as pd
import requests
import urllib.parse
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone
import numpy as np
import os
import io
import time
import traceback
from helper import LogData, LogMessage, log_message


# Function to fetch secrets from AWS Secrets Manager
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
        options=f"-c statement_timeout={sqlExecLimit if sqlExecLimit else 60000}",
    )

    return conn


class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response_db = client.get_secret_value(SecretId=secret_name)
            # Decrypts secret using the associated KMS key.
            secret_db = get_secret_value_response_db["SecretString"]
            secret_dict_db = json.loads(secret_db)
            return secret_dict_db
        except Exception as e:
            raise e


def create_token(client_id, client_secret, loginUrl):

    payload = {
        "client_id": str(client_id),
        "client_secret": str(client_secret),
        "grant_type": "client_credentials",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.request("POST", loginUrl, headers=headers, data=payload)

    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        # Log token generation failure
        logs = {"Token Generation": "Failed", "Status Code": response.status_code}
        log_data = LogData(event=logs)
        log_message(LogMessage("ERROR", "received", log_data))


def api_call_and_get_count(last_modification_date, loginUrl, token):

    loginUrl = "https://api.listhub.com/odata/Property"

    params = {
        "$filter": "ModificationTimestamp ge {} and PropertyType ne 'Residential Lease' and PropertyType ne 'CommercialnLease'".format(
            last_modification_date
        ),
        "$count": "true",
        "$select": "ModificationTimestamp",
        "$orderby": "ModificationTimestamp desc",
        "$top": 1,
    }

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url=loginUrl, headers=headers, params=params)
    response.raise_for_status()
    data = json.loads(response.text)
    total_count = data["@odata.count"]
    if total_count != 0:
        latest_listing_date = data["value"][0]["ModificationTimestamp"]
        return total_count, latest_listing_date
    else:
        return 0, last_modification_date


def db_conn(db_secret):
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
        )
        response_dict_success = {"status": "Success"}
        log_data = LogData(event=response_dict_success)
        log_message(LogMessage("INFO", "Connection established successfully", log_data))
        return connection
    except Exception as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        log_data = LogData(event=log_msg)
        log_message(LogMessage("ERROR", "received", log_data))


def execute_query(connection, query, cursor, query_mode=None):

    log_msg = {"Executed Query": query}
    log_data = LogData(event=log_msg)
    log_message(LogMessage("INFO", "received", log_data))
    cursor.execute(query)

    # If query_mode == None; the query is for selection, otherwise it's for insertion.
    if query_mode == None:
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
    pentaho_db_con,
    serverless_db_con,
    source_id,
    cursor_serverless,
    cursor_pentaho,
    flow_type,
    rolling_window_offset=None,
):

    if flow_type == "backlog":

        query_for_last_modified_date_serverless = """
            SELECT
                bl_start_date::timestamp(0)
            FROM 
                stage.serverless_idx_loads
            WHERE
                source_id = {} 
            """.format(
            source_id
        )

    elif flow_type == "respecs":

        query_for_last_modified_date_serverless = """
            SELECT
                respecs_start_date::timestamp(0)
            FROM 
                stage.serverless_idx_loads
            WHERE
                source_id = {} 
            """.format(
            source_id
        )

    elif flow_type == "rolling_window":

        # if rolling_window_offset is not None:
        # then we need to subtract the rolling_window_offset from the last_modified_date
        query_for_last_modified_date_serverless = """
            SELECT last_modified_date::timestamp(0) - interval '{1} hours' FROM stage.serverless_idx_loads
            WHERE source_id = {0} """.format(
            source_id, rolling_window_offset
        )

    else:

        query_for_last_modified_date_serverless = """
            SELECT
                last_modified_date::timestamp(0)
            FROM 
                stage.serverless_idx_loads
            WHERE
                source_id = {} 
            """.format(
            source_id
        )

    max_date_serverless = execute_query(
        serverless_db_con, query_for_last_modified_date_serverless, cursor_serverless
    )

    formatted_date_serverless = formatted_date(max_date_serverless)
    return formatted_date_serverless


def formatted_date(date):
    # If no date found then use dummy date
    dumy_date = "1990-01-01 00:00:00"
    naive_datetime = datetime.strptime(dumy_date, "%Y-%m-%d %H:%M:%S")
    original_datetime_utc = naive_datetime.replace(tzinfo=timezone.utc)
    formatted_datetime_utc = original_datetime_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # date may be None or a tuple
    if date == None:
        return formatted_datetime_utc

    original_datetime_aware = datetime.strptime(str(date[0]), "%Y-%m-%d %H:%M:%S")
    formatted_time = original_datetime_aware.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return formatted_time

def remove_characters(value):
    # First check if it's an array-like object
    if isinstance(value, (pd.Series, np.ndarray, list)):
        if len(value) == 0:
            return None
        # Convert array to string
        value_str = str(value)
    # Then handle None/NaN values
    elif pd.isna(value):
        return None
    # Handle remaining scalar values
    else:
        value_str = str(value)

    # Clean the string
    return (
        value_str.replace("[", "")
        .replace("]", "")
        .replace("'", "")
        .replace("{", "")
        .replace("}", "")
    )


def clean_value(value):
    # First, handle the case where value is an array-like object
    if isinstance(value, (pd.Series, np.ndarray, list)):
        if len(value) == 0:
            return None
        # For non-empty arrays, return as is
        return value

    # Now handle scalar values
    if pd.isna(value):
        return None

    if isinstance(value, str) and value.lower() in ["none", "nan", "na", ""]:
        return None

    # Return everything else as is
    return value


def table_creation_and_loading(
    df_instance,
    table_name,
    source_id,
    source_name,
    resource_name,
    cursor_serverless,
    serverless_db_con,
):

    max_column_name_length = 62

    columns_to_rename = {
        col: col[:max_column_name_length]
        for col in list(df_instance.columns)
        if len(col) > max_column_name_length
    }

    df_instance.rename(columns=columns_to_rename, inplace=True)

    df_instance.fillna(pd.NaT)
    df_instance.fillna("")
    df_instance = df_instance.apply(lambda col: col.map(remove_characters))
    df_instance = df_instance.apply(lambda col: col.map(clean_value))
    column_names = """SELECT column_name FROM information_schema.columns WHERE table_name = '{}' AND column_name NOT IN ('pid')""".format(
        table_name
    )
    cursor_serverless.execute(column_names)
    table_column_names = [column[0] for column in cursor_serverless.fetchall()]
    df_instance.columns = df_instance.columns.str.lower()
    df_cols = list(df_instance.columns)
    extra_cols = set(df_cols) - set(table_column_names)
    # droping duplicate columns
    df_instance = df_instance.loc[:, ~df_instance.columns.duplicated()]

    if extra_cols:
        for n in extra_cols:
            alter_query = """ALTER TABLE idx_stage.{0} ADD COLUMN {1} TEXT""".format(
                table_name, n
            )
            cursor_serverless.execute(alter_query)
            # Sync metadata for new columns
            insert_query = f""" INSERT INTO dev.field_metadata 
                (source_id, source_name, resource_name, class_name, long_name, renamed_long_name) 
                VALUES ({source_id}, '{source_name}', '{resource_name}', '{resource_name}', '{n}', '{n}'); 
                """
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "resource_name": resource_name,
                "class_name": resource_name,
                "column_name": n,
                "alter_query": alter_query,
                "inser_query": insert_query
            }
            log_data = LogData(log_msg)
            log_message(LogMessage("INFO", "received", log_data))
            cursor_serverless.execute(insert_query)
            serverless_db_con.commit()
            serverless_db_con.commit()

    cols = ",".join(list(df_instance.columns))
    data_values = [tuple(row) for row in df_instance.values]

    insert_query = """INSERT INTO idx_stage.{0} ({1}) VALUES %s""".format(
        table_name, cols
    )
    extras.execute_values(cursor_serverless, insert_query, data_values)
    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "resource_name": resource_name,
        "class_name": resource_name,
        "message": f'{len(df_instance)} Rows Inserted into {table_name} Table'
    }
    log_data = LogData(log_msg)
    log_message(LogMessage("INFO", "received", log_data))

    serverless_db_con.commit()
    del df_instance


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


def api_call_and_load_tables(source_data, token, cursor, connection):

    batch_id = source_data["batch_id"]
    batch_creation_date = source_data.get("batch_creation_date")
    source_id = source_data["source_id"]
    run_host = source_data["run_host"]
    flow_type = source_data["flow_type"]
    source_type = source_data["source_info"].get("source_type")
    limit = source_data["source_info"].get("limit")
    mls_board = source_data["source_info"].get("mls_board")
    metadataUrl = source_data["auth"]["metadataUrl"]
    source_name = source_data["source_name"]
    last_modification_date = source_data["last_modification_date"]

    classes_query = """SELECT class_name FROM dev.class_metadata WHERE source_id = {} AND download_flag = 'true' AND class_name = 'Property' ORDER BY id""".format(
        source_id
    )
    cursor.execute(classes_query)
    classes = cursor.fetchall()
    classes = [k[0] for k in classes]
    property_index = classes.index("Property")
    classes.insert(0, classes.pop(property_index))
    loginUrl = metadataUrl.replace("$metadata", "")

    headers = {"Authorization": f"Bearer {token}"}

    for listhub_class in classes:
        top = 500
        skip = 0
        if listhub_class == "Property":
            url_endpoint = loginUrl + listhub_class
            listhub_list_data = []
            listhub_media = []
            listhub_openhouse = []
            listhub_customfields = []
            individual_media_recs = []
            individual_openhouse_recs = []

            total_count = None

            while skip < limit:
                value = "ModificationTimestamp gt {} and PropertyType ne 'Commercial Lease' and PropertyType ne 'Residential Lease'".format(
                    last_modification_date
                )

                full_url = (
                    f"{url_endpoint}?"
                    f"$filter={urllib.parse.quote(value)}&"
                    f"$count=true&"
                    f"$top={top}&"
                    f"$skip={skip}&"
                    f"$orderby=ModificationTimestamp asc"
                )

                try:
                    response = requests.get(url=full_url, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                    listhub_list_data.extend(data["value"])
                    data['request'] = full_url
                    if total_count is None:
                        total_count = data['@odata.count']

                    if skip == 0:
                        data_dict = [{
                            'Request_Url': full_url,
                            'source_count': total_count
                        } ]  
                        df_upload = pd.DataFrame(data_dict)                            
                        df_upload = adding_extra_columns(df_upload, batch_creation_date, source_id, batch_id)
                        # return {}
                        Upload_data_into_S3_DataLake (df_upload, source_id, source_type, source_name ,batch_id, listhub_class, 'Request')

                    # Loading into S3 data lake
                    df_upload = pd.DataFrame(data)
                    
                    df_upload = adding_extra_columns(df_upload, batch_creation_date, source_id, batch_id)
                    # return {}
                    Upload_data_into_S3_DataLake (df_upload, source_id, source_type, source_name ,batch_id, listhub_class, skip)
                    
                    OpenHouse_upload = []
                    Media_upload = []
                    CustomFields_upload = []

                    for i in data["value"]:
                        
                        for openhouse_dic in i.get("OpenHouse"):
                             listhub_openhouse.append(openhouse_dic)
                        #     OpenHouse_upload.append(openhouse_dic)
                        openhouse_list = i.get("OpenHouse")
                        #listhub_openhouse.append(openhouse_list)
                        OpenHouse_upload.append(openhouse_list)    

                        media_list = i.get("Media", [])
                        for media in media_list:
                            media['ListingKey'] = i['ListingKey']
                        listhub_media.extend(media_list)
                        Media_upload.extend(media_list)

                        customfields = i.get("CustomFields", {})
                        customfields['ListingKey'] = i['ListingKey']
                        listhub_customfields.append(customfields)
                        CustomFields_upload.append(customfields)
                        
                   #openhouse   
                    individual_openhouse_recs = []
                    for openhouse in OpenHouse_upload:
                        if openhouse is not None:
                            for o in openhouse:
                                individual_openhouse_recs.append(o)

                    if individual_openhouse_recs:
                        df_openhouse_upload = pd.DataFrame(individual_openhouse_recs)
                        df_openhouse_upload = adding_extra_columns(df_openhouse_upload, batch_creation_date, source_id, batch_id)
                        Upload_data_into_S3_DataLake(df_openhouse_upload, source_id, source_type, source_name, batch_id, listhub_class + '_OpenHouse', skip)
                        OpenHouse_upload = []  # clear after upload
                     # Media
                     
                     # Upload Media to S3 for current iteration (based on skip)
                    if Media_upload:
                        df_media_upload = pd.DataFrame(Media_upload)
                        df_media_upload = adding_extra_columns(df_media_upload, batch_creation_date, source_id, batch_id)
                        Upload_data_into_S3_DataLake(df_media_upload, source_id, source_type, source_name, batch_id, listhub_class + '_Media', skip)
                        Media_upload = []  # Clear it for next iteration
                    
                    # Upload CustomFields to S3 for current iteration
                    if CustomFields_upload:
                        df_cf_upload = pd.DataFrame(CustomFields_upload)
                        df_cf_cleaned = adding_extra_columns(df_cf_upload, batch_creation_date, source_id, batch_id)
                        Upload_data_into_S3_DataLake(df_cf_cleaned, source_id, source_type, source_name, batch_id, listhub_class + '_CustomFields', skip)
                        CustomFields_upload = []  # clear after upload
                    
                  
                    
                    if skip + top >= total_count:
                        break
                    
                    skip += top
                    time.sleep(2)

                except requests.exceptions.RequestException as e:
                    time.sleep(4)
                    response = requests.get(url=full_url, headers=headers)
                    response.raise_for_status()
                    data = json.loads(response.text)
                    listhub_list_data.extend(data["value"])

                    for i in data["value"]:
                        listhub_openhouse.extend(i.get("OpenHouse"))
                        media_list = i.get("Media", [])
                        for media in media_list:
                            media["ListingKey"] = i["ListingKey"]
                        listhub_media.extend(media_list)

                        customfields = i.get("CustomFields", {})
                        customfields["ListingKey"] = i["ListingKey"]
                        listhub_customfields.append(customfields)

                    if total_count is None:
                        total_count = data["@odata.count"]

                    if skip + top >= total_count:
                        break

                    skip += top

            # Media
            individual_media_recs = listhub_media
            media_table = "ps_listhub2_media_905"
            df = pd.DataFrame(individual_media_recs)
            df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
            table_creation_and_loading(
                df, media_table, source_id, source_name, "Media", cursor, connection
            )

            # OpenHouse
            if len(listhub_openhouse) != 0:
                openhouse_table = "ps_listhub2_openhouse_905"

                df = pd.DataFrame(listhub_openhouse)
                df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(
                    df,
                    openhouse_table,
                    source_id,
                    source_name,
                    "OpenHouse",
                    cursor,
                    connection,
                )

            # CustomFields
            if len(listhub_customfields) != 0:
                df = pd.DataFrame(listhub_customfields)
                cf_table = "ps_listhub2_customfields_905"
                df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
                table_creation_and_loading(
                    df,
                    cf_table,
                    source_id,
                    source_name,
                    "CustomField",
                    cursor,
                    connection,
                )

            # Property
            filtered_cols_query = """SELECT long_name FROM dev.field_metadata WHERE source_id = {0} AND class_name = '{1}' AND download_flag = false""".format(
                source_id, listhub_class
            )
            cursor.execute(filtered_cols_query)
            filtered_cols = cursor.fetchall()
            final_columns = [k[0] for k in filtered_cols]

            df = pd.DataFrame(listhub_list_data)
            df.drop(columns=final_columns, inplace=True, errors="ignore")
            df = df.drop(
                columns=["@odata.id", "Media", "OpenHouse", "CustomFields"],
                errors="ignore",
            )
            property_table = "ps_listhub2_property_905"
            df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
            table_creation_and_loading(
                df,
                property_table,
                source_id,
                source_name,
                listhub_class,
                cursor,
                connection,
            )

    return {
        "source_id": source_id,
        "source_name": source_name,
        "source_type": source_type,
        "mls_board": mls_board,
        "flow_type": flow_type,
        "batch_creation_date": batch_creation_date,
        "batch_id": batch_id,
        "last_refresh_date": last_modification_date,
        "status": True,
        "run_host": run_host,
        "success": False,
    }

def Upload_data_into_S3_DataLake (df_upload, source_id,source_type,source_name,batch_id,class_Name,Skip):
   
    # Construct filename and folder path
    filename = f"{source_name}_{class_Name}_{Skip}.parquet"
    folder_path = f"{source_type}/{source_id}_{source_name}/{batch_id}/{class_Name}/"
    s3_key = folder_path + filename

    df_upload.columns = df_upload.columns.map(lambda x: x.split('.')[-1] if '.' in x else x)
    for col in df_upload.columns:
        # Convert all values to strings first, then handle empty/null values
        df_upload[col] = df_upload[col].astype(str).replace(['nan', 'None', ''], None)

    # Convert DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('bucket_name')

    # Optional: Ensure folders exist (not strictly required in S3 since it's flat storage)
    # You can create a zero-byte object as folder markers if needed
    ensure_folder_structure(s3, bucket_name, folder_path)

    # Upload parquet file
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

    return {
        'statusCode': 200,
        'body': f'File uploaded to s3://{bucket_name}/{s3_key}'
    }

def ensure_folder_structure(s3, bucket, path):
    # S3 doesn't require folders but you can create empty placeholders
    parts = path.strip('/').split('/')
    cumulative_path = ''
    for part in parts:
        cumulative_path += part + '/'
        s3.put_object(Bucket=bucket, Key=cumulative_path)  # Creates "folder"


def validation_func(
    pentaho_db_con,
    serverless_db_con,
    cursor_serverless,
    cursor_pentaho,
    source_data,
    token,
):

    source_id = source_data.get("source_id")
    source_info = source_data.get("source_info")
    loginUrl = source_data["auth"]["metadataUrl"]
    runtime_count = source_data.get("runtime_count")
    flow_type = source_info.get("flow_type", "normal")
    rolling_window_batch = source_info["rolling_window_batch"]
    batch_execution_params = source_data["batch_execution_params"]
    bl_flag = batch_execution_params["bl_flag"]
    itr_value = batch_execution_params["itr_value"]
    respecs_flag = batch_execution_params["respecs_flag"]
    rolling_window_offset = None

    if flow_type not in ["sold", "full_load"]:

        if respecs_flag is True and (runtime_count % itr_value != 0):
            flow_type = "respecs"

        elif bl_flag is True and (runtime_count % itr_value != 0):
            flow_type = "backlog"

        elif runtime_count % rolling_window_batch == 0:
            rolling_window_offset = source_info["rolling_window_offset"]
            flow_type = "rolling_window"

    # GET LAST MODIFICATION DATE FROM IDX SERVERLESS LOADS TABLE
    max_last_modified_date = get_max_last_modified_date(
        pentaho_db_con,
        serverless_db_con,
        source_id,
        cursor_serverless,
        cursor_pentaho,
        flow_type,
        rolling_window_offset,
    )
    source_data["last_modification_date"] = max_last_modified_date

    # GET ROW COUNT AND LATEST LISTING TIMESTAMP
    total_count, latest_listing_date = api_call_and_get_count(
        max_last_modified_date, loginUrl, token
    )

    source_data["row_count"] = int(total_count)
    source_data["flow_type"] = flow_type
    source_data["latest_listing_date"] = latest_listing_date
    source_data["download_flag"] = True

    log_data = LogData(event=source_data, query=total_count)
    log_message(
        LogMessage(
            "INFO",
            "{} Total_count for source_id {}, latest listing timestamp {} ".format(
                total_count, source_id, latest_listing_date
            ),
            log_data,
        )
    )

    log_data = LogData(event=source_data)
    log_message(LogMessage("INFO", "received", log_data))
    return source_data


def lambda_handler(event, context):

    log_data = LogData(event)
    log_message(LogMessage("INFO", "received", log_data))

    # DATABASE CONNECTIONS SETUP
    rdsDatabase = os.environ.get("rdsDatabase")
    listingDatabase = os.environ.get("listingDatabase")
    sqlExecLimit = context.get_remaining_time_in_millis()
    db_secret_dev = SecretManagerHelper.get_secret(rdsDatabase, "us-west-2")
    db_secret_stage = SecretManagerHelper.get_secret(listingDatabase, "us-west-2")
    serverless_db_con = setup_db_connection(db_secret_dev, sqlExecLimit)
    pentaho_db_con = setup_db_connection(db_secret_stage, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    cursor_pentaho = pentaho_db_con.cursor()
    source_data = event

    try:

        # FETCHING KEYWORDS FROM EVENT
        loginUrl = source_data["auth"]["loginUrl"]
        client_id = source_data["auth"]["user"]
        client_secret = source_data["auth"]["password"]
        token = source_data["auth"]["password"]
        download_flag = source_data.get("download_flag")

        # GENERATES TOKEN
        token = create_token(client_id, client_secret, loginUrl)
        # loginUrl = source_data["auth"]["metadataUrl"]
        response = None
        if download_flag:
            response = api_call_and_load_tables(
                source_data, token, cursor_serverless, serverless_db_con
            )

        else:
            response = validation_func(
                pentaho_db_con,
                serverless_db_con,
                cursor_serverless,
                cursor_pentaho,
                source_data,
                token,
            )

        return response

    except Exception as e:
        # LOGGING AN ERROR MESSAGE
        log_msg = {
            "source_id": source_data["source_id"],
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        source_data.update(log_msg)
        log_data = LogData(source_data)
        log_message(LogMessage("ERROR", "received", log_data))
        return source_data

    finally:
        cursor_serverless.close()
        cursor_pentaho.close()
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()
