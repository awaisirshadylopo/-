import traceback
import datetime
import json
import os
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import boto3
import psycopg2
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

serverless_db_con = None
pentaho_db_con = None


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
        except ClientError as e:
            raise e


# def db_conn(db_secret, sql_execlimit):

#     db_username = db_secret.get("username")
#     db_password = db_secret.get("password")
#     db_host = db_secret.get("host")
#     db_name = db_secret.get("dbname")
#     db_port = db_secret.get("port")

#     try:
#         connection = psycopg2.connect(
#             database=db_name,
#             user=db_username,
#             password=db_password,
#             host=db_host,
#             port=db_port,
#             options=f"-c statement_timeout={sql_execlimit}",
#         )
#         response_dict_success = {
#             "status": "Success",
#             "message": "Connection established successfully",
#         }
#         logger.info(response_dict_success)
#         return connection
#     except Exception as e:
#         log_msg = {
#             "Error": e,
#             "Error At line": traceback.format_exc(),
#             "message": "Connection failed",
#         }
#         logger.error(log_msg)


def get_connection(secret, connection, sql_execlimit):
    try:
        if connection is not None and connection.closed == 0:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return connection
    except Exception:
        pass

    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        connect_timeout=15,
        options=f"-c statement_timeout={sql_execlimit}",
    )


def get_tables(cursor, source_id, func_name):

    if "bright" in func_name.lower():
        func_name = "bright"
    elif "rets" in func_name.lower():
        func_name = "rets"

    tables_query = """
        select table_name from information_schema.tables
        where table_schema ~*'idx_stage' and table_name ~*'ps_' and table_name ~*'{1}'
        union all
        select table_name from information_schema.tables
        where table_schema ~*'idx_stage' and table_name ~*'ps_{0}_' and table_name !~*'[0-9]+';
    """.format(func_name, source_id)

    cursor.execute(tables_query)
    tables = cursor.fetchall()
    tables_list = [t[0] for t in tables]
    return tables_list


def execute_query(connection, query, cursor, query_mode=None):
    log_msg = {"Executed Query": query}
    logger.info(log_msg)
    cursor.execute(query)

    # IF QUERY_MODE IS NONE, DEFINE THAT QUERY IS FOR SELECTION, OTHERWISE, QUERY_MODE IS INSERT
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


def delete_data_prestage(connection, cursor, source_id, source_info):

    func_name = source_info["func_name"]
    deletion_tables = get_tables(cursor, source_id, func_name)
    delete_sql = ""

    for del_t in deletion_tables:

        delete_sql = f"DELETE FROM idx_stage.{del_t} where source_id = {source_id}"

        cursor.execute(delete_sql)
        connection.commit()
        msg1 = "ROWS DELETED {} against source_id {}:".format(
            cursor.rowcount, source_id
        )

        log_msg = {"stats": msg1, "query": delete_sql}
        logger.info(log_msg)


def insert_into_etl_batches(pentaho_db_con, source_id, data, run_host, cursor_pentaho):

    insert_into_etl_batches = """
        INSERT INTO stage.etl_batches (load_date, source_id, source, load_inactive_lst_status, run_host)  
        VALUES (now() , {},'{}','{}','{}');
    """.format(
        source_id,
        data["source_name"],
        "in_progress",
        run_host,
    )

    execute_query(pentaho_db_con, insert_into_etl_batches, cursor_pentaho, "insert")

    latest_batch_serverless = """
    select batch_id, load_date from stage.etl_batches where source_id = {0} ORDER BY batch_id DESC limit 1
    """.format(source_id)

    result = execute_query(pentaho_db_con, latest_batch_serverless, cursor_pentaho)

    return (
        result[0],
        result[1].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    )


def check_respecs(cursor_serverless, source_id):
    sql = f"select respecs_start_date from stage.serverless_idx_loads where source_id ={source_id};"
    cursor_serverless.execute(sql)
    result = cursor_serverless.fetchone()

    if result[0] is not None:
        return True
    else:
        return False


def respecs_creation(
    source_id,
    latest_listing_date,
    source_data,
    cursor_pentaho,
    pentaho_db_con,
    cursor_serverless,
    serverless_db_con,
):

    query = f"""
        UPDATE source
        SET batch_execution_params = jsonb_set(batch_execution_params::jsonb, '{{"respecs_flag"}}', 'true'::jsonb)
        WHERE id = {source_id};
        """
    cursor_pentaho.execute(query)
    pentaho_db_con.commit()

    sql = f"""
        UPDATE stage.serverless_idx_loads
        SET respecs_finish_date = '{latest_listing_date}'
        WHERE source_id = {source_id};"""

    cursor_serverless.execute(sql)
    serverless_db_con.commit()
    source_data["batch_execution_params"]["respecs_flag"] = True


def cleaning_func(source_id, batch_id, conn, cursor, rd_cursor):

    insert_listing_ids = "select source_listing_id  from stage.etl_direct_idx_insert_listings where source_id = {0} and batch_id = {1} ;".format(
        source_id, batch_id
    )
    rd_cursor.execute(insert_listing_ids)
    source_listing_id = rd_cursor.fetchall()
    # Exit early if no listing IDs found
    if len(source_listing_id) == 0:
        log_msg = {
            "source_id": source_id,
            "batch_id": batch_id,
            "Message": "No listing IDs found for deletion.",
        }
        logger.info(log_msg)
        return
    source_listing_id = [i[0] for i in source_listing_id]
    query = """ select id from public.listing where load_flag = true and source_id = {0} and batch_id = {1} and source_listing_id in {2};""".format(
        source_id, batch_id, str(source_listing_id).replace("[", "(").replace("]", ")")
    )
    cursor.execute(query)
    inserted_listing_id = cursor.fetchall()

    target_listing_ids = [i[0] for i in inserted_listing_id]
    target_listing_ids.append(0)
    target_listing_ids = str(target_listing_ids).replace("[", "(").replace("]", ")")

    tables = [
        "public.listing",
        "public.listing_address_standard",
        "public.listing_address",
        "public.listing_attribute",
        "public.listing_attribute_2",
        "public.listing_attribute_3",
        "public.listing_attribute_custom",
        "public.listing_attribute_custom_2",
        "public.listing_attribute_custom_3",
        "public.listing_description",
        "public.listing_openhouse",
        "public.listing_participant_rel",
        "public.listing_photo",
        "public.listing_price_history",
        "public.listing_real_estate_office_rel",
        "public.listing_school",
        "listing_school_district",
        "listing_attribute_custom_4",
        "listing_marketing_info",
        "listing_photo_prefetch",
        "listing_property_type_search",
    ]

    for table_name in tables:
        sqlDeleteforAllTable = (
            "DELETE FROM {0} WHERE batch_id = {1} and listing_id in {2} ".format(
                table_name, batch_id, target_listing_ids
            )
        )
        if table_name == "public.listing":
            sqlDeleteforAllTable = (
                "DELETE FROM {0} WHERE batch_id = {1} and id in {2}".format(
                    table_name, batch_id, target_listing_ids
                )
            )

        cursor.execute(sqlDeleteforAllTable)
        log_msg = {
            "source_id": source_id,
            "batch_id": batch_id,
            "Message": f"Deteted Count is {cursor.rowcount}",
            "Delete Query": sqlDeleteforAllTable,
        }
        logger.info(log_msg)

    conn.commit()


def backlog_creation(
    source_id,
    latest_listing_date,
    last_modification_date,
    source_data,
    cursor_pentaho,
    pentaho_db_con,
    cursor_serverless,
    serverless_db_con,
):

    query = f"""
        UPDATE source
        SET batch_execution_params = jsonb_set(batch_execution_params::jsonb, '{{"bl_flag"}}', 'true'::jsonb)
        WHERE id = {source_id};
        """
    cursor_pentaho.execute(query)
    pentaho_db_con.commit()

    sql = f"""
        UPDATE stage.serverless_idx_loads
        SET last_modified_date = '{latest_listing_date}',
            bl_finish_date = '{latest_listing_date}',  
            bl_start_date = 
            CASE 
                WHEN bl_start_date IS NULL OR '{last_modification_date}' < bl_start_date 
                THEN '{last_modification_date}' 
                ELSE bl_start_date 
            END
        WHERE source_id = {source_id};
        """

    cursor_serverless.execute(sql)
    serverless_db_con.commit()

    source_data["batch_execution_params"]["bl_flag"] = True


def failed_batch_initialization(pentaho_db_con, cursor_pentaho, source_data):
    data_dictionay = {}
    source_id = source_data.get("source_id")
    check_the_source_latest_batch = """
    SELECT e.batch_id, e.load_inactive_lst_status,e.run_host, e.source,e.load_date
    FROM STAGE.etl_batches e 
    where e.source_id = {}
    ORDER BY batch_id DESC LIMIT 1
    """.format(source_id)
    result = execute_query(
        pentaho_db_con, check_the_source_latest_batch, cursor_pentaho
    )
    batch_id = data_dictionay["batch_id"] = result[0]
    data_dictionay["status"] = result[1]
    data_dictionay["run_host"] = result[2]
    data_dictionay["source_name"] = result[3]
    creation_date = str(result[4])

    # NEW BATCH CREATION
    if data_dictionay["status"] == "Completed":

        batch_id, batch_start_time = insert_into_etl_batches(
            pentaho_db_con,
            source_id,
            data_dictionay,
            data_dictionay["run_host"],
            cursor_pentaho,
        )
    # if data_dictionay["status"] == "in_progress":

    #     source_data["batch_initialized"] = False
    #     source_data["batch_id"] = batch_id
    #     return source_data

    source_data["batch_id"] = batch_id
    log_msg = {
        # "batch_id": batch_id,
        "'Error'": "Error in Validation Lambda",
        "Error": source_data["Error"],
    }

    raise Exception(log_msg)


# filter-out listings from idx_stage.temp_table that are neither insert-able nor update-able
def filter_temp_listings(
    source_id,
    source_type,
    flow_type,
    serverless_db_con,
    cursor_serverless,
    cursor_pentaho,
):

    # if flow_type == "sold":
    cursor_serverless.execute(f"""
        UPDATE idx_stage.temp_table set download_flag = 'f' 
            where source_id = {source_id} 
            and sold_date::date < current_date - interval '3 years';
    """)
    serverless_db_con.commit()

    cursor_serverless.execute(f"""SELECT distinct listingkey as source_listing_id,
            COALESCE(modification_timestamp, CURRENT_TIMESTAMP)::timestamp,
            COALESCE(media_modification_timestamp, CURRENT_TIMESTAMP)::timestamp
            FROM idx_stage.temp_table
            WHERE source_id = {source_id} and download_flag = 't' and respecs_flag = 'f';
    """)
    temp_listings = cursor_serverless.fetchall()

    if temp_listings and len(temp_listings) > 0:
        # temp.source_listing_id = each_row_temp[0], temp.modification_timestamp = each_row_temp[1], temp.media_modification_timestamp = each_row_temp[2]
        s_l_ids_list = [
            each_row_temp[0] for each_row_temp in temp_listings
        ]  # s_l_ids_list = ['temp.source_listing_id', 'temp.source_listing_id', ...]
        ids_timestamps_dict = {
            each_row_temp[0]: (each_row_temp[1], each_row_temp[2])
            for each_row_temp in temp_listings
        }

        del temp_listings

        if "mls grid" in source_type.lower():
            target_listing_field = "mls_number"
        else:
            target_listing_field = "source_listing_id"

        cursor_pentaho.execute(
            f"""SELECT distinct {target_listing_field}, 
            modification_timestamp::timestamp, 
            COALESCE(media_modification_timestamp, '1990-01-01 00:00:00.000')::timestamp as media_modification_timestamp
            FROM public.listing
            WHERE source_id = %s AND {target_listing_field} IN %s
        """,
            (source_id, tuple(s_l_ids_list)),
        )

        del s_l_ids_list

        """ --- definition of ids_timestamps_dict and its usage in the list comprehension below ---
        ids_timestamps_dict = {
            'temp.source_listing_id': (temp.modification_timestamp,temp.media_modification_timestamp), 
            ...
        }
        listing.source_listing_id = each_row_listing[0]
        listing.modification_timestamp = each_row_listing[1]
        listing.media_modification_timestamp = each_row_listing[2]
        if ids_timestamps_dict[listing.source_listing_id][temp.modification_timestamp] <= listing.modification_timestamp and ids_timestamps_dict[listing.source_listing_id][temp.media_modification_timestamp] <= listing.media_modification_timestamp
        """

        to_disable = [
            each_row_listing[0]
            for each_row_listing in cursor_pentaho.fetchall()
            if ids_timestamps_dict[each_row_listing[0]][0] <= each_row_listing[1]
            and ids_timestamps_dict[each_row_listing[0]][1] <= each_row_listing[2]
        ]

        del ids_timestamps_dict

        if len(to_disable) > 0:
            cursor_serverless.execute(
                """
                UPDATE idx_stage.temp_table
                SET download_flag = 'f'
                WHERE source_id = %s AND listingkey IN %s
            """,
                (source_id, tuple(to_disable)),
            )
            serverless_db_con.commit()

            log_msg = {
                "source_id": source_id,
                "function_name": "filter_temp_listings()",
                "Message": f"Disabled {len(to_disable)} listings in idx_stage.temp_table",
            }
            logger.info(log_msg)

        del to_disable

    cursor_serverless.execute(f"""SELECT count(distinct listingkey)
        FROM idx_stage.temp_table
        WHERE source_id = {source_id} and download_flag = 't' and respecs_flag = 'f';
    """)

    return int(cursor_serverless.fetchone()[0])


# moving folder of temp_table request folder inside batch_id folder
def move_temp_request_folder(
    source_id, source_type, source_name, temp_folder_name, batch_id
):

    s3 = boto3.resource("s3")
    bucket_name = os.environ.get("bucket_name")
    bucket = s3.Bucket(bucket_name)

    source_prefix = f"{source_type}/{source_id}_{source_name}/{temp_folder_name}/"
    target_prefix = (
        f"{source_type}/{source_id}_{source_name}/{batch_id}/{temp_folder_name}/"
    )

    for obj in bucket.objects.filter(Prefix=source_prefix):

        old_key = obj.key
        new_key = old_key.replace(source_prefix, target_prefix, 1)

        # Copy
        s3.Object(bucket.name, new_key).copy_from(
            CopySource={"Bucket": bucket.name, "Key": old_key}
        )

        # Delete old
        s3.Object(bucket.name, old_key).delete()


def lambda_handler(event, context):
    """
    AWS Lambda function to handle data processing and validation based on the specified criteria.

    Args:
    - event (dict): Event data triggering the Lambda function. Following is the example event
        {"source_id": 483,
        "source_name": "MIAMIRE_RAPB3 DIRECT",
        "auth": { "type": "noImageAuth","user": "trestle_YlopoLLCYlopoLicense20190617015205", "proxy": false,"loginUrl": "https://api-prod.corelogic.com/trestle/oidc/connect/token","password": "6d9dfa7a47db4ce78d36ab17a55a3412",    "logoutUrl": "",    "isPlaintext": true},
        "source_info": {"limit": 200, "mls_board": "MIAMIRE_RAPB3",  "source_type": "Trestle",  "idx_mls_type": "Trestle Web API",   "originating_system_name": "SEFMIAMI"},
        "run_host": "Serverless-Trestle",
        "success": false
        }

    Returns:
    - dict: Processed data and status information.Following is the output
        {"source_id": 483,"mls_board": "MIAMIRE_RAPB3",  "source_type": "Trestle",  "batch_creation_date": "2024-01-02 12:36:39.575000",
        "batch_id": 7051356,  "last_refresh_date": "2024-01-02T12:23:13.000000Z",  "status": true,  "run_host": "Serverless-Trestle",  "success": false}

    """
    global serverless_db_con
    global pentaho_db_con

    logger.info(event)

    rds_database = os.environ.get("rdsDatabase")
    listing_database = os.environ.get("listingDatabase")
    db_secret_dev = SecretManagerHelper.get_secret(rds_database, "us-west-2")
    db_secret_stage = SecretManagerHelper.get_secret(listing_database, "us-west-2")
    sql_execlimit = context.get_remaining_time_in_millis()
    serverless_db_con = get_connection(db_secret_dev, serverless_db_con, sql_execlimit)
    pentaho_db_con = get_connection(db_secret_stage, pentaho_db_con, sql_execlimit)
    cursor_serverless = serverless_db_con.cursor()
    cursor_pentaho = pentaho_db_con.cursor()
    data_dictionay = {}
    source_data = event

    try:
        source_id = source_data.get("source_id")
        run_host = source_data.get("run_host")
        source_info = source_data.get("source_info")
        source_type = source_info.get("source_type")
        last_modification_date = source_data["last_modification_date"]
        source_data["iteration"] = False
        total_count = source_data.get("row_count", -1)
        if total_count == -1:
            failed_batch_initialization(pentaho_db_con, cursor_pentaho, source_data)
            # return source_data
        respecs_flag = source_data["batch_execution_params"]["respecs_flag"]
        bl_flag = source_data["batch_execution_params"]["bl_flag"]
        bl_threshold = source_data["batch_execution_params"]["bl_threshold"]
        batch_execution_params = source_data.get("batch_execution_params", {})
        flow_type = source_data.get("flow_type", "lmd")
        temp_table_status = source_data.get("temp_table_status", None)

        if temp_table_status and total_count > 0 and flow_type != "respecs":
            total_count = filter_temp_listings(
                source_id,
                source_type,
                flow_type,
                serverless_db_con,
                cursor_serverless,
                cursor_pentaho,
            )
            source_data["row_count"] = total_count

        check_the_source_latest_batch = """
        SELECT e.batch_id, e.load_inactive_lst_status,e.run_host, e.source,e.load_date
        FROM STAGE.etl_batches e 
        where e.source_id = {}
        ORDER BY batch_id DESC LIMIT 1
        """.format(source_id)

        result = execute_query(
            pentaho_db_con, check_the_source_latest_batch, cursor_pentaho
        )
        source_data["batch_id"] = result[0]

        if flow_type != "sold" and flow_type != "full_load":
            latest_listing_date = source_data["latest_listing_date"]
            latest_listing_date = (
                str(latest_listing_date).split(".", 1)[0].replace("T", " ")
            )

            # checking respecs
            if respecs_flag is False:
                is_respecs = None
                is_respecs = check_respecs(cursor_serverless, source_id)

                if is_respecs is True:
                    respecs_creation(
                        source_id,
                        latest_listing_date,
                        source_data,
                        cursor_pentaho,
                        pentaho_db_con,
                        cursor_serverless,
                        serverless_db_con,
                    )

            # checking backlog
            if (
                (int(total_count) > int(bl_threshold))
                and (bl_flag is not True)
                and (flow_type in ["lmd", "rolling_window"])
            ):

                flow_type = "backlog"
                backlog_creation(
                    source_id,
                    latest_listing_date,
                    last_modification_date,
                    source_data,
                    cursor_pentaho,
                    pentaho_db_con,
                    cursor_serverless,
                    serverless_db_con,
                )

        # batch creation and resumption
        if total_count >= 0:

            data_dictionay["batch_id"] = result[0]
            data_dictionay["status"] = result[1]
            data_dictionay["run_host"] = result[2]
            data_dictionay["source_name"] = result[3]
            creation_date = str(result[4])

            # NEW BATCH CREATION
            if data_dictionay["status"] == "Completed":

                batch_id, batch_start_time = insert_into_etl_batches(
                    pentaho_db_con, source_id, data_dictionay, run_host, cursor_pentaho
                )
                source_data["batch_id"] = batch_id
                source_data["batch_initialized"] = True
                if total_count == 0:
                    source_data["batch_initialized"] = False

                source_data["batch_creation_date"] = batch_start_time
                source_data["batch_last_status"] = data_dictionay["status"]
                logger.info(source_data)

            # RESUME OLD BATCH
            elif (
                data_dictionay["status"] == "in_progress"
                and "Serverless" in data_dictionay["run_host"]
                and total_count > 0
            ):
                source_data["batch_id"] = data_dictionay["batch_id"]
                batch_id = data_dictionay["batch_id"]
                cleaning_func(
                    source_id,
                    batch_id,
                    pentaho_db_con,
                    cursor_pentaho,
                    cursor_serverless,
                )
                source_data["batch_initialized"] = True
                source_data["batch_creation_date"] = creation_date
                source_data["batch_last_status"] = data_dictionay["status"]
                logger.info(source_data)

            else:
                source_data["run_host"] = run_host
                source_data["batch_initialized"] = False
                source_data["batch_last_status"] = data_dictionay["status"]
                source_data["batch_creation_date"] = creation_date
                logger.error(source_data)
        else:
            source_data["batch_initialized"] = False

            logger.error(source_data)

        # DELETE PRE-STAGE DATA
        if source_data["batch_initialized"]:
            delete_data_prestage(
                serverless_db_con, cursor_serverless, source_id, source_info
            )

        logger.info(source_data)

        if temp_table_status:
            # move temp_table request folder inside 'newly created batch_id' folder; S3 DATA ARCHIVAL
            lmd_date = source_data["last_modification_date"]
            lmd_date = str(lmd_date).split(".", 1)[0].replace(":", "").replace("-", "")
            move_temp_request_folder(
                source_id,
                source_type,
                source_data["source_name"],
                f"{lmd_date}_temp",
                source_data["batch_id"],
            )

        # UPDATE TOTAL COUNT IN ETL BATCHES TABLE
        query = f"""
        update stage.etl_batches
        set source_t_counts = {total_count}
        where batch_id = {source_data["batch_id"]}
        """
        cursor_pentaho.execute(query)
        pentaho_db_con.commit()

        source_data["flow_type"] = flow_type
        source_data["last_modification_date"] = last_modification_date
        source_data["scan_s3_flag"] = batch_execution_params.get("scan_s3_flag", False)
        source_data["respecs_flag"] = batch_execution_params.get("respecs_flag", False)
        logger.info(source_data)
        return source_data

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
        if serverless_db_con:
            serverless_db_con.close()
        if pentaho_db_con:
            pentaho_db_con.close()
