# Import necessary modules
import os
import json
import logging
import boto3
import psycopg2
import traceback
from datetime import datetime

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-unlocking-func")
logger.setLevel("INFO")

homelisting_connection = None
rds_connection = None

# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    # Initialize AWS Secrets Manager client
    client = boto3.client("secretsmanager")
    # Retrieve secret value using the provided secret name
    response = client.get_secret_value(SecretId=secret_name)
    # Parse and return the secret as a dictionary
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
def setup_db_connection(secret):
    # Extract database connection parameters from the secret
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    return conn

def get_connection(secret, connection):
    try:
        if connection is not None and connection.closed == 0:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return connection
    except Exception:
        pass

    return psycopg2.connect(
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        connect_timeout=15,
    )

# Lambda function handler
def lambda_handler(event, context):

    global homelisting_connection
    global rds_connection

    step_function_url = ""
    event_payload = {}
    try:
        event_payload = event["originalPayload"]
        execution_arn = event["executionArn"]
        step_function_url = (
            "https://us-west-2.console.aws.amazon.com/states/home?region=us-west-2#/map-runs/executions/"
            + str(execution_arn)
        )

    except:
        event_payload = event

    logger.info(event_payload)

    # Fetching database secrets from AWS Secrets Manager
    homelisting_secret_name = os.environ.get("listingDatabase")
    rds_secret_name = os.environ.get("rdsDatabase")
    homelisting_secrets = fetch_secrets(homelisting_secret_name)
    rds_secrets = fetch_secrets(rds_secret_name)

    # Setting up database connections and cursors for execution of SQL queries
    # homelisting_connection = setup_db_connection(homelisting_secrets)
    
    # rds_connection = setup_db_connection(rds_secrets)
    homelisting_connection = get_connection(
                    homelisting_secrets,
                    homelisting_connection,
                )

    rds_connection = get_connection(
                    rds_secrets,
                    rds_connection,
                )
    cursor_rds = rds_connection.cursor()
    cursor_homelisting = homelisting_connection.cursor()

    formatted_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    id = None

    try:

        # Check if the event is a list, if not convert it into a list
        if isinstance(event_payload, list):
            pass
        elif isinstance(event_payload, dict):
            event_payload = event_payload.get("input", [event_payload])
        else:
            event_payload = [event_payload]

        # Iterate through each item in the event
        for source_params in event_payload:
            if source_params:
                # Extract relevant data from the event
                source_id = source_params["source_id"]
                success = source_params["success"]
                source_name = source_params["source_name"]

                # If batch_id is available and success is True, update 'stage.etl_batches'
                if success is True:

                    # Get max media modified date always from the public.listing in homelistings
                    max_media_modification_timestamp_query = f""" SELECT MAX(modification_timestamp), MAX(media_modification_timestamp) FROM public.listing WHERE source_id = {source_id} """
                    cursor_homelisting.execute(max_media_modification_timestamp_query)
                    result = cursor_homelisting.fetchone()

                    last_modified_date = (
                        str(result[0]) if result[0] else "1990-01-01 00:00:00.000"
                    )
                    last_media_modified_date = (
                        str(result[1]) if result[1] else "1990-01-01 00:00:00.000"
                    )

                    # "update_timestamp_expression" is used to dynamically build the SET clause for stage.serverless_idx_loads
                    update_timestamp_expression = (
                        "last_modified_date = last_modified_date"
                    )

                    source_type = source_params["source_type"]
                    source_type = source_type.lower()
                    batch_id = source_params["batch_id"]
                    flow_type = source_params["flow_type"]

                    if (
                        "commercialmls" not in source_type
                    ):  # Commercialmls API doesn't have temp_table.
                        # source_info = event.get("source_info", {})

                        source_info = source_params.get("source_info", {})

                        limit = source_params.get(
                            "limit", source_info.get("limit", 1000)
                        )

                        temp_table_status = source_params["temp_table_status"]
                        batch_execution_params = source_params.get(
                            "batch_execution_params", {}
                        )
                        bl_flag = source_params.get(
                            "bl_flag", batch_execution_params.get("bl_flag", False)
                        )

                        temp_respecs_flag = "f"
                        orderby_column = "modification_timestamp"
                        orderby_type = "asc"

                        if flow_type in ["lmd", "rolling_window"]:
                            if bl_flag is True and temp_table_status is True:
                                orderby_type = "desc"
                        elif flow_type == "respecs":
                            temp_respecs_flag = "t"
                            flow_key_word = "respecs"
                        elif flow_type == "backlog":
                            flow_key_word = "bl"
                        elif (
                            flow_type == "sold"
                            and "mlsgrid" not in source_type.lower()
                            and "gsmls" not in source_type.lower()
                            and "mls router" not in source_type.lower()
                        ):
                            # mlsgrid doesn't filter nor sort on basis of sold date; so using modification_timestamp for ordering and request as well.
                            # gsmls doesn't populate sold_date in temp_table, so using modification_timestamp for ordering purpose only.
                            # mlsrouter doesn't allow orderby on sold date in request; so using modification_timestamp for ordering purpose only.
                            orderby_column = "sold_date"

                        # fetching listingkeys that were downloaded in current batch from temp_table
                        query = f"""select distinct on ({orderby_column}::timestamp, listingkey)
                            listingkey from idx_stage.temp_table 
                            where source_id = {source_id} and download_flag = 't'  and respecs_flag = '{temp_respecs_flag}'
                            order by {orderby_column}::timestamp {orderby_type}, listingkey 
                            limit {limit};"""

                        cursor_rds.execute(query)
                        temp_listings = cursor_rds.fetchall()
                        temp_listings = [l[0] for l in temp_listings]
                        temp_listings.append(0)  # handling case of "no insert/update"
                        processed_listings = ", ".join(f"'{x}'" for x in temp_listings)

                        # set download_flag = false for listings that were downloaded in current batch
                        query = f""" Update idx_stage.temp_table set download_flag = 'f' where source_id = {source_id} and listingkey in ({processed_listings}) ;"""
                        cursor_rds.execute(query)
                        rds_connection.commit()
                        del (
                            temp_listings,
                            processed_listings,
                            orderby_type,
                        )  # releasing memory

                        # fetching max modification_timestamp from temp_table
                        if flow_type in ["lmd", "rolling_window"]:
                            # fetch the max for lmd from temp_table...
                            max_date_query = f"select max(modification_timestamp) from idx_stage.temp_table where source_id = {source_id} and respecs_flag = 'f';"
                        else:
                            # fetch the max of only downloaded listings from temp_table for other than lmd flow...
                            max_date_query = f"select max(modification_timestamp) from idx_stage.temp_table where source_id = {source_id} and download_flag = 'f' and respecs_flag = '{temp_respecs_flag}';"

                        cursor_rds.execute(max_date_query)
                        result = cursor_rds.fetchone()[0]

                        if result:  # when temp_table is not empty
                            max_date = result.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )  # extracting timestamp without milliseconds

                            # lmd / rolling_window
                            if flow_type in ["lmd", "rolling_window"]:
                                update_timestamp_expression = f"last_modified_date = case when last_modified_date >= '{max_date}' then last_modified_date else '{max_date}' end"

                            # backlog / respecs
                            elif flow_type in ["backlog", "respecs"]:

                                start_and_finish_date_query = f""" select case when '{max_date}' >= {flow_key_word}_finish_date then true else false end as finish_flag
                                    from stage.serverless_idx_loads where source_id = {source_id} """
                                cursor_rds.execute(start_and_finish_date_query)

                                if cursor_rds.fetchone()[0]:  # finish backlog/respecs
                                    update_batch_execution_params_query = f"""
                                        UPDATE source
                                        SET 
                                            batch_execution_params = jsonb_set(batch_execution_params::jsonb, '{{"{flow_key_word}_flag"}}', 'false'::jsonb)
                                        WHERE id = {source_id};"""
                                    cursor_homelisting.execute(
                                        update_batch_execution_params_query
                                    )
                                    homelisting_connection.commit()

                                    update_timestamp_expression = f"{flow_key_word}_start_date = NULL, {flow_key_word}_finish_date = NULL"

                                else:  # update bl/respecs start_date
                                    update_timestamp_expression = (
                                        f"{flow_key_word}_start_date = '{max_date}'"
                                    )

                            # full_load
                            elif flow_type == "full_load":

                                update_timestamp_expression = f"""full_load_date = case when full_load_date >= '{max_date}' then full_load_date else '{max_date}' end"""

                            # sold
                            elif flow_type == "sold":
                                result, max_date = None, None

                                max_date_query = f"select max(sold_date::timestamp::date) from idx_stage.temp_table where source_id = {source_id} and download_flag = 'f' "
                                if (
                                    "mlsgrid" in source_type.lower()
                                    or "gsmls" in source_type.lower()
                                ):
                                    # mlsgrid do not order or filter on sold date in request
                                    # gsmls do not populate sold date in temp_table
                                    max_date_query = f"select max(modification_timestamp::date) from idx_stage.temp_table where source_id = {source_id} and download_flag = 'f'"
                                cursor_rds.execute(max_date_query)
                                max_date = str(cursor_rds.fetchone()[0])

                                update_timestamp_expression = f"""sold_date = case when sold_date >= '{max_date}' then sold_date else '{max_date}' end"""

                    else:
                        update_timestamp_expression = (
                            f"last_modified_date = '{last_modified_date}'"
                        )

                    # update_serverless_idx_loads_query --> UPDATE stage.serverless_idx_loads in RDS

                    if isinstance(update_timestamp_expression, tuple):
                        update_timestamp_expression = update_timestamp_expression[0]

                    update_serverless_idx_loads_query = f"""
                        UPDATE stage.serverless_idx_loads 
                        SET 
                            {update_timestamp_expression},
                            last_media_modified_date = '{last_media_modified_date}', 
                            y_last_update_date = '{formatted_datetime}', 
                            batch_id = {batch_id}
                        WHERE source_id = {source_id}"""

                    cursor_rds.execute(update_serverless_idx_loads_query)
                    rds_connection.commit()

                    row_count = source_params.get("row_count", 1)
                    if row_count == 0:
                        flow_type = "No Insert/Update"

                    # update_etl_batches_query --> UPDATE stage.etl_batches in homelistings
                    update_etl_batches_query = f"""UPDATE stage.etl_batches 
                        SET load_inactive_lst_status='Completed', 
                            load_inactive_lst_end_time=current_timestamp , 
                            batch_type = '{flow_type.lower()}',
                            description = '{step_function_url}'
                        WHERE batch_id ='{batch_id}' """
                    cursor_homelisting.execute(update_etl_batches_query)
                    homelisting_connection.commit()

                    executed_queries = {
                        "source_id": source_id,
                        "source_name": source_name,
                        "update_serverless_idx_loads_query": update_serverless_idx_loads_query,
                        "update_etl_batches_query": update_etl_batches_query,
                        "status": "Successful Execution",
                    }
                    logger.info(executed_queries)

                    get_runtime_count_query = f""" select case 
                            when date(last_refresh_date) != current_date 
                            then 1 
                            else runtime_count + 1 
                        end as runtime_count
                        from source where id = {source_id} """
                    cursor_homelisting.execute(get_runtime_count_query)
                    count_sql = cursor_homelisting.fetchone()[0]

                    # update_source_query --> UPDATE public.source in homelistings
                    update_source_query = f""" 
                        UPDATE public.source 
                        SET
                            run_host = NULL, 
                            batch_id = {batch_id}, 
                            runtime_count={count_sql} , 
                            last_refresh_date = '{formatted_datetime}',
                            last_completion_date = '{formatted_datetime}',
                            last_update_date = '{last_modified_date}'
                        WHERE id = {source_id};
                    """

                    cursor_homelisting.execute(update_source_query)
                    homelisting_connection.commit()

                else:  # If success is False or batch_id is None
                    batch_id = source_params["batch_id"]
                    update_etl_batches_query = f""" UPDATE stage.etl_batches 
                        SET description = '{step_function_url}'
                        WHERE batch_id = {batch_id} """
                    cursor_homelisting.execute(update_etl_batches_query)
                    homelisting_connection.commit()

                    # update 'public.source' table
                    update_source_query = f"UPDATE public.source SET run_host = NULL, last_refresh_date='{formatted_datetime}' WHERE id = {source_id} ;"
                    cursor_homelisting.execute(update_source_query)
                    homelisting_connection.commit()
                    # Log ERROR for executed query and status
                    log_msg = {
                        "status": "Unsuccessful Execution",
                        "source_id": source_id,
                        "source_name": source_name,
                        "Link": step_function_url,
                    }
                    logger.error(log_msg)

        return

    except Exception as e:
        # update 'public.source' table
        update_source_query = f"UPDATE public.source SET run_host = NULL, last_refresh_date='{formatted_datetime}' WHERE id = {source_id};"
        cursor_homelisting.execute(update_source_query)
        homelisting_connection.commit()

        executed_queries = {
            "update_query": update_source_query,
            "status": "Unsuccessful Execution",
        }
        logger.info(executed_queries)

        log_msg = {
            "Link": step_function_url,
            "Error": str(e),
            "Error at Line": traceback.format_exc(),
            "Event": event_payload,
        }
        logger.error(log_msg)

        return log_msg

    finally:
        # Close the database cursor and connection in the finally block
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()
        if cursor_rds:
            cursor_rds.close()
            rds_connection.close()
