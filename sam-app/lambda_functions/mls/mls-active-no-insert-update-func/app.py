# Import necessary modules
import json
import os
import logging
import psycopg2
import boto3
import traceback
from datetime import datetime

logger = logging.getLogger("mls-no-insert-update-func")
logger.setLevel("INFO")

rds_connection = None
avalon_connection = None


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
# def setup_db_connection(secret):
#     # Extract database connection parameters from the secret
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_name = secret["dbname"]

#     # Establish a connection to the PostgreSQL database
#     conn = psycopg2.connect(
#         dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
#     )
#     return conn

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

def lambda_handler(event, context):
    """
     This  function perform insert and updates in respective ETL Tables
     Takes the following example event and returns the same  event
    {  "source_id": 796, "mls_board": "GJARA2",  "source_type": "Trestle",  "batch_creation_date": "2024-01-02 13:21:38.536000",
       "batch_id": 7051414,  "last_refresh_date": "2024-01-01T23:39:12.000000Z",  "status": true,  "run_host": "Serverless-Trestle","success": false
     }
    """

    global rds_connection
    global avalon_connection
    
    logger.info(event)
    event["source_type"] = event["source_info"]["source_type"]
    try:
        # Fetching database secrets from AWS Secrets Manager
        rds_secret_name = os.environ.get("rdsDatabase")
        rds_secrets = fetch_secrets(rds_secret_name)
        rds_connection = get_connection(rds_secrets, rds_connection)
        avalon_secret_name = os.environ.get("listingDatabase")
        avalon_secrets = fetch_secrets(avalon_secret_name)
        avalon_connection = get_connection(avalon_secrets, avalon_connection)

        if rds_connection and avalon_connection:
            log_msg = {"Status": "Connection Successfull With RDS and Listing DB"}

            logger.info(log_msg)

            rds_cursor = rds_connection.cursor()
            avalon_cursor = avalon_connection.cursor()

            # flow_type = event['flow_type']
            source_id = event["source_id"]
            batch_id = event["batch_id"]

            insert_count = 0
            update_count = 0
            # last_refresh_date = event['last_refresh_date']

            # Deletion from etl_status against batch_id
            del_etl_status = (
                """ delete from public.etl_status where batch_id = {0};""".format(
                    batch_id
                )
            )
            avalon_cursor.execute(del_etl_status)
            avalon_connection.commit()

            # Build and execute a SQL query to insert data into etl_status table
            insert_query = f"""
            INSERT INTO public.etl_status 
            (batch_id, insert_count, update_count, source_id)
            VALUES 
            ({batch_id},{insert_count},{update_count},{source_id})
            """
            avalon_cursor.execute(insert_query)
            avalon_connection.commit()

            update_query = f"UPDATE stage.etl_batches SET load_inactive_lst_status='Completed', load_inactive_lst_end_time=current_timestamp, batch_type = 'No Insert/Update' WHERE batch_id ='{batch_id}'"

            avalon_cursor.execute(update_query)
            avalon_connection.commit()

            if "batch_creation_date" not in event:
                current_date = datetime.now()
                event["batch_creation_date"] = current_date.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3]

        logger.info(event)
        event["success"] = True
        if event.get("temp_table_status") is not None:
            event["limit"] = event["source_info"].get("limit", 1000)
            event["bl_flag"] = event["batch_execution_params"]["bl_flag"]

        return event

    except Exception as e:

        log_msg = {"Error": e, "Error At line": traceback.format_exc(), "Event": event}
        event["status"] = False
        logger.exception(f"Event : {event}, LogMessage : {log_msg}")

        return event

    finally:
        # Close database connections and cursors in the finally block
        if avalon_cursor:
            avalon_cursor.close()
        if rds_cursor:
            rds_cursor.close()
        if avalon_connection:
            avalon_connection.close()
        if rds_connection:
            rds_connection.close()
