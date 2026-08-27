import os
import psycopg2
from psycopg2 import extras
import boto3
import json
import logging
import traceback
import pandas as pd

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

listing_conn  = None
rds_conn = None

# FUNCTION TO FETCH SECRETS FROM AWS SECRETS MANAGER
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# FUNCTION TO SET UP A POSTGRESQL DATABASE CONNECTION
# def setup_db_connection(secret):
#     # EXTRACT DATABASE CONNECTION PARAMETERS FROM THE SECRET
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_name = secret["dbname"]

#     # ESTABLISH A CONNECTION TO THE POSTGRESQL DATABASE
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


def update_query_execution(source_id, listing_cursor, listing_conn):
    # EXECUTE UPDATE SQL QUERIES
    select_query = f"select batch_execution_params->'query'  FROM public.source WHERE id in ({source_id}) "
    listing_cursor.execute(select_query)
    sql = listing_cursor.fetchone()

    if sql[0] is not None:
        sqls = str(sql[0]).split(";,")
        for s in sqls:
            listing_cursor.execute(s)
            listing_conn.commit()
            msg4 = "Update Query for Source ID: {}".format(source_id)
            logValue4 = {"Action": msg4, "Query": s}
            logger.info(logValue4)


def stage_to_listing_migration(
    source_id, stage_cursor, listing_cursor, listing_conn, target_tables
):
    # MIGRATE DATA FROM STATE TO LISTING DB
    for table in target_tables:

        delete_sql = f"Delete FROM {table} where source_id in ({source_id}) "
        listing_cursor.execute(delete_sql)
        listing_conn.commit()

        msg1 = "ROWS DELETED {} against {}".format(listing_cursor.rowcount, source_id)
        logValue = {"Stats": msg1, "Query": delete_sql}
        logger.info(logValue)

        col_names = f"SELECT STRING_AGG(target_column, ',') AS concatenated_columns FROM (SELECT distinct  target_column FROM etl.mappings WHERE source_id = {source_id} and target_column !~* 'photo_order' and target_column is not null AND LOWER(target_table) = '{table}') AS cte_cols"

        stage_cursor.execute(col_names)
        result = stage_cursor.fetchone()
        column_list = list(result)
        for i in column_list:
            if i is None:
                column_list.remove(i)

        if len(column_list) == 0:
            continue

        column_list = ",".join(column_list)
        # elif "photo_order" in column_list:
        #     column_list.remove("photo_order")

        # column_list = str(column_list).replace('[','').replace(']','')
        final_query = f"SELECT {column_list} FROM {table} WHERE source_id = {source_id}"

        stage_cursor.execute(final_query)
        result = stage_cursor.fetchall()
        msg2 = "ROWS SELECTED {} FOR SOURCE ID:{}".format(
            stage_cursor.rowcount, source_id
        )
        logValue2 = {"Stats": msg2, "Query": final_query}
        logger.info(logValue2)

        column_names = [desc[0] for desc in stage_cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))

        insert_query = """INSERT INTO  {} ({}) VALUES %s""".format(table, cols)
        extras.execute_values(listing_cursor, insert_query, result)
        listing_conn.commit
        msg3 = {
            "Stats": "ROWS INSERTED {} IN {} for source_id {}".format(
                listing_cursor.rowcount, table, source_id
            )
        }
        logger.info(msg3)


def lambda_handler(event, context):
    """
    Lambda function to perform ETL operations based on the input event.
    Input & Output event structure is same as follow:
    {
        "source_id": 838,
        "batch_id": 11873,
        "last_refresh_date": "2023-11-23T15:25:00.000000Z",
        "status": true,
        "run_host": "serverless"
    }

    """
    logger.info(f"Function Input: {event}")
    global listing_conn
    global rds_conn

    try:
        # FETCHING DATABASE SECRETS FROM AWS SECRETS MANAGER
        secret_name = os.environ.get("rdsDatabase")
        secrets = fetch_secrets(secret_name)
        listing_secret_name = os.environ.get("listingDatabase")
        listing_secrets = fetch_secrets(listing_secret_name)

        target_tables = [
            "stage.direct_idx_listing",
            "stage.direct_idx_address",
            "stage.direct_idx_school",
            "stage.direct_idx_agent",
            "stage.direct_idx_office",
            "stage.direct_idx_openhouse",
            "stage.direct_idx_photo",
            "stage.direct_idx_description",
            "stage.direct_idx_attribute",
            "stage.direct_idx_attribute_2",
            "stage.direct_idx_attribute_3",
            "stage.direct_idx_attribute_custom",
            "stage.direct_idx_attribute_custom_2",
            "stage.direct_idx_attribute_custom_3",
            "stage.direct_idx_attribute_custom_4",
        ]

        # SETTING UP A DATABASE CONNECTION
        rds_conn = get_connection(secrets, rds_conn)
        listing_conn = get_connection(listing_secrets, listing_conn)
        source_id = event["source_id"]

        if listing_conn and rds_conn:
            stage_cursor = rds_conn.cursor()
            listing_cursor = listing_conn.cursor()
            stage_to_listing_migration(
                source_id, stage_cursor, listing_cursor, listing_conn, target_tables
            )
            update_query_execution(source_id, listing_cursor, listing_conn)

        event["success"] = True
        return event

    except Exception as e:
        # LOG AN ERROR MESSAGE AND RETURN A 500 STATUS CODE WITH THE ERROR DETAILS
        logError = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(logError)
        logger.error(event)
        return event

    finally:
        # CLOSE THE DATABASE CURSOR AND CONNECTION IN THE FINALLY BLOCK
        if stage_cursor:
            stage_cursor.close()
        if listing_cursor:
            listing_cursor.close()
        if rds_conn:
            rds_conn.close()
        if listing_conn:
            listing_conn.close()
