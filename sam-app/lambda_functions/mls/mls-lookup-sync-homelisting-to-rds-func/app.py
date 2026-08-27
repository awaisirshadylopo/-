### syncing lookup tables -->> SELECT from HomeListing and INSERT into RDS (if missing in RDS)

import os
import json
import boto3
import psycopg2
from psycopg2 import extras
import traceback
import logging

logger = logging.getLogger("mls-lookup-sync-func")
logger.setLevel(logging.INFO)

conn_homelisting = None
conn_rds = None


def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
# def setup_db_connection(secret):
#     db_user = secret['username']
#     db_password = secret['password']
#     db_host = secret['host']
#     db_port = secret['port']
#     db_name = secret['dbname']
#     conn = psycopg2.connect(
#         dbname=db_name,
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         port=db_port
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
     AWS Lambda function to synchronize lookup tables between databases.

    Returns:
    - str: Status message indicating the success or failure of the synchronization.
    """
    logger.info(
        "Lookup table synchronization is temporarily disabled. Skipping execution."
    )

    return True  # currently this needs fixation thats why its being return true without execution
    global conn_homelisting
    global conn_rds
    try:

        secret_homelisting = os.environ.get("listingDatabase")
        homelisting_secrets = fetch_secrets(secret_homelisting)
        secret_rds = os.environ.get("rdsDatabase")
        rds_secrets = fetch_secrets(secret_rds)

        conn_homelisting = get_connection(homelisting_secrets, conn_homelisting)
        cursor_homelisting = conn_homelisting.cursor()
        conn_rds = get_connection(rds_secrets, conn_rds)
        cursor_rds = conn_rds.cursor()

        sync_tables = [
            "listing_category",
            "listing_property_sub_type",
            "listing_property_type",
            "listing_status",
            "mls_board",
            "idx_config.listing_mls_number_regex",
            "stage.area_mapping",
            "idx_config.listing_property_type",
        ]

        for table_name in sync_tables:

            # hl_ids -->> IDs of homelisting's table; rds_ids -->> IDs of RDS' table
            cursor_homelisting.execute(f"SELECT id FROM {table_name} ORDER BY id;")
            results = cursor_homelisting.fetchall()
            hl_ids = [row[0] for row in results]

            cursor_rds.execute(f"SELECT id FROM {table_name} ORDER BY id;")
            results = cursor_rds.fetchall()
            rds_ids = [row[0] for row in results]

            sync_ids = [id for id in hl_ids if id not in rds_ids]
            del hl_ids, rds_ids, results

            total_ids_synced = len(sync_ids)

            if total_ids_synced > 0:
                sync_ids = str(sync_ids).replace("[", "").replace("]", "")
                cursor_homelisting.execute(
                    f"SELECT * FROM {table_name} WHERE id IN ({sync_ids})"
                )
                data_tuples = cursor_homelisting.fetchall()
                column_names = [col[0] for col in cursor_homelisting.description]

                insert_query = """
                    INSERT INTO {0} ({1}) VALUES %s
                """.format(table_name, column_names)
                insert_query = (
                    insert_query.replace("[", "").replace("]", "").replace("'", "")
                )

                extras.execute_values(cursor_rds, insert_query, data_tuples)
                conn_rds.commit()

            log_msg = {
                "Table Name": table_name,
                "No. of entries synced": total_ids_synced,
            }
            logger.info(log_msg)

        response = {
            "lambda_name": "mls-lookup-sync-func",
            "status": True,
            "message": "Lookup tables synchronization completed successfully",
        }

        return response

    except Exception as e:

        response = {
            "lambda_name": "mls-lookup-sync-func",
            "status": False,
            "message": "Lookup tables synchronization failed",
            "Error": str(e),
            "Error at line": traceback.format_exc(),
        }
        logger.error(response)
        return response

    finally:
        if conn_homelisting:
            conn_homelisting.close()
        if conn_rds:
            conn_rds.close()
        if cursor_homelisting:
            cursor_homelisting.close()
        if cursor_rds:
            cursor_rds.close()
