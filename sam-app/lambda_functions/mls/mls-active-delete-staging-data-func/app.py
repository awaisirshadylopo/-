# Import necessary modules
import os
import psycopg2
import boto3
import json
import traceback
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-unlocking-func")
logger.setLevel("INFO")

conn = None

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
# def setup_db_connection(secret, sql_execlimit):
#     # Extract database connection parameters from the secret
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_name = secret["dbname"]
#     # Establish a connection to the PostgreSQL database
#     conn = psycopg2.connect(
#         dbname=db_name,
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         port=db_port,
#         options=f"-c statement_timeout={sql_execlimit}",
#     )
#     return conn

def get_connection(secret, connection, sql_execlimit):
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
        options=f"-c statement_timeout={sql_execlimit}",
    )


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
    global conn
    # TODO implement
    try:
        # Fetching database secrets from AWS Secrets Manager
        secret_name = os.environ.get("rdsDatabase")
        secrets = fetch_secrets(secret_name)
        tables = [
            "stage.direct_idx_listing",
            "stage.direct_idx_address",
            "stage.direct_idx_agent",
            "stage.direct_idx_broker",
            "stage.direct_idx_office",
            "stage.direct_idx_description",
            "stage.direct_idx_photo",
            "stage.direct_idx_openhouse",
            "stage.direct_idx_school",
            "stage.direct_idx_attribute",
            "stage.direct_idx_attribute_2",
            "stage.direct_idx_attribute_3",
            "stage.direct_idx_attribute_custom",
            "stage.direct_idx_attribute_custom_2",
            "stage.direct_idx_attribute_custom_3",
            "stage.direct_idx_attribute_custom_4",
        ]
        sql_execlimit = context.get_remaining_time_in_millis()
        # Setting up a database connection
        conn = get_connection(secrets, conn, sql_execlimit)
        sId = event["id"]
        if conn:
            cursor = conn.cursor()
            # Iterate through each table and delete rows for the specified source_id
            for t in tables:
                delete_sql = "DELETE FROM {} WHERE source_id in {}".format(t, sId)
                cursor.execute(delete_sql)
                conn.commit()
                msg1 = "ROWS DELETED {} from {} against {}".format(
                    cursor.rowcount, t, sId
                )
                logValue1 = {"Stats": msg1, "Query": delete_sql}
                logger.info(logValue1)
        return event["input"]

    except Exception as e:
        # Log an error message and return a 500 status code with the error details
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        event.update(log_msg)
        logger.error(event)
        return event
    finally:
        # Close the database cursor and connection in the finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()
