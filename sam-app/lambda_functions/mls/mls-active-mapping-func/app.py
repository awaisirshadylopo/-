# Import necessary modules
import os
import logging
import json
import traceback
import boto3
import psycopg2

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-mapping-func")
logger.setLevel("INFO")

conn = None


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    """Initialize AWS Secrets Manager client"""
    client = boto3.client("secretsmanager")

    # Retrieve secret value using the provided secret name
    response = client.get_secret_value(SecretId=secret_name)

    # Parse and return the secret as a dictionary
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection


# def setup_db_connection(secret, sql_exec_limit):
#     """Extract database connection parameters from the secret"""
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
#         options=f"-c statement_timeout={sql_exec_limit}",
#     )
#     return conn

def get_connection(secret, connection, sql_exec_limit):
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
        options=f"-c statement_timeout={sql_exec_limit}",
    )


# Lambda function handler
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
        listing_database = os.environ.get("listingDatabase")
        sql_exec_limit = context.get_remaining_time_in_millis()
        secrets = fetch_secrets(secret_name)
        # Setting up a database connection
        conn = get_connection(secrets, conn, sql_exec_limit)
        sId = event["source_id"]
        source_name = event["source_name"]
        source_info = event.get("source_info", {})
        batch_id = event["batch_id"]
        source_type = event["source_type"] or source_info.get("source_type","")
        # mls_board = event["mls_board"]
        mls_board   = event.get("mls_board") or source_info.get("mls_board", "")

        cursor = conn.cursor()
        mapping_table = "etl.mappings"

        # separated mappings of trestle sources after source_id=921
        if source_type == "Trestle API" and sId < 922 and sId != 694:
            cursor.execute(
                "select distinct source_id,resource_name,reference_id  from {} where (source_id = {} OR shared_source_type='{}') order by reference_id".format(
                    mapping_table, sId, source_type
                )
            )
        else:
            cursor.execute(
                "select distinct source_id,resource_name,reference_id  from {} where source_id = {} order by reference_id".format(
                    mapping_table, sId
                )
            )
        all_mappings = cursor.fetchall()
        # Iterate through each mapping and perform ETL operations
        source_id = sId
        for map in all_mappings:
            if map[1]:
                resource_name = map[1]
                reference_id = map[2]

                # Retrieve transformation details for the given resource_name and reference_id
                cursor.execute(
                    "SELECT target_column, source_column, replace(business_transformation,'<mls_board>','UPPER(''{}'') AS mls_board') AS business_transformation , reference_id ,target_table FROM {} where status = 'true' and resource_name = '{}'and reference_id  = {};".format(
                        mls_board, mapping_table, resource_name, reference_id
                    )
                )
                result = cursor.fetchall()

                ins_col, tar_table, sel_col, trans_col = [], [], [], []

                # Extract transformation details into separate lists
                for tup in result:
                    target, source, trans, reference, target_table = tup
                    ins_col.append(target)
                    sel_col.append(source)
                    trans_col.append(trans)
                    tar_table.append(target_table)

                insert_columns_str = ", ".join(ins_col)
                select_columns_str = ", ".join(sel_col)
                select_columns_str = select_columns_str.lower()
                final_query_cols = []

                # Build the final list of columns for the SELECT clause
                for index, elem in enumerate(trans_col):
                    if elem == "N\A":
                        final_query_cols.append(sel_col[index])
                    else:
                        final_query_cols.append(elem)

                final_query_cols = ", ".join(
                    item.strip("'") for item in final_query_cols
                )

                # Retrieve join conditions for the given source_id and reference_id
                query = f"""select source_id,resource_name,
                    replace(replace(joins_and_conditions,'<source_id>','{source_id}'),'<batch_id>','{batch_id}') AS joins_and_conditions
                    from etl.mapping_joins
                    where id = {reference_id};
                    """

                cursor.execute(query)
                refrence_data = cursor.fetchall()

                join_refrence = refrence_data[0][2]

                # Build the INSERT query based on the retrieved information
                insert_query = "INSERT INTO {} ({}) SELECT DISTINCT ON (source_listing_id) {} {} ".format(
                    target_table,
                    insert_columns_str,
                    final_query_cols,
                    join_refrence,
                )
                insert_query = (
                    insert_query.replace("SELECT [", "SELECT ")
                    .replace("] FROM", " FROM")
                    .replace('"', "")
                    .replace("@#$", '"')
                )
                insert_query = insert_query.replace("USA", "'USA'")

                log = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "target_table": target_table,
                    "Insert Query": insert_query,
                }
                logger.info(log)

                # Execute the INSERT query and commit changes
                if target_table in [
                    "stage.direct_idx_openhouse",
                    "stage.direct_idx_photo",
                    "stage.direct_idx_description",
                ]:
                    insert_query = insert_query.replace(
                        "DISTINCT ON (source_listing_id)", ""
                    )
                try:
                    cursor.execute(insert_query)
                except Exception as e:
                    log_msg = {
                        "target_table": target_table,
                        "Error": e,
                        "insert_query": insert_query.replace(r"\t", " ").replace(
                            r"\n", " "
                        ),
                    }
                    raise Exception(log_msg)
                conn.commit()
                logValue2 = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "target_table": target_table,
                    "Message": f"ROWS INSERTED {cursor.rowcount}",
                    "Query": insert_query,
                }
                logger.info(logValue2)

        if source_id in [765, 592]:
            sold_main_photo_only_query = """
                Delete from stage.direct_idx_photo where id in (
                SELECT a.id from
                    (SELECT 
                        lp.id,lp.source_listing_id, lp.photo_order
                        from stage.direct_idx_photo lp
                        JOIN 
                        (select source_listing_id, listing_status from stage.direct_idx_listing 
                        where source_id =  {0} and listing_status ~* 'close|Sold') l
                        on lp.source_listing_id = l.source_listing_id 
                        WHERE lp.source_id =  {0}
                ) a
                WHERE a.photo_order > 1)
                """.format(
                source_id
            )
            cursor.execute(sold_main_photo_only_query)
            conn.commit()

        return event

    except Exception as e:
        # Log an error message and return a 500 status code with the error details

        log_msg = {
            'status':False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event.update(log_msg)
        logger.error(event)
        return event

    finally:
        # Close the database cursor and connection in the finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()
