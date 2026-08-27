import json
import boto3
import logging
import os
import psycopg2
from datetime import datetime

logger = logging.getLogger("mls-unified-locking-func")
logger.setLevel("INFO")

homelisting_connection = None


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
# def setup_db_connection(secret):
#     db_user = secret["username"]
#     db_password = secret["password"]
#     db_host = secret["host"]
#     db_port = secret["port"]
#     db_name = secret["dbname"]
#     homelisting_connection = psycopg2.connect(
#         dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
#     )
#     return homelisting_connection

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
    """this lambda returns list containing data dictionaries as mentioned below
      {"source_id": 831,
      "source_name": "INCLINE DIRECT",
      "auth": {"type": "noImageAuth","user": "trestle_YlopoLLCYlopoLicense20190617015205","proxy": false,"loginUrl": "https://api-prod.corelogic.com/trestle/oidc/connect/token","password": "6d9dfa7a47db4ce78d36ab17a55a3412","logoutUrl": "", "isPlaintext": true},
      "originating_system_name": null,
      "run_host": "Serverless-Trestle"
    }
    """
    global homelisting_connection
    # Log data initialization

    try:
        # Fetching Stage database secrets from AWS Secrets Manager
        secret_name = os.environ.get("listingDatabase")
        secrets = fetch_secrets(secret_name)
        max_row_count = os.environ.get("limit")

        # Extracting 'source_type' from the Lambda event
        source_type = event["source_type"]

        # Setting up a database connection
        homelisting_connection = get_connection(secrets,homelisting_connection)

        if homelisting_connection:
            cursor_homelisting = homelisting_connection.cursor()

            # NOTE: UPDATE SOURCE IDS IN ENVIRONMENT VARIABLES
            if source_type in ["MLS Grid V2 API"]:
                select_query_1 = f"""
                    SELECT s.id, s.runtime_count FROM (
                        SELECT s.id ,s.runtime_count
                        ,CASE WHEN (source_info ->>'execution_priority')::boolean = true THEN '1990-01-01' ELSE s.last_refresh_date END AS last_refresh_date
                        FROM public.source AS s
                        WHERE s.active_flag IS TRUE
                        AND s.is_scheduled IS TRUE                    
                        AND s.run_host IS NULL
                        AND (CASE WHEN s.runtime_count = 0 THEN 1 ELSE s.runtime_count END) % s.inactive_runtime_step > 0
                        AND s.source_info->'source_type'='"{source_type}"' 
                    ) s order by s.last_refresh_date ASC limit 2;
                    """
            elif source_type in ["Bridge API"]:
                select_query_1 = f"""SELECT s.id, s.runtime_count FROM (

                                            SELECT  s.id ,s.runtime_count
                                                    ,ROW_NUMBER () 
                                                        OVER(PARTITION BY  auth->>'password' 	
                                                                ORDER BY S.last_refresh_date   
                                                            ) AS ROW_NUMB
                                            FROM public.source AS s
                                            WHERE s.active_flag IS TRUE
                                            AND s.is_scheduled IS TRUE                    
                                            AND s.run_host IS NULL
                                            AND (CASE WHEN s.runtime_count = 0 THEN 1 ELSE s.runtime_count END) % s.inactive_runtime_step > 0
                                            AND s.source_info->'source_type'='"{source_type}"' 
                        ) s
                            WHERE ROW_NUMB < 12;"""

            else:

                select_query_1 = f"""
                    SELECT s.id, s.runtime_count  FROM public.source AS s
                    WHERE s.active_flag IS TRUE
                    AND s.is_scheduled IS TRUE
                    AND s.run_host IS NULL 
                    AND (CASE WHEN s.runtime_count = 0 THEN 1 ELSE s.runtime_count END) % s.inactive_runtime_step > 0
                    AND s.source_info->'source_type'='"{source_type}"' 
                    order by s.last_refresh_date ASC limit {max_row_count}
                    """

            # Executing the select query
            result = cursor_homelisting.execute(select_query_1)
            result = cursor_homelisting.fetchall()

            if result:
                # Updating records in the database based on the result
                list_id = []
                timestamp_now = datetime.now()
                run_host_timestamp = timestamp_now.strftime("%Y%m%d%H%M%S%f")
                start_date = timestamp_now.strftime("%Y-%m-%d %H:%M:%S.%f")
                run_host_value = f"Serverless-{source_type}-{run_host_timestamp}"

                for r in result:

                    list_id.append(r[0])
                    update_query = f"UPDATE public.source SET run_host='{run_host_value}', last_start_date = '{start_date}'  WHERE id = %s"
                    cursor_homelisting.execute(update_query, (r[0],))
                    homelisting_connection.commit()

                id_tuple = tuple(list_id)
                id_tuple = (
                    f"({id_tuple[0]})"
                    if len(id_tuple) == 1
                    else f"({', '.join(map(str, id_tuple))})"
                )
                msg_log = {
                    "source_id": id_tuple,
                    "source_type": source_type,
                    "run_host_value": run_host_value,
                }
                logger.info(msg_log)

                # Fetching records after the update
                if id_tuple:
                    select_query_2 = f"""
                        SELECT id, name, auth, source_info, batch_execution_params, runtime_count 
                        FROM public.source WHERE run_host='{run_host_value}' AND id in {id_tuple}"""
                    # NOTE: UPDATE SOURCE IDS IN ENVIRONMENT VARIABLES

                    cursor_homelisting.execute(select_query_2)
                    rows = cursor_homelisting.fetchall()

                    # Creating a list of dictionaries from the fetched records
                    dict_list = []
                    for row in rows:
                        tup = {
                            "source_id": row[0],
                            "source_name": row[1],
                            "auth": row[2],
                            "source_info": row[3],
                            "batch_execution_params": row[4],
                            "runtime_count": row[5],
                            "run_host": run_host_value,
                            "flow": True,
                            "success": False,
                            "download_flag": False,
                        }
                        dict_list.append(tup)

                    # Logging the processed data
                    output_dict = {"input": dict_list, "flow": True, "id": id_tuple}
                    logger.info(output_dict)

                    # Returning the processed data
                    return output_dict

            else:

                logger.info("Source ID Not Available")
                return {
                    "flow": False,
                }

    except Exception as e:
        # Logging an error message

        logger.exception(f"Event : {event}, Error : {str(e)}")

        # Returning an error response
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

    finally:
        # Closing the cursor and connection
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()
