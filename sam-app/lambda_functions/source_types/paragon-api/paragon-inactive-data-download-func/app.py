'''Paragon API Inactive Data Download'''

import os
import logging
import json
import traceback
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
import time
from datetime import datetime

logger = logging.getLogger("Paragon-API-Inactive-Data-Download-Lambda")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret


# Function to set up a PostgreSQL database connection
def setup_db_connection(secret, sqlExecLimit):
    db_user = secret['username']
    db_password = secret['password']
    db_host = secret['host']
    db_port = secret['port']
    db_name = secret['dbname']
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        options = f"-c statement_timeout={sqlExecLimit}"
    )
    return conn


#runtime token generation
def create_token(url, client_id, client_secret):

    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "OData",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        log_msg = {"message": "token generation failed", "response": response}
        logger.error(log_msg)


# execute and commit query in db
def execute_query(source_id, source_name, query,cursor,connection):
    cursor.execute(query)
    connection.commit()

    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "query": query
    }
    logger.info(log_msg)



# Function to make API call and load tables
def api_call_and_load_tables(source_id, source_name, source_info, auth, batch_id, cursor, connection):
    # update batch status
    etl_status_update = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where source_id = {0} and batch_id = {1}""".format(source_id, batch_id)
    execute_query(source_id, source_name, etl_status_update,cursor,connection)
    
    loginUrl = auth['loginUrl']
    token = auth['password']
    status_column = source_info['status_column']
    sold_status = source_info['sold_status']
    cdc_column = source_info['cdc_column']
    reference_key = source_info['reference_key']

    # making filter value; params{"$filter": filter_value}
    filter_value = f"{status_column} ne {sold_status}"

    # check for multiple sold statuses
    statuses = sold_status.split(',')
    if len(statuses) > 1:
        filter_value = ' and '.join([f"{status_column} ne {status}" for status in statuses])

    # token generation for Paragonrels (source_id = 904)
    if source_id in [904,981]:
        tokenUrl = auth['tokenUrl']
        client_id = auth["user"]
        client_secret = auth["password"]

        token = create_token(tokenUrl, client_id, client_secret)

        filter_value = filter_value + " and Sale_Or_Rent ne 'R'" if  source_id == 904 else filter_value
    
    elif source_id == 784: 
        # skip limit = 10000; lambda times out for 784 if status ne 'sold' is used; huge count for statuses which are neither in active nor in sold
        active_status = source_info['active_status']
        filter_value = f"{status_column} in ({active_status})" + " and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"
    elif source_id == 981:
        # no need to add filter for property_type for this sources.
        pass 
    else:
        filter_value = filter_value + " and PropertyType ne 'Residential Lease' and PropertyType ne 'Commercial Lease'"


    # initial values for GET Request and loop iterations
    loginUrl = loginUrl.replace("$metadata","Property")
    headers = {
        "Authorization": f"Bearer {token}" 
    }
    download_data_list = []
    status_flag = True
    top = 200
    skip = 0
    total_count = 0
    one_more_hit = 0
    max_timestamp = "1990-01-01T00:00:00.000Z" # max_timestamp may be needed in case of exceeding limit of "$skip"
    params = {
                "$filter":filter_value + f" and {cdc_column} ge {max_timestamp}",
                "$count":"true",
                "$orderby": f"{cdc_column} asc",
                "$top": top,
                "$skip": skip,
                "$select": f"{reference_key},{status_column},{cdc_column}"
            }


    # getting source count initially; because data['@odata.count'] may change in the loop if we update the max_timestamp in case of exceeding $skip limit
    response = requests.get(url=loginUrl, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = json.loads(response.text)
    source_count = data['@odata.count']

    # updating source count
    source_count_update = """
    update stage.etl_batches set source_t_counts = {0} 
    where source_id = {1} and batch_id = {2};
    """.format(source_count, source_id, batch_id)
    execute_query(source_id, source_name, source_count_update,cursor,connection)


    # starting loop for GET requests; incremental 200 records in each iteration (top = 200)
    while True:
        response = None
        params["$skip"] = skip # updating skip

        try:
            response = requests.get(url=loginUrl, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = json.loads(response.text)

            download_data_list.extend(data['value'])
            total_count = data['@odata.count']
            
            if skip + top >= total_count:
                break
            skip += top
            
            # wait for 1 second before next GET request (next iteration)
            time.sleep(1)
            

        except requests.exceptions.RequestException as e:
            # try at least once again if API's GET request fails
            if one_more_hit == 0:
                one_more_hit += 1
                log_msg = {
                    "source_id": source_id, 
                    "source_name": source_name, 
                    "message": "Request Failed. Trying once more.",
                    "response": response.text,
                    "params": params
                }
                logger.info(log_msg)

                # if the limit of "$skip" exceeds then
                if "maximum value for $skip" in response.text.lower():
                    max_timestamp = max(
                        download_data_list,
                        key=lambda x: datetime.fromisoformat(x[cdc_column].replace('Z', '+00:00'))
                    )[cdc_column]

                    skip = 0
                    params["$filter"] = filter_value + f" and {cdc_column} ge {max_timestamp}"
                    params["$skip"] = skip


                time.sleep(3)
                continue # return to the beginning of loop


            # logging error
            status_flag = False
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "download_status": status_flag,
                "message": "Paragon Inactive Data Download Failed",
                "Error": e,
                "Response": str(response.text)
            }
            logger.error(log_msg)

            return status_flag

    # dataframe
    df = pd.DataFrame(download_data_list)

    # only keep needed columns in dataframe
    df = df[[reference_key, status_column]]
    df['source_id'] = source_id
    df['batch_id'] = batch_id

    # renaming dataframe fields for table columns
    df = df.rename(columns={f"{status_column}": "status", f"{reference_key}": "source_listing_id"})
    df = df.drop_duplicates()

    # updating counts in stage.etl_batches
    downloaded_count = len(df)
    download_count_update = """
    update stage.etl_batches set downloaded_d_counts = {0} 
    where source_id = {1} and batch_id = {2};
    """.format(downloaded_count, source_id, batch_id)
    execute_query(source_id, source_name, download_count_update,cursor,connection)


    # inserting into generic table "stage.direct_idx_id"
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ','.join(list(df.columns))
    
    insert_query = """ 
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                 """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    # logging info
    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "download_status": status_flag,
        "source_count": source_count,
        "downloaded_count": downloaded_count
    }
    logger.info(log_msg)
    
    return status_flag



def lambda_handler(event, context):

    # making database connections
    listing_secret = os.environ.get('listingDatabase')
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_conn = setup_db_connection(listing_secrets , sqlExecLimit)
    listing_cursor = listing_conn.cursor()

    # fetching values from event
    source_id = event['source_id']
    source_name = event['source_name']
    source_info = event['source_info']
    auth = event['auth']
    batch_id = event['batch_id']
    download_status = False

    # response to be returned
    final_response = {
        "source_id": source_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "source_type": source_info['source_type'],
        "mls_board": source_info['mls_board'],
        "run_host": event['run_host'],
        "inactive_threshold": event['inactive_threshold'],
        "download_status": download_status,
        "success": False
    }
    
    try:
        # delete downloaded data from generic table
        previous_data_deletion =  "DELETE FROM stage.direct_idx_id where source_id = {0}".format(source_id)
        execute_query(source_id, source_name, previous_data_deletion, listing_cursor, listing_conn)

        # calling for API request and inertion in generic table
        download_status = api_call_and_load_tables(source_id, source_name, source_info, auth, batch_id, listing_cursor, listing_conn)

        # update download_status and return
        final_response["download_status"] = download_status
        final_response['auth'] = event['auth']
        final_response['source_info'] = event['source_info']
        return final_response

    except Exception as e:
        
        log_msg = {'Error':str(e) , 'Error At line': traceback.format_exc()}
        final_response.update(log_msg)
        logger.error(final_response)
        
        return final_response


    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()