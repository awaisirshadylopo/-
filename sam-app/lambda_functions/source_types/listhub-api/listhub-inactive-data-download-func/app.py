""" ListHub API Inactive Data Download Lambda Function """
import os
import logging
import json
import traceback
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_token(event):

    client_id = event['user']
    client_secret = event['password']
    url = event['loginUrl']
    
    payload = {
        'client_id': str(client_id),
        'client_secret': str(client_secret),
        'scope': 'api',
        'grant_type': 'client_credentials'
    }
          
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers = headers, data = payload)

    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json['access_token']
        return token
    else:
        # Log token generation failure
        logs = {"Token Generation": 'Failed', "Status Code": response.status_code}
        logger.info(logs)

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

def status_update(query,cursor,connection):
    cursor.execute(query)
    connection.commit()

# Function to make API call and load tables
def download_listing_for_inactive( source_id, batch_id, cursor, connection, loginurl, password , limit, offset):
    
    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(batch_id)
    status_update(etl_status,cursor,connection)
    
    status_flag=True

    loginurl = loginurl.replace("$metadata","Property")
    top = 500  # Number of records to fetch in each request
    download_data_list = []
    chunk_size = 350
    query = f""" select source_listing_id from listing_p_active where source_id in ({source_id}) limit {limit} offset {offset}; """
    logger.info(query)
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    
    log_msg = {
        "source_id": source_id,
        "Count": cursor.rowcount,
        "Message": "source_listing_id Count from Listing"
    }
    logger.info(log_msg)

    inactive_listings_chunks = [inactive_listings[i:i+chunk_size] for i in range(0, len(inactive_listings), chunk_size)]
    
    status_flag=True
    total_count = 0
    headers = {
            "Authorization": f"Bearer {password}"
        }

    total_count = len(inactive_listings)
    for chunk in inactive_listings_chunks:
        chunk_ids = "','".join(chunk)
        base_url = f"{loginurl}('{chunk_ids}')"
        params = {
            "$top": top,
            "$select": "ListingKey,StandardStatus"
        }
        response = None
        try:
            response = requests.get(url=base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = json.loads(response.text)
            if 'value' in data:
                download_data_list.extend(data['value'])  # Extend for multiple records
            else:
                download_data_list.append(data)

            # download_data_list.extend(data['value'])
            total_count = len(download_data_list)
        except requests.exceptions.RequestException as e:
            if response is not None and response.status_code == 413:
                sub_chunk_size = 50
                sub_chunks = [chunk[i:i + sub_chunk_size] for i in range(0, len(chunk), sub_chunk_size)]
                for sub_chunk in sub_chunks:
                    sub_chunk_ids = "','".join(sub_chunk)
                    sub_base_url = f"{loginurl}('{sub_chunk_ids}')"
                    sub_response = None
                    try:
                        sub_response = requests.get(url=sub_base_url, params=params, headers=headers, timeout=30)
                        sub_response.raise_for_status()
                        sub_data = json.loads(sub_response.text)
                        # download_data_list.extend(sub_data['value'])
                        if 'value' in sub_data:
                            download_data_list.extend(sub_data['value'])  # Extend for multiple records
                        else:
                            download_data_list.append(sub_data)

                    except Exception as sub_e:
                        if sub_response is not None and sub_response.status_code == 404:
                            if "Could not find resource" in sub_response.text:
                                log_msg = {
                                    "source_id": source_id,
                                    "Message": "These ListingKey Not found from Source (404)",
                                    "Request_Url": sub_base_url,  # type: ignore
                                }
                                logger.warning(log_msg)
                                continue

                        log_msg = {
                            "source_id": source_id,
                            "Message": "Error in fetching data from Source (sub-chunk)",
                            "Response": str(sub_response.text),  # type: ignore
                            "Error": sub_e
                        }
                        logger.error(log_msg)
                        return {
                            'source_id': source_id,
                            'batch_id': batch_id,
                            'download_status': False,
                            'count': total_count
                        }
            
            elif response is not None and response.status_code == 404:
                if "Could not find resource" in response.text:
                    log_msg = {
                        "source_id": source_id,
                        "Message": "These ListingKey Not found from Source (404)",
                        "Request_Url": base_url,  # type: ignore
                    }
                    logger.warning(log_msg)
                    continue

            else:
                log_msg = {
                    "source_id": source_id,
                    "Message": "Error in fetching data from Source",
                    "Response": str(response),  # type: ignore
                    "Error": e
                }
                logger.error(log_msg)
                return {
                    'source_id': source_id,
                    'batch_id': batch_id,
                    'download_status': False,
                    'count': total_count
                }
        
        except Exception as e:
            

            log_msg = {
                    "source_id": source_id,
                    "Message": "Error from Source",
                    "Response": str(response.text),  # type: ignore
                    "Error": e
                }
            logger.error(log_msg)
            return {
                    'source_id': source_id,
                    'batch_id': batch_id,
                    'download_status': False,
                    'count': total_count
                }

    log_msg = {
        "source_id": source_id,
        "Downloaded_Count": len(download_data_list),
        "Message": "Active Listing Count from Source"
    }
    logger.info(log_msg)    
    df = pd.DataFrame(download_data_list)
    df['source_id'] = source_id
    df['batch_id'] = batch_id
    df = df.rename(columns={'StandardStatus': 'status', 'ListingKey':'source_listing_id'})
    df = df.drop(columns=['@odata.context', '@odata.id'], errors='ignore')
    log_msg = { 
        "source_id": source_id,
        "batch_id": batch_id,
        "downloaded_count": len(df),
        "Message": "Downloaded Data"
    }
    # logger.info(log_msg)
    
    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ','.join(list(df.columns))
    
    insert_query = """ 
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                 """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    query = """ select count(1) from stage.direct_idx_id where source_id = {0} ;""".format(source_id)
    cursor.execute(query)
    downloaded_count = cursor.fetchone()[0] # type: ignore
    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(downloaded_count, batch_id)
    status_update(d_count,cursor,connection)

    
    response = {
        'source_id': source_id, 
        'batch_id': batch_id,
        'download_status': True ,
        'Current_count':len(df),
        'Total_count':downloaded_count
        }
    logger.info(response)

    return response

def lambda_handler(event, context):
    # TODO implement   
    logger.info(event)
    run_host = event['run_host']
    source_id = event['source_id']
    source_type = event['source_info']['source_type']
    mls_board = event['source_info'].get('mls_board')
    batch_id = event['batch_id']
    limit = event.get('limit', 100000)
    offset = event.get('offset', 0)
    downloaded_count = event.get('Total_count')
    auth = event["auth"]
    loginurl = auth['metadataUrl']

    listing_secret = os.environ.get('listingDatabase')
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_conn = setup_db_connection(listing_secrets , sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    query = f""" select count(1) from listing_p_active where source_id in ({source_id}) ;"""
    listing_cursor.execute(query)
    source_count = int(listing_cursor.fetchone()[0]) # type: ignore
    if source_count < offset:
        log_msg = {
            "source_id": source_id,
            'Offset' : offset,
            "source_count": source_count,
            "Message": "Offset Exceed then source count "
        }
        
        logger.info(log_msg)

        response = {
            'source_id': source_id, 
            "source_name": event['source_name'],
            'batch_id': batch_id,
            'download_status': True ,
            'break_loop': True,
            'offset': offset,
            'source_name': event['source_name'],
            'success': event['success'],
            'inactive_threshold': event['inactive_threshold'],
            'source_type': source_type,
            'mls_board': mls_board,
            'run_host': run_host,
            'downloaded_count': downloaded_count,
            'source_count':source_count
        }
        return response
    
    try:        
        source_count_query = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(source_count , batch_id)
        if offset == 0:
            logger.info(source_count_query)
            status_update(source_count_query, listing_cursor, listing_conn)

        token = create_token(auth)
        delete_query =  """ DELETE FROM stage.direct_idx_id where source_id = {0} """.format(source_id)
        if offset == 0:
            listing_cursor.execute(delete_query)
            log_msg = {
                "source_id": source_id,
                "Deleted_count" : listing_cursor.rowcount,
                "Message": "Deleted Previous Data from Stage Table",
                "Query": delete_query                
            }
            listing_conn.commit()

        final_response = download_listing_for_inactive(source_id, batch_id,  listing_cursor, listing_conn, loginurl,token, limit, offset)
        offset =  offset + limit
        event.update(final_response)
        event['break_loop'] = False
        event['offset'] = offset
        event['source_count'] = source_count
        # logger.info(event)        
        return event

    except Exception as e:
        
        final_response = {
            'source_id': source_id, 
            'mls_board': mls_board, 
            'source_type': source_type,                     
            'batch_id': batch_id, 
            'download_status': False,
            'run_host': run_host
            }
        final_response['success'] = event['success']
        log_msg ={  
            'Error':e , 
            "Error At line": traceback.format_exc(),
            "Payload" :final_response
        }
        logger.error(log_msg)        
        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()