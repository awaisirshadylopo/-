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
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
  
def create_token(data):
    client_id = data["user"]
    loginUrl = data["loginUrl"]
    client_secret = data["password"]

    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*"
      }

    data = {
        "grant_type": "client_credentials",
        "client_id": str(client_id),
        "client_secret": str(client_secret),
        "audience": "rcapi.realcomp.com",
    }
    data = json.dumps(data)
    
    response = requests.post( url=loginUrl, headers=headers, data=data, timeout=30)
    response.raise_for_status()
    response = json.loads(response.text)
    token = response["access_token"]

    return token

    
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
def api_call_and_load_tables( source_id, batch_id, cursor, connection, loginurl, limit, offset, data):
    tokenData = data
    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress' where batch_id = {}""".format(batch_id)
    if offset == 0:
        status_update(etl_status,cursor,connection)
    
    status_flag=True

    loginurl = loginurl.replace("$metadata","Property")
    base_url = loginurl
    top = 200  # Number of records to fetch in each request
    download_data_list = []
    chunk_size = 30
    query = f""" select source_listing_id::int from listing where source_id in ({source_id}) and  source_status IN ('ACTIVE','INACTIVE') order by source_listing_id limit {limit} offset {offset}"""
    # query = f"""select source_listing_id  
    #             from (
    #                 select source_listing_id::int from listing_p_active where source_id in ({source_id})
    #                 union  
    #                 select source_listing_id::int from listing_p_inactive where source_id in ({source_id})
    #                 ) a  
    #             order by source_listing_id limit {limit} offset {offset}"""
    # query = f""" select source_listing_id::int from listing_p_inactive where source_id in ({source_id}) and batch_id =59217917 and mls_number in ('20261002060',
    #                    '20251061883',
    #                    '20261002271',
    #                    '20251061489',
    #                    '20261000922',
    #                    '20261002092',
    #                    '20261001141',
    #                    '20251056412',
    #                    '20251048967',
    #                    '20251060749',
    #                    '20251037630',
    #                    '20251031992') order by source_listing_id limit {limit} offset {offset}"""


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
    password = create_token(tokenData)
    status_flag=True
    total_count = 0
    headers = {            
            "User-Agent": "Ylopo",
            "Authorization": f"Bearer {password}"
        }

    total_count = len(inactive_listings)
    


    for listing_chunk in inactive_listings_chunks:
        listing_chunk = str(listing_chunk).replace('[', '').replace(']', '')
        # password = create_token(tokenData)
        # headers = {            
        #     "User-Agent": "Ylopo",
        #     "Authorization": f"Bearer {password}"
        # }
        params = {
            "$filter": f'ListingKeyNumeric in ({listing_chunk}) and InternetEntireListingDisplayYN eq true',
            "$top": top,
            "$select": "ListingKeyNumeric,StandardStatus"
        }
        log_msg = {
            "source_id": source_id,
            "Message": "Fetching Data from Source",
            "params": params,
            "headers": headers,
            "url": base_url
        }
        response = None
        try:
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            download_data_list.extend(data['value'])
            # break
        except requests.exceptions.RequestException as e:
            log_msg = {
                "source_id": source_id,
                "Message": "Error in fetching data from Source",
                "Response": str(response.text),  # type: ignore
                "Error":e
            }
            logger.error(log_msg)
            time.sleep(30)
            password = create_token(tokenData)
            headers = {            
                "User-Agent": "Ylopo",
                "Authorization": f"Bearer {password}"
            }
            response = requests.get(url=base_url, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            download_data_list.extend(data['value'])
                    
    df = pd.DataFrame(download_data_list)
    df['source_id'] = source_id
    df['batch_id'] = batch_id
    df = df.rename(columns={'StandardStatus': 'status', 'ListingKeyNumeric': 'source_listing_id'})
    log_msg = { 
        "source_id": source_id,
        "batch_id": batch_id,
        "Count_From": len(df),
        "Message": "Downloaded Data"
    }
    logger.info(log_msg)
    total_count = len(df)
    

    tuple_list = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cols = ','.join(list(df.columns))
    
    insert_query = """ 
                INSERT INTO stage.direct_idx_id ({}) VALUES %s
                 """.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()
    
    # getting download count 
    query = f""" select count(1) from stage.direct_idx_id where source_id in ({source_id}) """
    logger.info(query)
    cursor.execute(query)
    downloaded_counts = cursor.fetchone()[0]
    # inactive_listings = [item[0] for item in inactive_listings]

    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(downloaded_counts, batch_id)
    status_update(d_count,cursor,connection)
    
    status_flag = True 
    
    
    return {
        'source_id': source_id, 
        'batch_id': batch_id,
        'download_status': status_flag ,
        'download_count_in_current_itreation':total_count,
        'downloaded_counts' : downloaded_counts
        }

def lambda_handler(event, context):
    # TODO implement   
    logger.info(event)
    
    run_host = event['run_host']
    source_id = event['source_id']
    source_type = event['source_info']['source_type']
    mls_board = event['source_info'].get('mls_board')
    limit = 2000 #event.get('limit', 1000)
    offset = event.get('offset', 0)
    total_downloaded = event.get('total_downloaded', 0)
    batch_id = event['batch_id']
    last_batch_status = event['last_batch_status']
    data = event["auth"]
    loginurl = data["metadataUrl"]
    event['success'] = False
    # if count =
    listing_secret = os.environ.get('listingDatabase')
    listing_secrets = fetch_secrets(listing_secret)
    sqlExecLimit = context.get_remaining_time_in_millis()
    listing_conn = setup_db_connection(listing_secrets , sqlExecLimit)
    listing_cursor = listing_conn.cursor()
    
    query = f""" select count(1) from listing where  source_status IN ('ACTIVE','INACTIVE') and source_id in ({source_id}) ;"""
    # logger.info(query)
    listing_cursor.execute(query)
    source_count = int(listing_cursor.fetchone()[0])
    if source_count <= offset:
        log_msg = {
            "source_id": source_id,
            'Offset' : offset,
            "source_count": source_count,
            "Message": "Offset Exceed then source count "
        }
        event['success'] = True
        response = {
            'source_id': source_id, 
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
            'source_count':source_count,
            'total_downloaded': total_downloaded,
        }
        logger.info(log_msg)
        return response

    try:        
        source_count_query = """ update stage.etl_batches set source_t_counts = {0} where batch_id = {1};""".format(source_count , batch_id)
        if offset == 0:
            status_update(source_count_query, listing_cursor, listing_conn)

         #, source_id, listing_cursor, listing_conn)

        delete_query =  """ DELETE FROM stage.direct_idx_id where source_id = {0} """.format(source_id)
        if offset == 0:
            listing_cursor.execute(delete_query)
            listing_conn.commit()
        # while offset <= source_count:
            
        final_response = api_call_and_load_tables(source_id, batch_id,  listing_cursor, listing_conn, loginurl, limit, offset, data)
        offset =  offset + limit
        event.update(final_response)
        event['break_loop'] = True if source_count <= offset else False
        event['offset'] = offset
        event['success'] = True
        event['source_count'] = source_count
        event['source_type'] = source_type
        event['mls_board'] = mls_board


        logger.info(final_response)        
        return event

    except Exception as e:
        
        final_response = {
            'source_id': source_id, 
            'mls_board': mls_board, 
            'source_type': source_type,                     
            'batch_id': batch_id, 
            'download_status': False,
            'run_host': run_host,
            'error': str(e)
            }
        final_response['success'] = False
        log_msg ={  'Error':e , 
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