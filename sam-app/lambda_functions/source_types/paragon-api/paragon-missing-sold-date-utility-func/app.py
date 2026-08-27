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

logger = logging.getLogger("Paragon-API-Sold-Data-Download-Lambda")
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

def clean_value(value):
    if pd.isna(value) or str(value).strip().lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value

# Function to make API call and load tables
def api_call_and_load_tables(source_id, source_name, source_info, auth, batch_id, listing_cursor, ls_connection):

    
    loginUrl = auth['loginUrl']
    token = auth['password']
    status_column = source_info['status_column']
    sold_status = source_info['sold_status']
    cdc_column = source_info['cdc_column']
    reference_key = source_info['reference_key']


    query = f"""
        select mls_number from listing_p_sold l  where source_id = {source_id} and (sold_date is null or sold_price is null) order by modification_timestamp desc limit 3000
    """
    listing_cursor.execute(query)
    
    result = listing_cursor.fetchall()
    mls_numbers = []
    list_downloaded_data = []
    chunk_size = 50
    
    if len(result) > 0 :
        mls_numbers = [t[0] for t in result]
        count = len(mls_numbers)
        print("Mls Number Count:", count)
        log_data = {
            "source_id": source_id,
            "ListingId_count": len(mls_numbers),
            "Message": "Number of Listings Found With Missing Sold_Date or Sold_Price."
        }
        logger.info(log_data)
    else:
        log_data = {
            "source_id": source_id,
            "Message": "No Listing Found With Missing Sold_Date or Sold_Price."
        }
        logger.info(log_data)
        return True
    
    chunks = [mls_numbers[i:i + chunk_size] for i in range(0, len(mls_numbers), chunk_size)]

    # initial values for GET Request and loop iterations
    loginUrl = loginUrl.replace("$metadata","Property")

    for item in chunks:
        item = str(item).replace("[","").replace("]","")
        params = {
            "$filter": f"ListingId in ({item})",
            "$select": "ListingId,CloseDate,ClosePrice",
            "$top": 200
        }

        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = None
        try:
            response = requests.get(url=loginUrl, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            list_downloaded_data.extend(data['value'])
            print("Total records downloaded:", len(list_downloaded_data))

        except requests.exceptions.RequestException as e:

            log_data = {
                "source_id": source_id,
                "Error_AT ": traceback.format_exc(),
                "Server_Response": response.text,  # type: ignore
                "Error": str(e)
            }
            logger.error(log_data)
            
            return False
    
    log_data = {
        "source_id" :source_id,
        "download_count": len(list_downloaded_data)
    }
    logger.info(log_data)
    
    if len(list_downloaded_data) == 0:
        log_data = {
            "source_id": source_id,
            "Message": "No Listing Found From Source Side."
        }
        logger.info(log_data)
        return True
    df = pd.DataFrame(list_downloaded_data)
    if 'ClosePrice' not in df.columns:
        df.insert(0, 'ClosePrice', None) 
        log_data = {
            "source_id": source_id,
            "ListingId": df['ListingId'].values.tolist(),
            "Message": "No Column ClosePrice Found From Source Side."
        }
        logger.info(log_data)
    if 'CloseDate' not in df.columns:
        df.insert(0, 'CloseDate', None)
        log_data = {
            "source_id": source_id,
            "ListingId": df['ListingId'].values.tolist(),
            "Message": "No Column CloseDate Found From Source Side."
        }
        logger.info(log_data)
    df.insert(0, 'source_id', source_id)
    df.fillna(pd.NaT) # type: ignore
    df.fillna('')
    df = df.apply(lambda col: col.map(clean_value))


    query = """
        update listing set sold_date = %s , sold_price = %s where mls_number = %s and source_id  =  %s
        """

    data_to_update = [tuple(row) for row in df[['CloseDate', 'ClosePrice', 'ListingId', 'source_id']].values]

    listing_cursor.executemany(query, data_to_update)
    ls_connection.commit()
 

    log_data = {
        "source_id": source_id,
        "Update_count": len(df),
        "Status": True,
    }
    logger.info(log_data)
    return True

   

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
        "sold_backfilling_status": download_status,
        "success": False
    }
    
    try:
        download_status = api_call_and_load_tables(source_id, source_name, source_info, auth, batch_id, listing_cursor, listing_conn)

        # update download_status and return
        final_response["sold_backfilling_status"] = download_status
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