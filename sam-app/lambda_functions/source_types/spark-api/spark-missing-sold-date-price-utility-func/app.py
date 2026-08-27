
""" Spark Missing Sold Date Utillity Download Lambda """
import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime
import os
import time
import traceback
# from helper import LogData, LogMessage, log_message
import itertools
import logging

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-spark-missing-sold-date-utility")
logger.setLevel(logging.INFO)

# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret


def setup_db_connection(secret,sqlExecLimit):
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
        options = f"-c statement_timeout={sqlExecLimit}")

    return conn


def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    else:
        return value

# force fully update sold_date in target
def sold_date_update(listing_cursor, source_id, auth, ls_connection , event):
    
    loginurl = auth['loginUrl']
    password = auth['password']
    source_name = event['source_name']
    status_column = event['source_info']['status_column']
    limit = event['source_info'].get('limit', 1000)
    has_mlsid = event['source_info']['has_mlsid']
    mlsids = event['source_info']['mlsid']

    loginurl = loginurl.replace("$metadata", "Property")

    query = f"""
    select  source_listing_id 
    from listing_p_sold l  
    where (sold_date is null or sold_price is null)  
    and source_id  =  {source_id}
     order by modification_timestamp desc limit {limit}
    """
    listing_cursor.execute(query)
    
    result = listing_cursor.fetchall()
    source_listing_id = [r[0] for r in result]
    log_msg = {
        "source_id": source_id,
        "source_name": source_name,
        "listing_count": len(source_listing_id),
        "message": "Missing Sold Date and Sold Price Listings",
    }
    logger.info(log_msg)

    spark_list_data = []
    chunk_size = 50

    chunks = [source_listing_id[i:i + chunk_size] for i in range(0, len(source_listing_id), chunk_size)]
    
    for data in chunks:
        data = str(data).replace("[","").replace("]","")
        # status_column = "StandardStatus" if source_id == 906 else "MlsStatus"
        value = f"ListingKey in ({data})"
        if has_mlsid:
            mlsids = mlsids.replace("(", "").replace(")", "")
            value = f"{value} and MlsId in ({mlsids})"
        params = {
            "$filter": value,
            "$top":200,
            "$select": "ListingKey,CloseDate,ClosePrice,ListPrice"
        }

        headers = {
            "Authorization": f"Bearer {password}"
        }
        try:
            response = requests.get(url=loginurl, params=params, headers=headers)
            response.raise_for_status()
            data = json.loads(response.text)
            spark_list_data.extend(data['value'])
            time.sleep(1)
          

        except requests.exceptions.RequestException as e:
            log_data = {
                "source_id": source_id,
                'source_name': source_name,
                "Server_Response": response.text,  # type: ignore
                "Error_AT ": traceback.format_exc(),
                "Error": str(e)
            }
            logger.error(log_data)
            return
    
    df = pd.DataFrame(spark_list_data)
    if len(df) == 0:
        log_msg = {
            "source_id": source_id,
            "source_name": source_name,
            "message": "No Records Found",
        }
        logger.info(log_msg)
        return
    df['source_id'] = source_id
    df = df.drop(columns=['@odata.id','@Core.Permissions'], errors='ignore')
    df.fillna(pd.NaT)
    df.fillna('')
    df = df.apply(lambda col: col.map(clean_value))
    
    if 'ClosePrice' not in df.columns:
        df.rename(columns={'ListPrice':'ClosePrice'}) 
        log_data = {
            "source_id": source_id,
            "ListingId": df['ListingKey'].values.tolist(),
            "Message": "No Column ClosePrice Found From Source Side. So convert ListPrice to  ClosePrice"
        }
        logger.info(log_data)
    if 'CloseDate' not in df.columns:
        df.insert(0, 'CloseDate', None)
        log_data = {
            "source_id": source_id,
            "ListingId": df['ListingKey'].values.tolist(),
            "Message": "No Column CloseDate Found From Source Side."
        }
        logger.info(log_data)
    
    query = """
        update listing_p_sold set sold_date = %s , sold_price = %s where source_listing_id = %s and source_id  =  %s
        """
  
    data_to_update = [
        tuple(row)
        for row in df[['CloseDate','ClosePrice', 'ListingKey', 'source_id']].values
    ]


    listing_cursor.executemany(query, data_to_update)
    ls_connection.commit()
                    

    log_data = {
        "source_id": source_id,
        'source_name': source_name,
        "Update_count": len(df),
        "Status": True,
    }
    logger.info(log_data)
    

def lambda_handler(event, context):

    secret_name = os.environ.get('rdsDatabase')
    sqlExecLimit= context.get_remaining_time_in_millis() 
    secrets = fetch_secrets(secret_name)
    connection = setup_db_connection(secrets, sqlExecLimit)
    cursor = connection.cursor()

    ls_secret_name = os.environ.get('listingDatabase')    
    ls_secrets = fetch_secrets(ls_secret_name)
    ls_connection = setup_db_connection(ls_secrets, sqlExecLimit)
    ls_cursor = ls_connection.cursor()
    
    source_id = event['source_id']
    auth = event['auth']
    # source_name = event['source_name']
    # batch_id = event['batch_id']

    
    try:
        sold_date_update(ls_cursor, source_id, auth, ls_connection, event)
        event["sold_backfilling_status"] = True
        return event

    except Exception as e:
        
       
        log_msg = {'Error': str(e), "Error At line": traceback.format_exc() }
        event.update(log_msg)
        # log_data = LogData(event=event)
        # log_message(LogMessage('ERROR', 'received', log_data))
        logger.info(event)
        return event

    finally:

        if cursor:
            cursor.close()
        if connection:
            connection.close()
        if ls_cursor:
            ls_cursor.close()
        if ls_connection:
            ls_connection.close()
