import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import re
import os
import traceback
import logging


logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger('mls-treste-rets-inactive-data-download-func')
logger.setLevel(logging.INFO)




def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret


def db_conn(db_secret, sqlExecLimit):
    db_username = db_secret.get('username')
    db_password = db_secret.get('password')
    db_host     = db_secret.get('host')
    db_name     = db_secret.get('dbname')
    db_port     = db_secret.get('port')
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
            options=f"-c statement_timeout={sqlExecLimit}"
        )
        logger.info('Connection established successfully')
        return connection
    except Exception as e:
        logger.error({'Error': e, 'Error At line': traceback.format_exc()})




def login(data):
    loginUrl = data['loginUrl']
    password = data['password']
    username = data['user']

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)

    response = session.get(loginUrl)

    if response.status_code == 200:
        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_text = root.find('RETS-RESPONSE').text.strip()
            rets_data = dict(re.findall(r'(\w+)=([^\n\r]*)', rets_response_text))
            logger.info("Login successful!")
            rets_data['session'] = session
            rets_data['Login'] = True
            return rets_data
        except ET.ParseError as e:
            try:
                response_json = json.loads(response_text)
                error_code    = response_json['error']['code']
                error_message = response_json['error']['message']
                logger.error(f"Login failed with error code: {error_code}")
                logger.error(f"Error message: {error_message}")
            except json.JSONDecodeError:
                lines = response_text.splitlines()
                for line in lines:
                    if line.startswith("Login failed! Status code:"):
                        logger.error(f"Status Code: {line.split(':')[1].strip()}")
                    elif "Page not found" in line:
                        logger.error("Error Message: Page not found")
            return None
    else:
        logger.error(f"Login failed! Status code: {response.status_code}")
        logger.error(response.text)
        return None


def data_download(data):
    session    = data['session']
    search_url = data['Search']
    query_params = data['query_params']
    query_params['QueryType'] = 'DMQL2'
    query_params['Format']    = 'COMPACT-DECODED'
    query_params['Count']     = '1'
    query_params['Limit']     = 1000

    response      = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root          = ET.fromstring(response_text)
        count_element = root.find('.//COUNT')
        data_count    = int(count_element.get('Records'))
        columns       = root.find('./COLUMNS').text.split('\t')[1:-1]

        data_rows = []
        for data_element in root.findall('./DATA'):
            row = data_element.text.split('\t')[1:-1]
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except Exception as e:
        try:
            root       = ET.fromstring(response_text)
            reply_text = root.attrib.get('ReplyText')
            if "No records found" in reply_text:
                logger.warning(f"{reply_text}")
                return pd.DataFrame(), 0
            logger.error(f"{reply_text} Error {e}")
            return pd.DataFrame(), None
        except Exception as e:
            logger.error(f"{response_text} Error {e}")
            return pd.DataFrame(), None




def status_update(query, cursor, connection):
    cursor.execute(query)
    connection.commit()


def download_listing_for_inactive(cursor, connection, event, response):

    run_host                 = event['run_host']
    source_id                = event['source_id']
    source_type              = event['source_info']['source_type']
    originating_system_name  = event['source_info']['originating_system_name']
    source_name              = event['source_name']
    batch_id                 = event['batch_id']
    last_batch_status        = event['last_batch_status']
    inactive_threshold       = event['inactive_threshold']

    etl_status = """update stage.etl_batches set load_missing_lst_status = 'in-progress', batch_type= 'Inactive' where batch_id = {}""".format(batch_id)
    status_update(etl_status, cursor, connection)

    chunk_size = 200
    query = f""" select source_listing_id from listing where source_status IN ('ACTIVE', 'INACTIVE') and source_id in ({source_id}) """
    logger.info(query)
    cursor.execute(query)
    inactive_listings = cursor.fetchall()
    inactive_listings = [item[0] for item in inactive_listings]
    logger.info(f"source_listing_id Count from Listing{len(inactive_listings)}")

    inactive_listings_chunks = [inactive_listings[i:i + chunk_size] for i in range(0, len(inactive_listings), chunk_size)]

    status_flag  = True
    property     = pd.DataFrame()
    total_count  = 0

    for listing_chunk in inactive_listings_chunks:
        listing_chunk = str(listing_chunk).replace('[', '').replace(']', '').replace("'", '"')
        query = f"(OriginatingSystemName={originating_system_name}),(ListingKey = {listing_chunk})"

        query_params = {
            'SearchType': 'Property',
            'Class':      'Property',
            'Query':      query,
            'Select':     'ListingKey,StandardStatus'
        }
        response['query_params'] = query_params

        df, count = data_download(response)
        if count == 0:
            msg = f'source_id: {source_id} {len(df)} Records Downloaded, Resource: Property,  Query: {query}'
            logger.info(msg)
        elif count is None:
            msg = f'source_id: {source_id}  {len(df)} Downloaded Error, Resource Property, Query: {query}'
            logger.error(msg)
            return False, 0
        else:
            property = pd.concat([property, df], ignore_index=True)

    total_count  = len(property)
    source_count = """ update stage.etl_batches set source_t_counts = {0}, inactive_threshold = {2} where batch_id = {1};""".format(total_count, batch_id, inactive_threshold)
    logger.info(source_count)
    status_update(source_count, cursor, connection)

    property['source_id'] = source_id
    property['batch_id']  = batch_id
    property = property.rename(columns={'StandardStatus': 'status', 'ListingKey': 'source_listing_id'})

    tuple_list   = [tuple(row) for row in property.itertuples(index=False, name=None)]
    cols         = ','.join(list(property.columns))
    insert_query = '''INSERT INTO stage.direct_idx_id ({}) VALUES %s'''.format(cols)
    extras.execute_values(cursor, insert_query, tuple_list)
    connection.commit()

    d_count = """update stage.etl_batches set downloaded_d_counts = {0} where batch_id = {1};""".format(total_count, batch_id)
    logger.info(d_count)
    status_update(d_count, cursor, connection)

    return status_flag, total_count


def lambda_handler(event, context):

    logger.info(event)
    run_host          = event['run_host']
    source_id         = event['source_id']
    source_type       = event['source_info']['source_type']
    mls_board         = event['source_info'].get('mls_board')
    source_name       = event['source_name']
    batch_id          = event['batch_id']
    last_batch_status = event['last_batch_status']
    inactive_threshold = event['inactive_threshold']
    auth              = event['auth']

    listing_secret  = os.environ.get('listingDatabase')
    sqlExecLimit    = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    listing_conn    = db_conn(listing_secrets, sqlExecLimit)
    listing_cursor  = listing_conn.cursor()

    try:
        response    = login(auth)
        status      = False
        total_count = 0

        if response and response['Login']:
            delete_query = '''DELETE FROM stage.direct_idx_id where source_id = {0}'''.format(source_id)
            listing_cursor.execute(delete_query)
            listing_conn.commit()

            status, total_count = download_listing_for_inactive(listing_cursor, listing_conn, event, response)
            event['download_status'] = status
            event['total_count']     = total_count
            event['source_type']     = source_type
            event['mls_board']       = mls_board

            logger.info(event)

        return event

    except Exception as e:
        final_response = {
            'source_id':       source_id,
            'source_name':     source_name,
            'mls_board':       mls_board,
            'source_type':     source_type,
            'batch_id':        batch_id,
            'download_status': False,
            'run_host':        run_host,
            'success':         event['success']
        }
        logger.info(final_response)
        return final_response

    finally:
        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()