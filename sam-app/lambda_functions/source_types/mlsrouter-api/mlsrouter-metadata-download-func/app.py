# mlsrouterapi metadata download
import psycopg2
import boto3
import json
import os
import re
import requests
import logging
import pandas as pd
from psycopg2.extras import execute_values
import xml.etree.ElementTree as et
import traceback

logger = logging.getLogger("MLSRouter-Metadata-Download-Lambda")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    # Initialize AWS Secrets Manager client
    client = boto3.client('secretsmanager')
    
    # Retrieve secret value using the provided secret name
    response = client.get_secret_value(SecretId=secret_name)
    
    # Parse and return the secret as a dictionary
    secret = json.loads(response['SecretString'])
    return secret



# Function to set up a PostgreSQL database connection
def setup_db_connection(secret):
    # Extract database connection parameters from the secret
    db_user = secret['username']
    db_password = secret['password']
    db_host = secret['host']
    db_port = secret['port']
    db_name = secret['dbname']
    
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return conn
    
    

def creds_from_source_table(connection,cursor, sid):
    creds = f"select id, name, auth->'loginUrl' as loginurl, auth->'client_id' as client_id, auth->'client_secret' as client_secret from public.source where id = {sid};"
    cursor.execute(creds)
    source_creds = cursor.fetchall()
    return source_creds



def create_token(client_id, client_secret):
    url = 'https://realtyfeed-sso.auth.us-east-1.amazoncognito.com/oauth2/token'
    headers = { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }
    data = {
        'grant_type': 'client_credentials',
        'client_id': str(client_id),
        'client_secret': str(client_secret)
    }
    
    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json['access_token']
        log_msg ={
            'message': 'token generated',
            'status': 'successfull'
        }
        logger.info(log_msg)
        return token
    else:
        log_msg ={
            'message': 'token generation failed',
            'response': response
        }
        logger.error(log_msg)



def DownloadMetaData(metadata_url, token):
    classList = []
    list_dict = {item: [] for item in classList}
    headers = { "Authorization": f"Bearer {token}" }

    resp = requests.get(url=metadata_url, headers=headers)
    xml_content = resp.content
    tree = et.ElementTree(et.fromstring(xml_content))
    root = tree.getroot()
    
    for all_tree in root:
        for inner_tree in all_tree:
            if 'Namespace' in inner_tree.attrib and inner_tree.attrib[
                'Namespace'] == 'RealtyFeed.RESO.DD':
                for entity_type in inner_tree.findall('.//{http://docs.oasis-open.org/odata/ns/edm}EntityType'):
                    class_name = entity_type.attrib["Name"]
                    classList.append(class_name)

                    if class_name not in list_dict:
                        list_dict[class_name] = []
                    columns_name = entity_type.findall('.//{*}Property')

                    for col in columns_name:
                        col_names = col.attrib['Name']
                        list_dict[class_name].append(col_names)
                        
    return classList,list_dict, resp.status_code



def metadata_insertion(classList, list_dict, source_name,source_id,connection,cursor):
    classmetadata_del_sql = "delete from dev.stage_class_metadata where source_id = \'{}'".format(str(source_id))
    fieldmetadata_del_sql = "delete from dev.stage_field_metadata where source_id = \'{}'".format(str(source_id))
    cursor.execute(classmetadata_del_sql)
    cursor.execute(fieldmetadata_del_sql)
    connection.commit()
    
    for class_name in classList:
        data = (source_id, source_name, class_name, class_name)  # Create a tuple with all the values
        
        cursor.execute(
            "INSERT INTO dev.stage_class_metadata (source_id, source_name, resource_name, class_name) VALUES (%s, %s, %s, %s)",
            data)
    connection.commit()
    
    # Insertion in Field Metadata table
    resource_prefix = 'key_value'
    for item1, item2 in zip(classList, (list_dict.get(class_name) for class_name in classList)):
        data_for_insert = [(source_id, source_name, item1, item1, field_value, resource_prefix) for field_value in
                           item2]
        insert_query = "INSERT INTO dev.stage_field_metadata (source_id, source_name, resource_name, class_name, long_name,key_value) VALUES %s"
        execute_values(cursor, insert_query, data_for_insert)
        connection.commit()

    class_meta_proc = "call dev.scd_class_metadata('{}')".format(source_id)
    cursor.execute(class_meta_proc) 
    connection.commit()

    fields_meta_proc = "call dev.scd_fields_metadata_trestle('{}')".format(source_id)
    cursor.execute(fields_meta_proc)
    connection.commit()


  
def replace_numbers_with_words(df):
    """    Returns: string: renamed digit with word e.g 1 => one    """
        
    # Dictionary to map numbers to words
    num_to_words = {
        '1': 'One', 
        '2': 'Two', 
        '3': 'Three', 
        '4': 'Four', 
        '5': 'Five', 
        '6': 'Six', 
        '7': 'Seven', 
        '8': 'Eight', 
        '9': 'Nine', 
        '0': 'Zero',
        '#':'num', 
        '/':'', 
        ' ':'_', 
        '$':'Price', 
        '%':'Percent', 
        '+':'Plus', 
        ',':'', 
        '.':'', 
        ' -':'_', 
        '-':'_', 
        '!':'', 
        '&':'and', 
        "'":'', 
        "_-_":'_',
        "(":'', 
        ")":'', 
        "__":'_',
        "___":'_',
        "?":"",
        ":":""
    }
    # Replace each digit pattern in the text with its word counterpart
    text=df['long_name']
    pattern = '|'.join(re.escape(num) for num in num_to_words)
    text = re.sub(pattern, lambda m: num_to_words[m.group(0)], str(text).rstrip())
    
    df['renamed_long_name'] = text
    if df['long_name'] != text:
        df['rename_flag'] = True
        df['status_flag'] = False
    return df


def rename(source_id, cursor_rds):
    """ Proforming renaming row wise and updating data frame with new values """
    
    qurey="select id, source_id, long_name, renamed_long_name , rename_flag,  status_flag from dev.field_metadata where source_id = {}".format(source_id)
    
    cursor_rds.execute(qurey)
    data=cursor_rds.fetchall()
    columns = [column_name[0] for column_name in cursor_rds.description]
    df = pd.DataFrame(data, columns= columns) #columns=['id','source_id', 'long_name', 'renamed_long_name' , 'rename_flag',  'status_flag'])


    # Renaming Those values which are required 
    df_01= df.apply(replace_numbers_with_words, axis=1)
    
    # Merge DataFrames with indicator column
    merged_df = df_01.merge(df, how='left', indicator=True)

    # Filter rows not in left DataFrame ('left_only' in _merge column)
    filtered_df = merged_df[merged_df['_merge'] == 'left_only']

    # Optionally, drop the indicator column '_merge'
    filtered_df = filtered_df.drop('_merge', axis=1)  # axis=1 for columns

    return filtered_df

def update_rename_in_db( cursor_rds, df):
    """ Updating renamed columns values to db"""
    
    query = """
    UPDATE dev.field_metadata
    SET renamed_long_name = %s, rename_flag = %s, status_flag = %s
    WHERE source_id = %s and id= %s;
    """    
    up_to_date_data = [tuple(row) for row in df[['renamed_long_name', 'rename_flag', 'status_flag', 'source_id', 'id']].values]
    try:
        cursor_rds.executemany(query, up_to_date_data)
        # extras.execute_values(cursor_rds, query, up_to_date_data)
    except Exception as e:
        log_msg ={  
                  'Level': 'Error',
                  'Location': 'update_rename_in_db()',
                  'Error':e , 
                  "Error At line": traceback.format_exc(),  
                  }
        logging.error(log_msg)


def ddl_generation(source_id,connection,cursor):
    constant_attribs = ['pid serial4 NOT NULL primary key','source_id int4 NULL','batch_id int4 NULL','source_creation_date timestamptz NULL','source_last_update_date timestamptz NULL','y_creation_date timestamptz NULL','y_last_update_date timestamptz NULL']
    keywords_to_quote = ["Group","Order"]
    ps_tables = '''select distinct class_name from dev.class_metadata where source_id = {} and download_flag = true order by class_name desc;'''.format(str(source_id))
    cursor.execute(ps_tables)
    
    ps_tables = cursor.fetchall()
    ps_tables_name = [i[0] for i in ps_tables]
    
    for each_table in ps_tables_name:
        long_names = '''select renamed_long_name from dev.field_metadata where source_id = {0} and resource_name = '{1}';'''.format(str(source_id),each_table)
        cursor.execute(long_names)
        ddl_ = cursor.fetchall()

        # ddl_names = [j[0] + " text NULL" for j in ddl_]
        ddl_names = [f'"{j[0]}" text NULL' if j[0] in keywords_to_quote else f'{j[0]} text NULL' for j in ddl_]
        final_names = constant_attribs + ddl_names
        final_names = str(final_names).replace('[','').replace(']','').replace('\'','')
        final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_mlsrouter_{0}_{2} ( {1} '''.format(each_table,final_names,source_id)
        final_query = final_query + ')'
        cursor.execute(final_query)
    connection.commit()



# Lambda function handler
def lambda_handler(event, context):
    try:
        rds_secret = os.environ.get('rdsDatabase')
        listing_secret = os.environ.get('listingDatabase')
        rds_secret = fetch_secrets(rds_secret)
        listing_secret = fetch_secrets(listing_secret)
        rds_conn = setup_db_connection(rds_secret)
        listing_conn = setup_db_connection(listing_secret)
        rds_cursor = rds_conn.cursor()
        listing_cursor = listing_conn.cursor()
        
        source_id = event['source_id']
        credentials = creds_from_source_table(listing_conn,listing_cursor, source_id)
        
        source_name = credentials[0][1]
        loginurl = credentials[0][2]
        client_id = credentials[0][3]
        client_secret = credentials[0][4]
        
        token = create_token (client_id, client_secret)
        
        classList,list_dict, status_code = DownloadMetaData(loginurl,token)
        metadata_insertion(classList,list_dict,source_name,source_id,rds_conn,rds_cursor)

        renamed_df = rename(source_id, rds_cursor)
        update_rename_in_db( rds_cursor, renamed_df)
        rds_conn.commit()
        
        if event['ddl_generation']:
            ddl_generation(source_id,rds_conn,rds_cursor)
        
        response_msg={
            "Status Code:": status_code,
            "Source Id:": source_id
        }
        return response_msg
    
    except Exception as e:
        
        log_msg ={  'Error':e , "Error At line": traceback.format_exc(), "Event": event }
        logger.error(log_msg)
        return  e

    finally:
        if rds_conn:
            rds_conn.close() 
        if listing_conn:
            listing_conn.close()
