import requests
from requests.auth import HTTPBasicAuth,HTTPDigestAuth
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
import re
import os
import boto3
import traceback
import psycopg2
from psycopg2 import extras
import json
import pandas as pd
import logging

import logging
import requests
import certifi
import ssl
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager


logger = logging.getLogger("mls-connectmls-rets-metadata-download")
logger.setLevel(logging.INFO)


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

def db_conn(db_secret, sqlExecLimit):
    db_username = db_secret.get('username')
    db_password = db_secret.get('password')
    db_host = db_secret.get('host')
    db_name = db_secret.get('dbname')
    db_port = db_secret.get('port')
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
            options = f"-c statement_timeout={sqlExecLimit}"
            )
        response_dict_success = {
        "status": "Success"
        }
        logger.info('Connection established successfully')
        return connection
    except Exception as e:
        log_msg ={  
            'Error':e ,
            "Error At line": traceback.format_exc()
            }
        logger.error(log_msg)
               
def source_table(source_id, cursor_pentaho):
    """ getting records from source table based on id """
    query = f""" select id as source_id, name, auth::json as auth from source where id in ({source_id});   """
    cursor_pentaho.execute(query)
    results = cursor_pentaho.fetchall()
    columns = [column_name[0] for column_name in cursor_pentaho.description]
    source_df =  pd.DataFrame(results , columns= columns)
    return source_df
    
def login(data):
    source_id =data['source_id']
    source_name =data['name']
    data=data['auth']
    loginUrl = data['loginUrl']
    password = data['password']
    username = data['user']

    USER_AGENT = "Python/3.8 RETS Client/1.0"

    # Create a session
    session = requests.Session()
    auth=HTTPDigestAuth(username, password)
    session.headers = {
      'RETS-Version': 'RETS/1.7.2'
       }
    session.auth = auth
    response = None
    # Send login request
    response = session.get(loginUrl)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find('RETS-RESPONSE').text.strip()
        #rets_data = dict(re.findall(r'(\\w+)=([^\  \n\\r]*)', rets_response_text))
        rets_data = dict(re.findall(r'(\w+)=([^\n\r]*)', rets_response_text))

        logger.info("Login successful!")
        rets_data['source_id'] = source_id
        rets_data['name'] = source_name
        rets_data['session'] = session
        return rets_data

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.get('ReplyText')
        log_msg = {
            'Level': 'Error',
            'Source': f'ID is: {source_id} and Name is: {source_name}',
            "Function": "login()",
            'Message': reply_text
        }
        logging.error(log_msg)
        return None

    logger.error(f"Login failed! Status code: {response.status_code}")
    logger.error(response.text)
    return None
        
def resource_metadata(data):
    Metadata_params = {
    'Type': 'METADATA-RESOURCE',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
    'ID': '0',          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
    'Format': 'COMPACT'
    }
    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    # metadata_url = data['GetMetadata']
    #metadata_url = data['Login'].split('/rets')[0] +  data['GetMetadata']
    if source_id==298:
        metadata_url = "http://sabor-rets.connectmls.com" +  data['GetMetadata']
    else:
      metadata_url = data['Login'].split('/rets')[0] +  data['GetMetadata']  
    # logger.info(metadata_url)
    metadata_response = session.get(metadata_url, params = Metadata_params)
    resource_df = pd.DataFrame()

    try:
        # logger.info(metadata_response.text)
        root = ET.fromstring(metadata_response.text)
        # Extract column names
        columns = root.find('.//COLUMNS').text.split('\t')[1:-1]
        # print(f"resource columns {columns}")
        # Extract data rows
        data_rows = []
        for data_element in root.findall('.//DATA'):
            row = data_element.text.split('\t')[1:-1]
            data_rows.append(row)
        resource_df = pd.DataFrame(data_rows, columns=columns)
    except Exception as e:
        root = ET.fromstring(metadata_response.text)
        reply_text = root.attrib.get('ReplyText')
        log_msg = {
            'Level':'Error',
            'Location': 'resource_metadata()',
            'Error':e,
            'Source':f'ID is: {source_id} and Name is: {source_name}',
            'Error AT': traceback.format_exc(),
            'Message': reply_text
        }
        logging.error(log_msg)
        return None

    # prompt: how to drop empty string or None columns or  in pandas
    resource_df.insert(0,'source_id',source_id)
    resource_df.insert(1,'source_name',source_name)
    renaming_columns = {
        "ResourceID": "resource_name",
        "Description": "resource_description",
        "KeyField": "keyfield"
    }
    resource_df = resource_df.rename(columns=renaming_columns)
    resource_df = resource_df[['source_id','source_name','resource_name','resource_description','keyfield']]
    log_msg = {
            'Level':'Info',
            'Function': 'resource_metadata()',
            'Resource':'Resource Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    return resource_df

def class_metadata(data, df):
    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    # metadata_url = data['GetMetadata']
    # metadata_url = data['Login'].split('/rets')[0] +  data['GetMetadata']
    #metadata_url = "http://sabor-rets.connectmls.com" +  data['GetMetadata']
    if source_id==298:
      metadata_url = "http://sabor-rets.connectmls.com" +  data['GetMetadata']
    else:
      metadata_url = data['Login'].split('/rets')[0] +  data['GetMetadata']     
    class_df=pd.DataFrame()

    for i, row in df.iterrows():
        resource = row['resource_name']
        Metadata_params = {
            'Type': 'METADATA-CLASS',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
            'ID': resource,          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
            'Format': 'COMPACT'
        }
        # print(f"Metadata_ResourceID {row['ResourceID']}")
        metadata_response = session.get(metadata_url, params=Metadata_params)
        root = ET.fromstring(metadata_response.text)
        try:
            # Extract column names
            columns = root.find('.//COLUMNS').text.split('\t')[1:-1]
            # print(f"class_name columns {columns}")
            # Extract data rows
            data_rows = []
            for data_element in root.findall('.//DATA'):
                row = data_element.text.split('\t')[1:-1]
                data_rows.append(row)

            df = pd.DataFrame(data_rows, columns=columns)
            df.insert(0, 'resource_name', resource)
            df.insert(0, 'source_name', source_name)
            df.insert(0, 'source_id', source_id)
            # df.head(10)
            class_df = pd.concat([class_df, df], ignore_index=True)
        except Exception as e:
            reply_text = root.attrib.get('ReplyText')
            log_msg = {
            'Level':'Error',
            'Function': 'class_metadata()',
            'Error':e,
            'Source':f'ID is: {source_id} and Name is: {source_name}',
            'Message': reply_text
            }
            logging.error(log_msg)
    renaming_columns = {
        # "Resource": "resource_name",
        "ClassName": "class_name",
        "StandardName": "standard_name",
        "VisibleName": "Visible_Name",
        "Description": "description",
        "TableDate": "key_date"
    }

    class_df = class_df.rename(columns=renaming_columns)
    class_df = class_df[['source_id','source_name','resource_name','class_name','standard_name','Visible_Name','description','key_date']]
    # print(f"After class_name columns {class_df.columns}")
    log_msg = {
            'Level':'Info',
            'Function': 'class_metadata()',
            'Resource':'Class Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    return class_df


def field_metadata(data, df):
    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    if source_id==298:
      metadata_url = "http://sabor-rets.connectmls.com" +  data['GetMetadata']
    else:
      metadata_url = data['Login'].split('/rets')[0] +  data['GetMetadata']
    field_df=pd.DataFrame()
    for i, row in df.iterrows():
        # class_name = row['class_name']
        resource = row['resource_name']
        class_name = row['class_name']
        Metadata_params = {
            'Type': 'METADATA-TABLE',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
            'ID': f"{resource}:{class_name}",          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
            'Format': 'COMPACT'
        }
        metadata_response = session.get(metadata_url, params=Metadata_params)

        try:
            root = ET.fromstring(metadata_response.text)
            # Extract column names
            columns = root.find('.//COLUMNS').text.split('\t')[1:-1]

            # Extract data rows
            data_rows = []
            for data_element in root.findall('.//DATA'):
                row = data_element.text.split('\t')[1:-1]

                data_rows.append(row)

            df = pd.DataFrame(data_rows, columns=columns)
            logger.info(f"Data Dwonload for Resource: {resource}")
            df.insert(0 ,'class_name', class_name)
            df.insert(0 ,'resource_name', resource)
            df.insert(0, 'source_name', source_name)
            df.insert(0, 'source_id', source_id)
            field_df = pd.concat([field_df, df], ignore_index=True)
        except Exception as e:
            root = ET.fromstring(metadata_response.text)
            reply_text = root.attrib.get('ReplyText')
            log_msg = {
            'Level':'Info',
            'Function': 'field_metadata()',
            'Error':e,
            'Source':f'ID is: {source_id} and Name is: {source_name}',
            'Message': reply_text
            }
            logging.error(log_msg)
    # print(f"field_name columns {field_df.columns}")
    renaming_columns = {
        "LongName": "Long_Name",
        "DBName": "DB_Name",
        "SystemName": "System_Name",
        "MaximumLength": "Max_Length",
        "LookupName":"Lookup_Name",
        "ForeignField":"Foreign_Field"
    }
    field_df['renamed_long_name'] = field_df['LongName']
    field_df = field_df.rename(columns=renaming_columns)
    field_df = field_df[['source_id','source_name','resource_name','class_name','Long_Name','renamed_long_name','DB_Name','System_Name','Max_Length','DataType','Lookup_Name','Foreign_Field']]
    log_msg = {
            'Level':'Info',
            'Function': 'field_metadata()',
            'Resource':'Field Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    field_df = field_df.drop_duplicates(subset=['Long_Name','resource_name'], keep='first')
    return field_df


def load_into_DB(table_name, df, cursor_serverless):
    """ load_into_DB """
    cols = ','.join(list(df.columns))
    data_values = [tuple(row) for row in df.values]
    insert_query = """ INSERT INTO dev.{0} ({1}) VALUES %s """.format(table_name,cols)
    extras.execute_values(cursor_serverless, insert_query, data_values)

def metadata_change_detection_and_updation(source_id,cursor_rds):
    """ Store Procedure (RDS) """       
    resource_meta_proc = "call dev.scd_resource_metadata('{}')".format(source_id)
    class_meta_proc = "call dev.scd_class_metadata('{}')".format(source_id)
    fields_meta_proc = "call dev.scd_fields_metadata_rets('{}')".format(source_id)
    cursor_rds.execute(resource_meta_proc)
    cursor_rds.execute(class_meta_proc)
    cursor_rds.execute(fields_meta_proc)
    
   
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
        '/':'_or_', 
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
    if df['long_name'] != text:
        df['rename_flag'] = True
        df['long_name'] = text
        df['status_flag'] = False
    return df

def rename(source_id, cursor_rds):
    """ Proforming renaming row wise and updating data frame with new values """
    
    qurey="select id, source_id, long_name, renamed_long_name , rename_flag,  status_flag from dev.field_metadata where Rename_flag= false and source_id = {}".format(source_id)
       
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
    SET long_name = %s, rename_flag = %s, status_flag = %s
    WHERE source_id = %s and id= %s;
    """    
    up_to_date_data = [tuple(row) for row in df[['long_name', 'rename_flag', 'status_flag', 'source_id', 'id']].values]
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
    constant_attribs = ['id serial4 NOT NULL primary key','source_id int4 NULL','batch_id int4 NULL','source_creation_date timestamptz NULL','source_last_update_date timestamptz NULL','y_creation_date timestamptz NULL','y_last_update_date timestamptz NULL', 'class_name text NULL']
    # getting resource name form class_metadata(Table) where download_flag is true
    ps_tables = '''select distinct resource_name from dev.class_metadata where source_id = {} and download_flag = true ;'''.format(str(source_id))
    cursor.execute(ps_tables)
    ps_tables = cursor.fetchall()
    
    for res_name in ps_tables:
        final_names=''
        if res_name[0] in 'Property':
            res_name= res_name[0] #str(res_name).replace('(','').replace(',)','').replace("'","")
            long_names = ''' select distinct lower(long_name) from dev.field_metadata where source_id = {0} and resource_name = '{1}' and active_flag = true and download_flag = true order by lower(long_name); '''.format(str(source_id),res_name)
            cursor.execute(long_names)
            ddl_ = cursor.fetchall()
            ddl_names = [ f'{j[0]} text NULL' for j in ddl_]
            final_names = constant_attribs + ddl_names 
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ')
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rets_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
        else:
            res_name=str(res_name).replace('(','').replace(',)','').replace("'","")
            long_names = ''' select distinct lower(long_name) from dev.field_metadata where source_id = {0} and resource_name = '{1}' and active_flag = true and download_flag = true order by lower(long_name); '''.format(str(source_id),res_name)
            cursor.execute(long_names)
            ddl_ = cursor.fetchall()
            #ddl_names = [f'{j[0]} text NULL' for j in ddl_]
            ddl_names = [f'{j[0]} text NULL' for j in ddl_ if j[0]]
            final_names = constant_attribs + ddl_names
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ')
        
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rets_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
        logger.info(final_query)
        
        cursor.execute(final_query)
    connection.commit()

    
def lambda_handler(event, context):
    
    rdsDatabase=os.environ.get('rdsDatabase')
    listingDatabase=os.environ.get('listingDatabase')
    sqlExecLimit = context.get_remaining_time_in_millis()
    
    db_secret_rds = fetch_secrets(rdsDatabase)
    db_secret_listing = fetch_secrets(listingDatabase)
    
    serverless_db_con = db_conn(db_secret_rds, sqlExecLimit) 
    pentaho_db_con = db_conn(db_secret_listing, sqlExecLimit)
    cursor_serverless=serverless_db_con.cursor()
    cursor_pentaho=pentaho_db_con.cursor()
    
    source_id = event.get("source_id")

    try:
        source_list = source_table(source_id, cursor_pentaho)
        for i, item in source_list.iterrows():
            
            rets_data = login(item)
            auth = item['auth']
            if rets_data:
                # Resource Metadata
                resource_df = resource_metadata(rets_data)
                delete_qurery = f" delete from dev.stage_resource_metadata where source_id = {item['source_id']}"
                cursor_serverless.execute(delete_qurery)
                serverless_db_con.commit()
                
                load_into_DB('stage_resource_metadata',resource_df, cursor_serverless)
                serverless_db_con.commit()
                
                # Class Metadata
                class_df = class_metadata(rets_data, resource_df)
                delete_qurery = f" delete from dev.stage_class_metadata where source_id = {item['source_id']}"
                cursor_serverless.execute(delete_qurery)
                serverless_db_con.commit()
                
                load_into_DB('stage_class_metadata',class_df, cursor_serverless)
                serverless_db_con.commit()
                
                # Field Metadata
                field_df = field_metadata(rets_data, class_df)
                delete_qurery = f" delete from dev.stage_field_metadata where source_id = {item['source_id']}"
                cursor_serverless.execute(delete_qurery)
                serverless_db_con.commit()
                
                load_into_DB('stage_field_metadata',field_df, cursor_serverless)
                serverless_db_con.commit()
                
                # call store procedure
                metadata_change_detection_and_updation(rets_data['source_id'], cursor_serverless)
                serverless_db_con.commit()
                
                # Rename column name
                renamed_df = rename(rets_data['source_id'], cursor_serverless)
                update_rename_in_db( cursor_serverless, renamed_df)
                serverless_db_con.commit()
                
                if event['ddl_generation']:
                    ddl_generation(rets_data['source_id'],serverless_db_con,cursor_serverless)
        
            else:
                log_msg ={
                    'status': 502,
                    'error message':f'Metadata download failed  {rets_data}'

                }
                logging.error(log_msg)
                return log_msg 
        log_msg ={ 
            'Level': 'INFO',
            'status': 200,
            'message':'Metadata download successfully'
            
        }
        logging.info(log_msg)
        
        return log_msg
        
    except Exception as e:
        
        # Logging an error message
        log_msg ={  
            'Error': str(e) , 
            'Location':'lambda_handler()', 
            "Error At line": traceback.format_exc()
        }
        
        logging.error(log_msg)
        
        log_msg ={ 
            'status': 502,
            'error message':f'Metadata download failed  {e}'
            
        }
        
        
        return log_msg
        
    finally:
        if serverless_db_con:
            cursor_serverless.close()
            serverless_db_con.close() 
        if pentaho_db_con:
            pentaho_db_con.close()
