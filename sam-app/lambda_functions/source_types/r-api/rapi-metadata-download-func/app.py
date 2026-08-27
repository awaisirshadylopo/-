""" Realcomp Metadata API """
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
import os
import boto3
import traceback
import psycopg2
from psycopg2 import extras
import json
import pandas as pd
import logging


logger = logging.getLogger("mls-RAPI-metadata-download")
logger.setLevel(logging.INFO)

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
               

def load_into_DB(table_name, df, cursor_serverless):
    """ load_into_DB """
    cols = ','.join(list(df.columns))
    data_values = [tuple(row) for row in df.values]
    insert_query = ''' INSERT INTO dev.{0} ({1}) VALUES %s '''.format(table_name,cols)
    extras.execute_values(cursor_serverless, insert_query, data_values)

def source_table(source_id, cursor_pentaho):
    """ getting records from source table based on id """
    query = f""" select id as source_id, name as source_name, auth::json as auth from source where id in ({source_id});   """
    cursor_pentaho.execute(query)
    results = cursor_pentaho.fetchall()
    columns = [column_name[0] for column_name in cursor_pentaho.description]
    source_df =  pd.DataFrame(results , columns= columns)
    return source_df

def create_token(data):
    data = data['auth']
    client_id= data['user']
    loginUrl= data['loginUrl']
    client_secret = data['password']

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
    }

    data = {
      'grant_type':'client_credentials',
      'client_id':str(client_id),
      'client_secret':str(client_secret),
      'audience':'rcapi.realcomp.com',
    }

    response = requests.post(url=loginUrl, headers=headers, data=data)
    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json['access_token']
        
        return token
    else:
        ret ={
            'statusCode':  response.status_code,
            'body': "Token Generation Failed"
        }
        logger.info(ret)

def metadata(data, token):
  source_id = data['source_id']
  source_name = data['source_name']
  data = data['auth']
  metadata_url= data['metadataUrl']  
  classList = []
  field_dict = {}
  headers = {
      'Authorization': token
  }
  response = requests.request("GET", metadata_url, headers=headers)
  if response.status_code == 200:
    root = ET.fromstring(response.text)
    
    tree = ET.ElementTree(root)
    root = tree.getroot()

    for all_tree in root:
        for inner_tree in all_tree:
            if 'Namespace' in inner_tree.attrib and inner_tree.attrib[
                'Namespace'] == 'APIRealcomp.RealcompData':
                for entity_type in inner_tree.findall('.//{http://docs.oasis-open.org/odata/ns/edm}EntityType'):
                    class_name = entity_type.attrib["Name"]
                    row = {
                      'class_name': class_name,
                      'resource_name': class_name
                    }
                    classList.append(row)

                    if class_name not in field_dict:
                        field_dict[class_name] = []
                    columns_name = entity_type.findall('.//{*}Property')

                    for col in columns_name:
                        col_names = col.attrib['Name']
                        col_type = col.attrib['Type']
                        col_type = col_type.replace('Edm.', '')
                        col={
                            'name': col_names,
                            'type': col_type
                        }
                        field_dict[class_name].append(col)
                break
    # class_metadata
    class_metadata = pd.DataFrame(classList)
    class_metadata.insert(0, 'source_name', source_name)
    class_metadata.insert(0, 'source_id', source_id)

    # field_metadata
    field_metadata = pd.DataFrame()
    for key, value in field_dict.items():
        df = pd.DataFrame(value)
        df.rename(columns={'name': 'long_name', 'type': 'datatype'}, inplace=True)
        df.insert(0, 'class_name', key)
        df.insert(0, 'resource_name', key)
        field_metadata = pd.concat([field_metadata, df], ignore_index=True)
    field_metadata.insert(0, 'source_name', source_name)
    field_metadata.insert(0, 'source_id', source_id)

    return class_metadata, field_metadata 

def metadata_change_detection_and_updation(source_id,cursor_rds):
    """ Store Procedure (RDS) """       
    class_meta_proc = "call dev.scd_class_metadata('{}')".format(source_id)
    fields_meta_proc = "call dev.scd_fields_metadata_trestle('{}')".format(source_id)
    # fields_meta_proc = "call dev.scd_fields_metadata('{}')".format(source_id)
    
    cursor_rds.execute(class_meta_proc)
    cursor_rds.execute(fields_meta_proc)
    
    renaming_query = '''UPDATE dev.field_metadata SET renamed_long_name = right(long_name, 63) WHERE source_id = {};'''.format(source_id)
    cursor_rds.execute(renaming_query)

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
            long_names = ''' select distinct lower(long_name) from dev.field_metadata where source_id = {0} and resource_name = '{1}' and active_flag = true order by lower(long_name); '''.format(str(source_id),res_name)
            cursor.execute(long_names)
            ddl_ = cursor.fetchall()
            ddl_names = [ f'{j[0]} text NULL' for j in ddl_]
            final_names = constant_attribs + ddl_names 
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ').replace('table', '"table"')
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rapi_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
        else:
            res_name=str(res_name).replace('(','').replace(',)','').replace("'","")
            long_names = ''' select distinct lower(long_name) from dev.field_metadata where source_id = {0} and resource_name = '{1}' and active_flag = true order by lower(long_name); '''.format(str(source_id),res_name)
            cursor.execute(long_names)
            ddl_ = cursor.fetchall()
            ddl_names = [f'{j[0]} text NULL' for j in ddl_]
            final_names = constant_attribs + ddl_names
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ').replace('table', '"table"')
        
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rapi_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
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
        log_msg = None
        for i, item in source_list.iterrows():
            token = create_token(item)
            
            # MetaData Downloading 
            class_df, field_df = metadata(item, token)
            
            # Deleting Previous data
            stage_class_delete_qurery = f" delete from dev.stage_class_metadata where source_id = {item['source_id']}"
            stage_field_delete_qurery = f" delete from dev.stage_field_metadata where source_id = {item['source_id']}"
            cursor_serverless.execute(stage_class_delete_qurery)
            cursor_serverless.execute(stage_field_delete_qurery)
            serverless_db_con.commit()

            # Loading to DB
            load_into_DB('stage_class_metadata',class_df, cursor_serverless)
            load_into_DB('stage_field_metadata',field_df, cursor_serverless)
            serverless_db_con.commit()

            # call store procedure
            metadata_change_detection_and_updation(item['source_id'], cursor_serverless)
            serverless_db_con.commit()

            if event['ddl_generation']:
                ddl_generation(item['source_id'],serverless_db_con,cursor_serverless)
        
            log_msg ={ 
                'status': 200,
                'source_id': item['source_id'],
                'source_name': item['source_name'],
                'message':'Metadata download successfully'
                
            }
            logging.info(log_msg)
        
        return log_msg
        
    except Exception as e:
        
        # Logging an error message
        log_msg ={  
            'Error':e , 
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
            cursor_pentaho.close()
            pentaho_db_con.close()
