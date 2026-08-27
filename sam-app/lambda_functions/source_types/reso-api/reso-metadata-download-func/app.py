# reso metadata

import psycopg2
import boto3
import json
import os
import requests
# from helper import log_message, LogData, LogMessage
from psycopg2.extras import execute_values
import xml.etree.ElementTree as et
import traceback
import logging

logger = logging.getLogger(__name__)
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
    

def creds_from_source_table(connection,cursor, source_id):
    if source_id in (968,639):
        creds = f"select name, auth->'loginUrl', auth->'tokenUrl', auth->'username', auth->'password', auth->'client_id', auth->'client_secret' from public.source where id = {source_id};"
    else: 
        creds = f"select id, name, auth->'loginUrl' as loginurl, auth->'password' as token from public.source where id = {source_id};"
    cursor.execute(creds)
    source_creds = cursor.fetchall()
    return source_creds


def create_token(url, username, password, client_id, client_secret):

    headers = { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'client_id':client_id,
        'client_secret':client_secret
    }
    
    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        log_msg = {
            "message": "token generation failed",
            "response": response
        }
        logger.error(log_msg)


def DownloadMetaData(metadata_url, token):
    classList = []
    list_dict = {item: [] for item in classList}
    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(url=metadata_url, headers=headers)
    
    xml_content = resp.content
    
    
    tree = et.ElementTree(et.fromstring(xml_content))
    root = tree.getroot()
    for all_tree in root:
        for inner_tree in all_tree:
            if 'Namespace' in inner_tree.attrib and inner_tree.attrib[
                'Namespace'] in ['Odata.Models', 'Models','ODataService','Rapattoni.Rets.Api.Models']:
                for entity_type in inner_tree.findall('.//{http://docs.oasis-open.org/odata/ns/edm}EntityType'):
                                      
                    class_name = entity_type.attrib["Name"]
                    # if not metadata_url.__contains__('sparkapi'):
                    class_name_list = ['Field','Lookup','Media','Member','Office','OpenHouse','Property','PropertyRooms','Room']
                    if not class_name in class_name_list:
                        continue

                    classList.append(class_name)

                    if class_name not in list_dict:
                        list_dict[class_name] = []
                    columns_name = entity_type.findall('.//{*}Property')

                    for col in columns_name:
                        col_names = col.attrib['Name']
                        list_dict[class_name].append(col_names)
    
    return classList,list_dict, resp.status_code


def DownloadMetaDataJSON(metadata_url, token):
    classList = []
    list_dict = {}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    resp = requests.get(url=metadata_url, headers=headers)
    if resp.status_code != 200:
        return classList, list_dict, resp.status_code

    json_data = resp.json()

    # Assuming the JSON has a top-level key for RESO metadata
    reso_metadata = json_data.get("RESO.OData.Metadata", {})

    # Class filter as in original logic
    class_name_list = ['Field', 'Lookup', 'Media', 'Member', 'Office', 'OpenHouse', 'Property', 'PropertyRooms', 'Room']

    for class_name, class_definition in reso_metadata.items():
        if class_definition.get("$Kind") != "EntityType":
            continue

        if class_name not in class_name_list:
            continue

        classList.append(class_name)
        list_dict[class_name] = []

        for field_name, field_props in class_definition.items():
            if field_name.startswith("$") or isinstance(field_props, dict) and field_props.get("$Kind") == "NavigationProperty":
                continue
            list_dict[class_name].append(field_name)

    return classList, list_dict, resp.status_code


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


def ddl_generation(source_id,connection,cursor):
    constant_attribs = ['id serial4 NOT NULL primary key','source_id int4 NULL','batch_id int4 NULL','source_creation_date timestamptz NULL','source_last_update_date timestamptz NULL','y_creation_date timestamptz NULL','y_last_update_date timestamptz NULL']
    keywords_to_quote = ["Group","Order"]
    ps_tables = '''select distinct class_name from dev.class_metadata where source_id = {} and download_flag = true order by class_name desc;'''.format(str(source_id))
    cursor.execute(ps_tables)
    ps_tables = cursor.fetchall()
    ps_tables_name = [i[0] for i in ps_tables]
    for each_table in ps_tables_name:
        long_names = '''select long_name from dev.field_metadata where source_id = {0} and resource_name = '{1}';'''.format(str(source_id),each_table)
        cursor.execute(long_names)
        ddl_ = cursor.fetchall()
        # ddl_names = [j[0] + " text NULL" for j in ddl_]
        ddl_names = [f'"{j[0]}" text NULL' if j[0] in keywords_to_quote else f'{j[0]} text NULL' for j in ddl_]
        final_names = constant_attribs + ddl_names
        final_names = str(final_names).replace('[','').replace(']','').replace('\'','')
        final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_reso_{0}_{2} ( {1} '''.format(each_table, final_names ,source_id)
        final_query = final_query + ')'
        cursor.execute(final_query)
        log_msg = {
            "source_id": source_id,
            "Resource": each_table,
            "Table": f"idx_stage.ps_reso_{each_table}_{source_id}"
        }
        logger.info(log_msg)
    connection.commit()


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

            if source_id in (968,639):
                credentials = creds_from_source_table(listing_conn,listing_cursor, source_id)
                source_name =  credentials[0][0]
                metadata_url =  credentials[0][1]
                tokenurl =  credentials[0][2]
                username =  credentials[0][3]
                password =  credentials[0][4]
                client_id =  credentials[0][5]
                client_secret =  credentials[0][6]
                token = create_token(tokenurl, username, password, client_id, client_secret)

            else:
                credentials = creds_from_source_table(listing_conn,listing_cursor, source_id)
                metadata_url = credentials[0][2]
                token = credentials[0][3]
                source_name =  credentials[0][1]

            if source_id == 968:
                classList,list_dict, status_code = DownloadMetaDataJSON(metadata_url,token)
            else:
                classList,list_dict, status_code = DownloadMetaData(metadata_url,token)
                
            metadata_insertion(classList,list_dict,source_name,source_id,rds_conn,rds_cursor)

            if event['ddl_generation'] == True:
                ddl_generation(source_id, rds_conn, rds_cursor)
            
            response_msg={
                "Status Code:": status_code,
                "Source Id:": source_id
            }
                
            return response_msg
        
        except Exception as e:
            
            log_msg = {'Error': str(e), "Error At line": traceback.format_exc(), "Event": event}
            logger.error(str(log_msg))
            return log_msg
            
        finally:
            if rds_conn:
                rds_conn.close() 
            if listing_conn:
                listing_conn.close()
