# make sure to add request, polar arn under the layer key in template file before PR to prod

""" Unified Rets Metadata Rets """
import requests
from requests.auth import HTTPBasicAuth,HTTPDigestAuth
import xml.etree.ElementTree as ET
import datetime
import re
import traceback
import psycopg2
from psycopg2 import extras
import json
import logging
import os
import boto3
import polars as pl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib.parse import urljoin, urlparse
from psycopg2.extras import execute_values

logger = logging.getLogger("mls-unified-rets-metadata-download")
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(levelname)s - %(message)s", force=True)

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

# get data from source table
def source_table(source_id, cursor_pentaho):
    query = f"SELECT id as source_id, name, auth::json as auth FROM source WHERE id in ({source_id});"
    cursor_pentaho.execute(query)
    results = cursor_pentaho.fetchall()
    columns = [column_name[0] for column_name in cursor_pentaho.description]
    source_df =  pl.DataFrame(results, schema = columns)
    return source_df

# Login
def login(data):
    source_id = data['source_id']
    source_name = data['name']
    auth = data['auth']
    login_url = auth['loginUrl']
    username = auth['user']
    password = auth['password']
    headers = auth.get("headers", {})

    # Creating a Session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth

    # Sending login request
    response = session.get(login_url, headers=headers)

    if response.status_code == 200:
        response_text = response.text
        try:
            root = ET.fromstring(response_text)
            rets_response_element = root.find('RETS-RESPONSE')

            if rets_response_element is not None and rets_response_element.text:
                rets_response_text = rets_response_element.text.strip()
                rets_data = dict(re.findall(r'(\w+)=([^\n\r]*)', rets_response_text)) # This line will make a dictionary of the response returned by the API after logging in.

            elif rets_response_element is None:
                rets_data = dict(root.attrib)  # ReplyCode, ReplyText
                if root.text and root.text.strip():
                    for line in root.text.strip().splitlines():
                        if "=" in line:
                            key, value = line.split("=", 1)
                            rets_data[key.strip()] = value.strip()
            else:
                logger.error("RETS-RESPONSE tag not found or empty in the XML response.")
                logger.error("Full response text:\n%s", response_text)
                return None

            logger.info("Login successful!")
            rets_data['session'] = session
            rets_data['source_id'] = source_id
            rets_data['name'] = source_name
            rets_data['loginUrl'] = login_url
            rets_data['headers'] = headers
            return rets_data

        except Exception as e:
            root = ET.fromstring(response_text)
            reply_text = root.get('ReplyText')
            log_msg = {
                'source_id': source_id,
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

# Date normalizer
def normalize_rets_date(val: str) -> str | None:
    if not val or val.strip() == "":
        return None
    
    # If it's already in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format, keep it
    try:
        # Attempt ISO parse first
        dt = datetime.datetime.fromisoformat(val.strip())
        return dt.date().isoformat()
    except Exception:
        pass

    # Try RETS weird formats
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H", "%a, %d %b %Y"):
        try:
            dt = datetime.datetime.strptime(val.strip(), fmt)
            return dt.date().isoformat()
        except Exception:
            continue

    # Fallback: return None so DB insert won’t break
    return None

# delete existing records for source_id
def delete_records(table_name, df, source_id, cursor_serverless, serverless_db_con):
    delete_query = f"DELETE FROM dev.{table_name} WHERE source_id = {source_id}"
    cursor_serverless.execute(delete_query)
    serverless_db_con.commit()
    
    load_into_DB(table_name, df, cursor_serverless)
    serverless_db_con.commit()

# metadata_url builder
def build_metadata_url(login_url: str, metadata_url: str) -> str:
    if metadata_url.startswith("http://") or metadata_url.startswith("https://"):
        return metadata_url
    return urljoin(login_url, metadata_url)

# Retrieves resource metadata and lists all classes in the source.
def resource_metadata(data):
    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    login_url = data['loginUrl']
    headers = data.get("headers", {})
    metadata_url = build_metadata_url(login_url, data['GetMetadata'])

    Metadata_params = {
    'Type': 'METADATA-RESOURCE',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
    'ID': '0',          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
    'Format': 'COMPACT' # We have 'COMPACT' which returns the metadata in tags format. In order to use the 'STANDARD-XML', you need to make changes while retrieving metadata columns.
    }
    
    # metadata API to hit and get the response
    metadata_response = session.get(metadata_url, params=Metadata_params, headers=headers)
    resource_df = pl.DataFrame()

    try:
        root = ET.fromstring(metadata_response.text)

        # Extract column names
        columns_elem = root.find('.//COLUMNS')
        if columns_elem is None or not columns_elem.text:
            raise ValueError("COLUMNS element not found or is empty in the metadata XML.")
        columns = columns_elem.text.split('\t')[1:-1]

        # Extract data rows
        data_rows = []
        for data_element in root.findall('.//DATA'):
            row = data_element.text.split('\t')[1:-1]
            data_rows.append(row)
        resource_df = pl.DataFrame(data_rows, schema=columns)
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

    resource_df = resource_df.with_columns([pl.lit(source_id).alias("source_id"), pl.lit(source_name).alias("source_name")])
    cols = ["source_id", "source_name"] + [c for c in resource_df.columns if c not in ("source_id", "source_name")]
    resource_df = resource_df.select(cols)
    renaming_columns = {
        "ResourceID": "resource_name",
        "Description": "resource_description",
        "KeyField": "keyfield"
    }
    resource_df = resource_df.rename(renaming_columns)
    resource_df = resource_df[['source_id','source_name','resource_name','resource_description','keyfield']]
    log_msg = {
            'Level':'Info',
            'Function': 'resource_metadata()',
            'Resource':'Resource Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    return resource_df

# Retrieves class metadata
def class_metadata(data, df):

    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    login_url = data['loginUrl']
    headers = data.get("headers", {})
    metadata_url = build_metadata_url(login_url, data['GetMetadata'])

    class_df=pl.DataFrame()

    for row in df.iter_rows(named=True):
        resource = row['resource_name']
        Metadata_params = {
            'Type': 'METADATA-CLASS',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
            'ID': resource,          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
            'Format': 'COMPACT'
        }

        metadata_response = session.get(metadata_url, params=Metadata_params, headers=headers)
        root = ET.fromstring(metadata_response.text)
        try:
            # Extract column names
            columns = root.find('.//COLUMNS').text.split('\t')[1:-1]

            # Extract data rows
            data_rows = []
            for data_element in root.findall('.//DATA'):
                row = data_element.text.split('\t')[1:-1]
                data_rows.append(row)

            temp_df = pl.DataFrame(data_rows, schema=columns)
            temp_df = temp_df.with_columns([
                pl.lit(source_id).alias("source_id"),
                pl.lit(source_name).alias("source_name"),
                pl.lit(resource).alias("resource_name")
            ])
            class_df = pl.concat([class_df, temp_df], how="vertical")
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
        "ClassName": "class_name",
        "StandardName": "standard_name",
        "VisibleName": "Visible_Name",
        "Description": "description",
        "TableDate": "key_date"
    }

    class_df = class_df.rename(renaming_columns)
    if "key_date" in class_df.columns:
        class_df = class_df.with_columns(
            pl.col("key_date")
            .map_elements(normalize_rets_date, return_dtype=pl.Utf8)
            .alias("key_date")
        )
    class_df = class_df[['source_id','source_name','resource_name','class_name','standard_name','Visible_Name','description','key_date']]
    log_msg = {
            'Level':'Info',
            'Function': 'class_metadata()',
            'Resource':'Class Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    return class_df

# Retrieves field metadata
def field_metadata(data, df):

    session = data['session']
    source_id = data['source_id']
    source_name = data['name']
    login_url = data['loginUrl']
    headers = data.get("headers", {})
    metadata_url = build_metadata_url(login_url, data['GetMetadata'])

    field_df=pl.DataFrame()
    for row in df.iter_rows(named=True):
        resource = row['resource_name']
        class_name = row['class_name']
        Metadata_params = {
            'Type': 'METADATA-TABLE',  # Change to METADATA-SYSTEM, METADATA-TABLE, etc., as needed
            'ID': f"{resource}:{class_name}",          # Replace with '0' for all, or a specific ID (e.g., Property, RE_1)
            'Format': 'COMPACT'
        }
        metadata_response = session.get(metadata_url, params=Metadata_params, headers=headers)


        try:
            root = ET.fromstring(metadata_response.text)
            # Extract column names
            columns = root.find('.//COLUMNS').text.split('\t')[1:-1]

            # Extract data rows
            data_rows = []
            for data_element in root.findall('.//DATA'):
                row = data_element.text.split('\t')[1:-1]

                data_rows.append(row)

            temp_df = pl.DataFrame(data_rows, schema=columns)
            logger.info(f"Data Dwonload for Resource: {resource}")
            temp_df = temp_df.with_columns([
                pl.lit(source_id).alias("source_id"),
                pl.lit(source_name).alias("source_name"),
                pl.lit(resource).alias("resource_name"),
                pl.lit(class_name).alias("class_name")
            ])
            field_df = pl.concat([field_df, temp_df], how="vertical")
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
    renaming_columns = {
        "LongName": "Long_Name",
        "DBName": "DB_Name",
        "SystemName": "System_Name",
        "MaximumLength": "Max_Length",
        "LookupName":"Lookup_Name"
    }
    field_df = field_df.with_columns(pl.col("StandardName").alias("renamed_long_name"))

    
    field_df = field_df.rename(renaming_columns)
    field_df = field_df.select(["source_id", "source_name", "resource_name", "class_name", "Long_Name", "renamed_long_name", "DB_Name", "System_Name", "Max_Length", "DataType", "Lookup_Name"])
    log_msg = {
            'Level':'Info',
            'Function': 'field_metadata()',
            'Resource':'Field Metadata Success',
            'Source':f'ID is: {source_id} and Name is: {source_name}',
        }
    logging.info(log_msg)
    field_df = field_df.unique(subset=["resource_name", "class_name", "Long_Name"], keep="first")
    return field_df

# After downloading and inserting the class and field metadata, this will insert the data into dev.class and dev.field tables
def metadata_change_detection_and_updation(source_id,cursor_rds):
    resource_meta_proc = "call dev.scd_resource_metadata('{}')".format(source_id)
    class_meta_proc = "call dev.scd_class_metadata('{}')".format(source_id)
    fields_meta_proc = "call dev.scd_fields_metadata_rets('{}')".format(source_id)
    cursor_rds.execute(resource_meta_proc)
    cursor_rds.execute(class_meta_proc)
    cursor_rds.execute(fields_meta_proc)

# Used to replace numbers with texts
def replace_numbers_with_words(text: str) -> str:
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
    pattern = '|'.join(re.escape(num) for num in num_to_words)
    return re.sub(pattern, lambda m: num_to_words[m.group(0)], str(text).rstrip())

# This will get the required columns which needs to be renamed.
def rename(source_id, cursor_rds):
    
    qurey="select id, source_id, long_name, renamed_long_name , rename_flag,  status_flag from dev.field_metadata where rename_flag= false and source_id = {}".format(source_id)
       
    cursor_rds.execute(qurey)
    data = cursor_rds.fetchall()
    columns = [col[0] for col in cursor_rds.description]
    df = pl.DataFrame(data, schema = columns) #columns=['id','source_id', 'long_name', 'renamed_long_name' , 'rename_flag',  'status_flag'])
    
    
    # Renaming Those values which are required 
    df = df.with_columns(
        pl.col("long_name").map_elements(replace_numbers_with_words, return_dtype=pl.Utf8).alias("renamed_long_name")
    )

    # Keep only changed rows
    filtered_df = df.filter(df["renamed_long_name"] != df["long_name"])

    filtered_df = filtered_df.with_columns([
        pl.lit(True).alias("rename_flag"),
        pl.lit(False).alias("status_flag"),
        pl.col("renamed_long_name").alias("long_name")
    ])

    return filtered_df
 
# Updating renamed names in the db
def update_rename_in_db(cursor_rds, df):
   
    query = """
    UPDATE dev.field_metadata
    SET long_name = %s, rename_flag = %s, status_flag = %s
    WHERE source_id = %s and id= %s;
    """    
    up_to_date_data = df.select(
        ["long_name", "rename_flag", "status_flag", "source_id", "id"]
    ).rows()
    try:
        cursor_rds.executemany(query, up_to_date_data)
    except Exception as e:
        log_msg ={  
                  'Level': 'Error',
                  'Location': 'update_rename_in_db()',
                  'Error':e , 
                  "Error At line": traceback.format_exc(),  
                  }
        logging.error(log_msg)

# Inserting data into db
def load_into_DB(table_name, df, cursor_serverless):
    cols = ','.join(df.columns)
    data_values = df.rows()
    insert_query = f"INSERT INTO dev.{table_name} ({cols}) VALUES %s"
    extras.execute_values(cursor_serverless, insert_query, data_values)

# This is universal function to create ddl
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
            # ddl_names = [ f'{j[0]} text NULL' for j in ddl_]
            ddl_names = [f'{j[0]} text NULL' for j in ddl_ if j[0]]  # This skips None or empty strings
            final_names = constant_attribs + ddl_names 
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ')
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rets_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
        else:
            res_name=str(res_name).replace('(','').replace(',)','').replace("'","")
            long_names = ''' select distinct lower(long_name) from dev.field_metadata where source_id = {0} and resource_name = '{1}' and active_flag = true order by lower(long_name); '''.format(str(source_id),res_name)
            cursor.execute(long_names)
            ddl_ = cursor.fetchall()
            ddl_names = [f'{j[0]} text NULL' for j in ddl_]
            final_names = constant_attribs + ddl_names
            final_names = str(final_names).replace('[','').replace(']','').replace('\'','').replace(':','').replace(' order ', ' "order" ')
        
            final_query = '''CREATE TABLE IF NOT EXISTS idx_stage.ps_rets_{0}_{1} ( {2} ); '''.format(res_name, source_id, final_names)
            
        logger.info(final_query)
        
        cursor.execute(final_query)
    connection.commit()

# main.
def lambda_handler(event, context):
    
    rdsDatabase=os.environ.get('rdsDatabase')
    listingDatabase=os.environ.get('listingDatabase')
    sqlExecLimit = context.get_remaining_time_in_millis()
    
    db_secret_rds = fetch_secrets(rdsDatabase)
    db_secret_listing = fetch_secrets(listingDatabase)
    
    serverless_db_con = db_conn(db_secret_rds, sqlExecLimit) 
    pentaho_db_con = db_conn(db_secret_listing, sqlExecLimit)
    cursor_serverless = serverless_db_con.cursor()
    cursor_pentaho = pentaho_db_con.cursor()
    
    source_id = event.get("source_id")

    try:
        data = source_table(source_id, cursor_pentaho)

        for data_item in data.iter_rows(named=True):

            rets_data = login(data_item)

            # Resource Metadata
            resource_df = resource_metadata(rets_data)
            delete_records("stage_resource_metadata", resource_df, data_item['source_id'], cursor_serverless, serverless_db_con)

            # Class Metadata
            class_df = class_metadata(rets_data, resource_df)
            delete_records("stage_class_metadata", class_df, data_item['source_id'], cursor_serverless, serverless_db_con)

            # Field Metadata
            field_df = field_metadata(rets_data, class_df)
            delete_records("stage_field_metadata", field_df, data_item['source_id'], cursor_serverless, serverless_db_con)

            # call store procedure
            metadata_change_detection_and_updation(rets_data['source_id'], cursor_serverless)
            
            serverless_db_con.commit()
            
            # Rename column name
            renamed_df = rename(rets_data['source_id'], cursor_serverless)
            update_rename_in_db( cursor_serverless, renamed_df)
            serverless_db_con.commit()
            
            if event['ddl_generation']:
                ddl_generation(rets_data['source_id'],serverless_db_con,cursor_serverless)


            log_msg ={ 
                'source_id': source_id,
                'status': 200,
                'message':'Metadata download successfully'
                
            }        
            return log_msg
        
    except Exception as e:
        
        # Logging an error message
        log_msg ={  
            'Error': str(e) , 
            'Location':'lambda_handler()', 
            "Error At line": traceback.format_exc()
        }     
        return log_msg
        
    finally:
        if cursor_serverless:
            cursor_serverless.close()
            serverless_db_con.close() 
        if cursor_pentaho:
            pentaho_db_con.close()
            cursor_pentaho.close()
