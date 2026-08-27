import os
import json
import boto3
import traceback
import psycopg2
import logging

logger = logging.getLogger("mls-bi-active-listings-logs-func")
logger.setLevel(logging.INFO)
pentaho_db_con = None

# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

# def db_conn(db_secret):
#     db_username = db_secret.get('username')
#     db_password = db_secret.get('password')
#     db_host = db_secret.get('host')
#     db_name = db_secret.get('dbname')
#     db_port = db_secret.get('port')
#     try:
#         connection = psycopg2.connect(database=db_name,
#                                       user=db_username,
#                                       password=db_password,
#                                       host=db_host,
#                                       port=db_port)
#         response_dict_success = {
#         'Level':'INFO',
#         "Message": 'Connection established successfully',
#         "status": "Success"
#         }
#         logging.info(response_dict_success)
#         return connection
#     except Exception as e:
#         log_msg ={  'Error':e , "Error At line": traceback.format_exc()}
#         logging.error(log_msg)

def get_connection(secret, connection):
    try:
        if connection is not None and connection.closed == 0:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return connection
    except Exception:
        pass

    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        connect_timeout=15,
    )

def lambda_handler(event, context):
    
    global pentaho_db_con
    listingDatabase=os.environ.get('listingDatabase')
    
    db_secret_listing = fetch_secrets(listingDatabase)
    
    pentaho_db_con = get_connection(db_secret_listing, pentaho_db_con)

    cursor_pentaho=pentaho_db_con.cursor()
    try:
        
        query = """ 
            INSERT INTO bi.ACTIVE_LISTINGS_LOGS (SOURCE_ID, SOURCE_NAME, MAX_MODIFICATION_DATE, BATCH_ID ,YLOPO_REFRESH ,YLOPO_UPDATE, YLOPO_CREATION  )  
            SELECT  
                sc.id as SOURCE_ID,  
                sc.name,  
                MAX(lp.modification_timestamp) AS Next_LMD_date,
                sc.batch_id,
                sc.last_completion_date as ylopo_refresh,
                max(lP.y_last_update_date) AS ylopo_update,
                max(lP.y_creation_date) AS ylopo_creation
            FROM  
                source sc  
            JOIN  
                listing lp ON lp.source_id = sc.id  
            WHERE  
                sc.active_flag IS TRUE  AND lp.load_flag is false
            GROUP BY  1,2,4,5;
        """
        cursor_pentaho.execute(query)
        
        #Query to delete from older than 30 days to reduce the resouce consumptions
        Delquery = """ 
            DELETE FROM bi.ACTIVE_LISTINGS_LOGS where ylopo_creation < NOW()- INTERVAL '30 days';
        """
        cursor_pentaho.execute(Delquery)
        pentaho_db_con.commit()

        # TODO implement
        return {
            'statusCode': 200,
            'Message': 'Success'
        }
        
    except Exception as e:
        
        # Logging an error message
        log_msg ={  
            'Error':e , 
            "Error At line": traceback.format_exc()
        }
        
        logging.error(log_msg)
        
        log_msg ={ 
            'status': 502,
            'error message':f'failed to run insertion query  {e}'
            
        }
        return log_msg
        
    finally:
        if pentaho_db_con:
            pentaho_db_con.close()
