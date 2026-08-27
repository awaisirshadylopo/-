import json
import boto3
import os 
import psycopg2
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

# Function to set up a PostgreSQL database connection
def setup_db_connection(secret):
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
        port=db_port
    )
    return conn

# Lambda function handler
def lambda_handler(event, context):
    ''' this lambda returns list containing data dictionaries as mentioned below
    {"source_id": 831,
    "source_name": "INCLINE DIRECT",
    "auth": {"type": "noImageAuth","user": "trestle_YlopoLLCYlopoLicense20190617015205","proxy": false,"loginUrl": "https://api-prod.corelogic.com/trestle/oidc/connect/token","password": "6d9dfa7a47db4ce78d36ab17a55a3412","logoutUrl": "", "isPlaintext": true},
    "originating_system_name": null,
    "run_host": "Serverless-Trestle"
  }
    '''
    
    
    # Log data initialization
    logger.info({"message": "received", "event": event})
														 

    try:
        # Fetching Stage database secrets from AWS Secrets Manager
        secret_name=os.environ.get('listingDatabase')
        secrets = fetch_secrets(secret_name)
        
        sId = os.environ.get('SourceIds')
        # Extracting 'source_type' from the Lambda event
        source_type = event['source_type']
        run_host_value = 'Serverless-Inactive-{}'.format(source_type)
        
        # Setting up a database connection
        conn = setup_db_connection(secrets)
       

        if conn:
            cursor = conn.cursor()
            
            # select_query_1 = f"""
            # select id from source where 1=1 and id = 592   and designated_run_host  is null ;
            #   """
            
            
            # SQL query to select records that meet certain criteria  
            
            # select_query_1= f"""
            # select id from source where id in (739)
            # """
            
            select_query_1 = f"""
            SELECT id
                FROM (
                    SELECT s.id,
                          CASE 
                                WHEN coalesce(s.runtime_count,0) = 1 THEN true
 
                                WHEN coalesce(s.runtime_count,0) > 1 AND s.runtime_count % inactive_runtime_step = 0 THEN true 
                                ELSE false 
                          END AS run_inactive
                    FROM public.source AS s
                    WHERE s.run_host IS NULL 
                    AND s.source_info->>'source_type' = '{source_type}' 
                    AND s.is_scheduled IS TRUE
                    AND s.id NOT IN (
                        SELECT s.id
                        FROM public.source AS s 
                        JOIN stage.etl_batches AS e ON replace(upper(s.name), ' ', '') = replace(upper(e.source), ' ', '')
                        WHERE e.load_inactive_lst_status <> 'Completed' 
                        AND e.run_host::text ILIKE '%pdi%'
                        AND s.is_scheduled IS TRUE 
                        AND s.source_info->>'source_type' = '{source_type}'
                    )
                ) AS subquery
                WHERE run_inactive = true; -- Filter only rows where run_inactive is true

            """
            
            # NOTE: UPDATE SOURCE IDS IN ENVIRONMENT VARIABLES
            
            #select_query_1 = "Select id from public.source where id= 759"
            
            
            # Executing the select query
            result = cursor.execute(select_query_1)
            result = cursor.fetchall()
        
            # Updating records in the database based on the result
            list_id = []
            for r in result:
                list_id.append(r[0])
                update_query = f"UPDATE public.source SET run_host='{run_host_value}' WHERE id =%s"
                cursor.execute(update_query, (r,))
                conn.commit()
            
            # update_query = f"UPDATE public.source SET run_host='{run_host_value}' WHERE id =904"
            # cursor.execute(update_query)
            # conn.commit()
            if len(list_id) == 1:
                id_tuple = f"({list_id[0]})"
            else:
                id_tuple = tuple(list_id)

           
            
            # Fetching records after the update
            if result:            
                select_query_2 = f"SELECT id, name, auth, originating_system_name, source_info,inactive_threshold FROM public.source WHERE  run_host='{run_host_value}'AND id in {id_tuple}"
            
            # NOTE: UPDATE SOURCE IDS IN ENVIRONMENT VARIABLES
                cursor.execute(select_query_2)
                rows = cursor.fetchall()
            

            # Creating a list of dictionaries from the fetched records
                dict_list = []
                for row in rows:
                    if row[0] == 1013:
                        source_info = row[4]
                        if source_info:
                            source_info["func_name"] = "sourcere"
                            source_info["source_type"] = "SourceRE API"
                            
                    tup = {
                        'source_id': row[0],
                        'source_name': row[1],
                        'auth': row[2],
                        'source_info': row[4],
                        'run_host': run_host_value,
                        'inactive_threshold': row[5],
                        'success': False
                    }
                    dict_list.append(tup)
                
                #Logging the processed data
                logger.info({"message": "received", "event": dict_list})
																	 

                # Returning the processed data
                return dict_list
            else:
                return []

    except Exception as e:
        # Logging an error message
        logger.debug({"message": "received", "event": str(e)})
															  
        
        # Returning an error response
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
    finally:
        # Closing the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

