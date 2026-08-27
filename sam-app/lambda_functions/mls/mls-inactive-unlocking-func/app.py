# Import necessary modules
import os 
import json
import boto3
import psycopg2
# from helper import LogData, LogMessage, log_message
from datetime import datetime
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

# Lambda function handler
def lambda_handler(event, context):
    # Create LogData instance for logging purposes
    # log_data = LogData(event=event)
    logger.info({"message": "received", "event": event})
    # run_host = event['run_host']
    # run_host = run_host[0]
    # log_message(LogMessage('INFO', 'received', log_data))
    
    try:
        # Fetching database secrets from AWS Secrets Manager
        stage_secret_name = os.environ.get('listingDatabase')
        dev_secret_name = os.environ.get('rdsDatabase')
        stage_secrets = fetch_secrets(stage_secret_name)
        dev_secrets = fetch_secrets(dev_secret_name)
        
        # Setting up database connections for staging and development databases
        stage_conn = setup_db_connection(stage_secrets)
        dev_conn = setup_db_connection(dev_secrets)
            
        # Check if the database connections are successful
        if stage_conn and dev_conn:
            # Create cursors for executing SQL queries on staging and development databases
            stage_cursor = stage_conn.cursor()
            dev_cursor = dev_conn.cursor()
        
            # Check if the event is a list, if not convert it into a list
            if isinstance(event, list):
                pass
            else:
                event = [event]
            
            # Iterate through each item in the event
            for a in event:
                if a:
                    # Extract relevant data from the event
                    id = a['source_id']
                    success = a["success"]
                    # If batch_id is available and success is True, update 'stage.etl_batches'
                    # If success is False or batch_id is None, update 'public.source' table
 
                    # if id == 306:
                    #     sql = f"UPDATE public.source SET run_host = NULL, scheduler_job=NULL, runtime_count=runtime_count+1 WHERE id='{id}' AND run_host like '%Serverless-Inactive-RETS-Silvar%'"
                    # else:
                    run_host = a['run_host']
                    sql = f"UPDATE public.source SET run_host = NULL, scheduler_job=NULL, runtime_count=runtime_count+1 WHERE id='{id}' AND run_host = '{run_host}' "

                    # Execute the SQL query and commit changes
                    stage_cursor.execute(sql)
                    stage_conn.commit()
                   
                    
                    # Log the executed query and status for unsuccessful execution
                    executed_queries = {
                        "update_query": sql,
                        "status": "Unsuccessful Execution"
                    }
                    # log_data = LogData(event=executed_queries)
                    # log_message(LogMessage('INFO', 'received', log_data))
                    logger.info({"message": "received", "executed_queries": executed_queries})

        return 
    except Exception as e:
        # Log an error message and return a 500 status code with the error details
        # log_data = LogData(event=e)
        # log_message(LogMessage('ERROR', 'received', log_data))
        logger.error({"message": "received", "error": str(e)})
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
    finally:
        # Close the database cursor and connection in the finally block
        if stage_cursor:
            stage_cursor.close()
        if dev_cursor:
            dev_cursor.close()
        if dev_conn:
            dev_conn.close()
        if stage_conn:
            stage_conn.close()
