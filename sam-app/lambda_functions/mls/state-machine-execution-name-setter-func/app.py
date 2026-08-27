import boto3
import datetime
import os
import json
import logging

# Initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stepfunctions = boto3.client('stepfunctions')

def get_state_machine_arn_by_name(state_machine_name):
    paginator = stepfunctions.get_paginator('list_state_machines')
    for page in paginator.paginate():
        for sm in page['stateMachines']:
            if sm['name'] == state_machine_name:
                return sm['stateMachineArn']
    raise ValueError(f"State machine with name '{state_machine_name}' not found.")

def lambda_handler(event, context):
    # Generate a unique execution name
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    source_type = event['source_type']    
    source_type_name = event['source_type'].replace(" ", "")
    state_machine_name = event.get('state_machine_name')
    execution_name = f"{source_type_name}-{timestamp}"
    if state_machine_name:
        del event["state_machine_name"]
        pass
    else:
        # Retrieve the state machine ARN from environment variables
        state_machine_name = os.environ['state_machine_name']
    state_machine_arn = get_state_machine_arn_by_name(state_machine_name)
    # Start the state machine execution
    response = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(event),
    )
    logger.info(response)
    return {
        "statusCode": 200,
        "SourceType": source_type 
        }
